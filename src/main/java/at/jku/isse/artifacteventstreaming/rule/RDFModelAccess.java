package at.jku.isse.artifacteventstreaming.rule;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.vocabulary.XSD;
import org.apache.jena.ontapi.model.OntClass.CardinalityRestriction;
import org.apache.jena.ontapi.model.OntClass.ValueRestriction;

import at.jku.isse.designspace.rule.arl.evaluator.ModelAccess;
import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import at.jku.isse.designspace.rule.arl.exception.ParsingException;
import at.jku.isse.designspace.rule.arl.parser.ArlType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.CARDINALITIES;
import at.jku.isse.passiveprocessengine.rdfwrapper.ListResourceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.MapResourceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFPropertyType;

public class RDFModelAccess extends ModelAccess<OntObject, OntIndividual> {

	private OntModel model;
	private ListResourceType listFactory;
	private MapResourceType mapFactory;
	
    public RDFModelAccess(OntModel model) {
    	this.model = model;
    	listFactory = new ListResourceType(model);
    	mapFactory = new MapResourceType(model);
        ModelAccess.instance = this;
    }

    public Set<OntIndividual> instancesOfArlType(ArlType type) {
        if (type.nativeType == null) return new HashSet<>();
        return ((OntClass)type.nativeType).individuals().collect(Collectors.toSet());
    }

    public ArlType arlTypeByQualifiedName(String name) {
    	//TODO: prefix resolving necessary?
        OntClass type = model.getOntClass(name);
        return ArlType.get(ArlType.TypeKind.INSTANCE, ArlType.CollectionKind.SINGLE, type);
    }

    @Override
    public boolean arlIsKindOf(ArlType typeA, ArlType typeB) {
        OntClass instanceTypeA = (OntClass) typeA.nativeType;
        OntClass instanceTypeB = (OntClass) typeB.nativeType;
        return instanceTypeA.hasSuperClass(instanceTypeB, false);
    }

    public ArlType arlSuperTypeOfType(ArlType type) {
        if (type.nativeType == null) throw new EvaluationException("supertype needs a native type");
        var superType = ((OntClass)type.nativeType).superClasses()
        		.filter(superClass -> !(superClass instanceof CardinalityRestriction))
				.filter(superClass -> !(superClass instanceof ValueRestriction))
				.findFirst();

        if (superType.isEmpty())
            return null;
        else {
            return ArlType.get(ArlType.TypeKind.INSTANCE, ArlType.CollectionKind.SINGLE, superType.get());
        }
    }

    //needed to obtain the arl type of an instance
    public Set<ArlType> arlTypeOfInstance(OntIndividual element) {
        if (element instanceof OntIndividual)
        	return element.classes()
        			.filter(superClass -> !(superClass instanceof CardinalityRestriction))
    				.filter(superClass -> !(superClass instanceof ValueRestriction))
        			.map(ontClass -> ArlType.get(ArlType.TypeKind.INSTANCE, ArlType.CollectionKind.SINGLE, ontClass))
        			.collect(Collectors.toSet())     	 ;
        else
            return Collections.emptySet();
    }

    public ArlType arlTypeOfProperty(ArlType type, final String property) {
        if (type.nativeType == null) {
        	throw new EvaluationException("supertype needs a native type");
        }
        // TODO: resolve from string (plus prefix) to fqn
        Property prop = model.getProperty(property);
        if (type.nativeType instanceof OntClass ontClass) {
        	var optProp = ontClass.declaredProperties()
        			.filter(ontprop -> ontprop.getLocalName().contains(property))
        			.findAny();
        	if (optProp.isPresent() ) {
        		prop = optProp.get().asProperty();// ugly
        	}
        }

        if (prop == null || !prop.canAs(OntRelationalProperty.class) ||  !((OntClass)type.nativeType).hasDeclaredProperty(prop.as(OntRelationalProperty.class), false)) {
        	throw new ParsingException("type '%s' does not have a property '%s'", type, property);
        } else {
        	var ontProp = prop.as(OntRelationalProperty.class);
        	var res = ontProp.ranges().findAny();
        	if (res.isEmpty()) {
        		throw new ParsingException("type '%s' has untyped property '%s'", type, property);
        	} 
        	var entry = RDFPropertyType.determineValueTypeAndCardinality(ontProp, mapFactory.getMapEntryClass(), listFactory.getListClass());
        	return ArlType.get(typeKind(entry.getKey()), collectionKind(entry.getValue()), entry.getKey());
        }
       
    }

    public Object propertyValueOfInstance(OntIndividual element, String propertyName) {
        if (element == null) return null;
        Property prop = model.getProperty(propertyName);
        // TODO: resolve from string (plus prefix) to fqn
        if (!element.hasProperty(prop)) return null;//to avoid NPE on unexpected instances without an expected property
    	return element.getProperty(prop); //FIXME: here we need to map not just to pure java value objects (incl set, map, list), not RDF centric objects
    }

    public ArlType.TypeKind typeKind(OntObject type) {
        if (Objects.equals(type.getURI(), XSD.integer.getURI())) return ArlType.TypeKind.INTEGER;
        if (Objects.equals(type.getURI(), XSD.xfloat.getURI())) return ArlType.TypeKind.REAL;
        if (Objects.equals(type.getURI(), XSD.xboolean.getURI())) return ArlType.TypeKind.BOOLEAN;
        if (Objects.equals(type.getURI(), XSD.xstring.getURI())) return ArlType.TypeKind.STRING;
        if (Objects.equals(type.getURI(), XSD.date.getURI())) return ArlType.TypeKind.DATE;
        return ArlType.TypeKind.INSTANCE;
    }

    public ArlType.CollectionKind collectionKind(CARDINALITIES cardinality) {
        if (cardinality == CARDINALITIES.SINGLE) return ArlType.CollectionKind.SINGLE;
        if (cardinality == CARDINALITIES.SET) return ArlType.CollectionKind.SET;
        if (cardinality == CARDINALITIES.LIST) return ArlType.CollectionKind.LIST;
        if (cardinality == CARDINALITIES.MAP) return ArlType.CollectionKind.MAP;
        throw new ParsingException("property cardinality '%s' not supported", cardinality);
    }

	@Override
	public Object scopeElement(OntIndividual instance, String property) {
		return new AbstractMap.SimpleEntry<OntIndividual, String>(instance, property);
	}
}

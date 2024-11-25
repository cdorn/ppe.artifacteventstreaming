package at.jku.isse.artifacteventstreaming.rule;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.XSD;
import org.apache.jena.ontapi.model.OntClass.CardinalityRestriction;
import org.apache.jena.ontapi.model.OntClass.ValueRestriction;

import at.jku.isse.designspace.rule.arl.evaluator.ModelAccess;
import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import at.jku.isse.designspace.rule.arl.exception.ParsingException;
import at.jku.isse.designspace.rule.arl.parser.ArlType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.CARDINALITIES;
import at.jku.isse.passiveprocessengine.rdfwrapper.ListResourceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.MapResource;
import at.jku.isse.passiveprocessengine.rdfwrapper.MapResourceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFPropertyType;
import at.jku.isse.passiveprocessengine.rdfwrapper.SingleResourceType;

public class RDFModelAccess extends ModelAccess<OntObject, Resource> {

	private OntModel model;
	private ListResourceType listFactory;
	private MapResourceType mapFactory;
	private SingleResourceType singleFactory;
	
    public RDFModelAccess(OntModel model) {
    	this.model = model;
    	listFactory = new ListResourceType(model);
    	mapFactory = new MapResourceType(model);
    	singleFactory = new SingleResourceType(model);
        ModelAccess.instance = this;
    }
    
    private OntProperty resolveToProperty(OntClass ontClass, String propertyName) {
    	return ontClass.declaredProperties()
    	.filter(prop -> prop.getLocalName().equals(propertyName)) 
    	.findAny().orElse(null);
    }

    public Set<Resource> instancesOfArlType(ArlType type) {
        if (type.nativeType == null) return new HashSet<>();
        return ((OntClass)type.nativeType).individuals().collect(Collectors.toSet());
    }

    public ArlType arlTypeByQualifiedName(String name) {
    	//prefix resolving necessary? --> ideally not as soon as the parser/lexer allows URIs 
    	// workaround, hoping for unique type names
    	OntClass type = model.getOntClass(name);
    	if (type == null) { // backup
    		type = model.classes()
    		.filter(ontClass -> ontClass.getLocalName().equals(name))
    		.findAny().orElse(null);
    	}        
        return ArlType.get(ArlType.TypeKind.INSTANCE, ArlType.CollectionKind.SINGLE, type);
    }

    @Override
    public boolean arlIsKindOf(ArlType typeA, ArlType typeB) {
        OntClass instanceTypeA = (OntClass) typeA.nativeType;
        OntClass instanceTypeB = (OntClass) typeB.nativeType;
        return instanceTypeA.hasSuperClass(instanceTypeB, false);
    }

    public ArlType arlSuperTypeOfType(ArlType type) {
   //not used anywhere but part of interface, hence we keep it here for now
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
    public Set<ArlType> arlTypeOfInstance(Resource element) {
       return getTypeOfInstance(element)
        			.map(ontClass -> ArlType.get(ArlType.TypeKind.INSTANCE, ArlType.CollectionKind.SINGLE, ontClass))
        			.collect(Collectors.toSet());        
    }
    
    private Stream<OntClass> getTypeOfInstance(Resource element) {
    	 if (element instanceof OntIndividual || element.canAs(OntIndividual.class)) {
    		var indiv = element.as(OntIndividual.class);
         	return indiv.classes(true) // direct types only
         			.filter(superClass -> !(superClass instanceof CardinalityRestriction))
     				.filter(superClass -> !(superClass instanceof ValueRestriction));
    	 } else
             return Stream.empty();
    }

    public ArlType getArlTypeOfProperty(OntObject type, final String property) {
    	var prop = resolveToProperty((OntClass)type, property);
        if (prop == null ) {
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
    
    public ArlType arlTypeOfProperty(ArlType type, final String property) {
        if (!(type.nativeType instanceof OntClass)) {
        	throw new EvaluationException("supertype needs a native type of OntClass");
        }
        return getArlTypeOfProperty((OntClass) type.nativeType, property);
       
    }
    
    @Override
    public  ArlType.CollectionKind getCardinality(Resource element, String propertyName) {
    	if (element == null) return ArlType.CollectionKind.ANY;       
        
    	var iter = element.listProperties();        
        List<Statement> content = new LinkedList<>();
        Property foundProp = null;
        while (iter.hasNext()) {
        	var stmt = iter.next();
        	var prop = stmt.getPredicate().getLocalName();
        	if (prop.equals(propertyName)) {
        		content.add(stmt);        		
        		foundProp = stmt.getPredicate();
        		iter.close();
        		break;
        	}
        }
        
        if (content.isEmpty() || foundProp == null) { //checking foundProp so that the linter stays quiet
        	return ArlType.CollectionKind.ANY;
        }
        var first = content.get(0);
        if (first.getObject().isLiteral()) { // must be a set or single
        	if (foundProp.canAs(OntRelationalProperty.class)) {
        		var ontProp = foundProp.as(OntRelationalProperty.class);
        		if (ontProp.hasSuperProperty(singleFactory.getSingleLiteralProperty(), false)) // single element
        			return ArlType.CollectionKind.SINGLE;
        		else
        			return ArlType.CollectionKind.SET;
        	} else {
        		return ArlType.CollectionKind.SET;
        	}        
        } else { //could be set, single, map, list
        	if (first.getObject().canAs(OntIndividual.class)) {
        		if (foundProp.canAs(OntRelationalProperty.class)) {
            		var ontProp = foundProp.as(OntRelationalProperty.class);
            		if (ontProp.hasSuperProperty(singleFactory.getSingleLiteralProperty(), false)) { // single element
            			return ArlType.CollectionKind.SINGLE;
            		} else if (ontProp.hasSuperProperty(mapFactory.getMapReferenceSuperProperty(), false)) { // a map
            			return ArlType.CollectionKind.MAP;
            		} else if (ontProp.hasSuperProperty(listFactory.getListReferenceSuperProperty(), false)) { // a list
            			return ArlType.CollectionKind.LIST;	    			
            		} else { // a regular set
            			return ArlType.CollectionKind.SET;
            		}
            	} else {
            		//no type info available, make a set:
            		return ArlType.CollectionKind.SET;
            	}            	      		
        	} else { //if not backed by an ontology then we cant reason and assume set
        		return ArlType.CollectionKind.SET;
        	}
        }         	
    }

    public Object propertyValueOfInstance(Resource element, String propertyName) {
        if (element == null) return null;       
        var iter = element.listProperties();        
        List<Statement> content = new LinkedList<>();
      // here we need to map not just to pure java value objects (incl set, map, list), not RDF centric objects
        Property foundProp = null;
        while (iter.hasNext()) {
        	var stmt = iter.next();
        	var prop = stmt.getPredicate().getLocalName();
        	if (prop.equals(propertyName)) {
        		content.add(stmt);        		
        		foundProp = stmt.getPredicate();
        	}
        }
        if (content.isEmpty() || foundProp == null) { //checking foundProp so that the linter stays quiet
        	return null;
            //  return null;//to avoid NPE on unexpected instances without an expected property
        }
        var first = content.get(0);
        if (first.getObject().isLiteral()) { // must be a set or single
        	return mapLiteralValues(content, foundProp);
        } else { //could be set, single, map, list
        	if (first.getObject().canAs(OntIndividual.class)) {
        		return mapAsObjectValues(content, element, foundProp);        		
        	} else { //if not backed by an ontology then we cant reason and assume set
        		return content.stream().map(Statement::getObject).collect(Collectors.toSet());
        	}
        }         	
        //return element.getProperty(prop); 
    }
    
    private Object mapLiteralValues(List<Statement> content, Property prop) {    	    	
    	if (content.size() > 1 || !prop.canAs(OntRelationalProperty.class)) { // either a set or we cant determine any type info from it
    		return content.stream().map(lit -> lit.getObject().asLiteral().getValue()); // we assume no mixing in sets
    	} else { // determine set or single
    		var ontProp = prop.as(OntRelationalProperty.class);
    		if (ontProp.hasSuperProperty(singleFactory.getSingleLiteralProperty(), false)) // single element
    			return content.get(0).getObject().asLiteral().getValue();
    		else
    			return content.stream().map(lit -> lit.getObject().asLiteral().getValue()).collect(Collectors.toSet()); // we assume no mixing in sets
    	}
    }
    
    private Object mapAsObjectValues(List<Statement> content, Resource element, Property prop) {
    	if (prop.canAs(OntRelationalProperty.class)) {
    		var ontProp = prop.as(OntRelationalProperty.class);
    		if (ontProp.hasSuperProperty(singleFactory.getSingleLiteralProperty(), false)) { // single element
    			return content.get(0).getObject().asResource();
    		} else if (ontProp.hasSuperProperty(mapFactory.getMapReferenceSuperProperty(), false)) { // a map
    			// map is not really supported by the rule engine, lets do this anyway
    			Map<String, Object> map = new HashMap<>();
    			MapResource.loadMap(element.listProperties(prop), mapFactory).stream()
    				.forEach(entry -> {
    					if(entry.getValue().getObject().isLiteral()) { 
    						map.put(entry.getKey(), entry.getValue().getObject().asLiteral());
    					} else {
    						map.put(entry.getKey(), entry.getValue().getObject().asResource());
    					}
    				});
    			return map;
    		} else if (ontProp.hasSuperProperty(listFactory.getListReferenceSuperProperty(), false)) { // a list
    			var list = content.get(0).getObject().asResource().as(Seq.class);
    			var iter = list.listProperties();
    			List<Object> result = new LinkedList<>();
    			while (iter.hasNext()) {
    				var stmt = iter.next();
    				if (stmt.getPredicate().getOrdinal() > 0) {
    					result.add(stmt.getResource());
    				}
    			}
    			return result;	    			
    		} else { // a regular set
    			return content.stream()
    					.map(Statement::getObject)
    					.map(RDFNode::asResource)
    					.collect(Collectors.toSet()); // we assume no mixing in sets
    		}
    	} else {
    		//no type info available, make a set:
    		return content.stream()
    				.map(Statement::getObject)
    				.map(RDFNode::asResource)
    				.collect(Collectors.toSet());
    	}
    	
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
	public Optional<Object> scopeElement(Resource instance, String propertyName) {
		var iter = instance.listProperties();
		Property foundProp = null;
        while (iter.hasNext()) {
        	var stmt = iter.next();
        	var prop = stmt.getPredicate().getLocalName(); 
        	if (prop.equals(propertyName)) {        		        	
        		foundProp = stmt.getPredicate();
        		iter.close();
        		break;
        	}
        }               
        if (foundProp == null) {
        	//if the property is not set, then it wont show up, need to go to the class to find declared property
            // however, the initial parsing ensured that the property per se could exist on this type, but hasn't been set yet.
            // the problem is, if we dont have a reference to this non-set/value-less property, then we cannot listen for changes to it as it doesn't show up in the context
        	//return Optional.empty();
        	//throw new RuntimeException("Cound not resolve property: "+propertyName);
        	// hence we need to look up the property in the instance's type, we arbitrarily take first
        	return getTypeOfInstance(instance).findAny().map(type -> {
        		var prop = resolveToProperty(type, propertyName);
        		if (prop != null) 
        			return new AbstractMap.SimpleEntry<Resource, Property>(instance, prop.asProperty());
        		else 
        			return null;
        	} );
        } else {
        	return Optional.of(new AbstractMap.SimpleEntry<Resource, Property>(instance, foundProp));
        }
	}
}

package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntClass.CardinalityRestriction;
import org.apache.jena.ontapi.model.OntClass.ValueRestriction;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntObjectProperty.Named;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Resource;

import com.ctc.wstx.shaded.msv_core.util.Uri;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import lombok.Getter;

public class RDFInstanceType extends RDFElement implements PPEInstanceType {

	@Getter
	protected final OntClass type;
	
	protected final Map<OntProperty, RDFPropertyType> propWrappers = new HashMap<>();
	
	public RDFInstanceType(OntClass element, NodeToDomainResolver resolver) {
		super(element, resolver);
		this.type = element;
	}

	@Override
	public void setInstanceType(PPEInstanceType arg0) {
		// noop, cannot override instancetype of an InstanceType (i.e., meta type cannot be overridden)
	}
	
	@Override
	public PPEInstanceType getInstanceType() {
		return BuildInType.METATYPE;
	}

	@Override
	public void markAsDeleted() {
		//first we remove the hierarchy below, then itself (instances need to be removed via resolver, not our concern here)
		resolver.removeTypeCascading(this);
		for (var subClass : type.subClasses().toList()) { 
			subClass.removeProperties(); 
		}
		super.markAsDeleted();
		
	}
	
	@Override
	public PPEPropertyType createListPropertyType(String arg0, PPEInstanceType arg1) {
		if (BuildInType.isAtomicType(arg1)) {
			var prop = resolver.getListBase().addLiteralListProperty(this.type, makePropertyURI(arg0), resolver.resolveAtomicInstanceType(arg1));
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		} else {
			var prop = resolver.getListBase().addObjectListProperty(this.type, makePropertyURI(arg0), ((RDFInstanceType) arg1).getType());
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		}
	}

	@Override
	public PPEPropertyType createMapPropertyType(String arg0, PPEInstanceType ignored, PPEInstanceType valueType) {
		if (BuildInType.isAtomicType(valueType)) {
			var prop = resolver.getMapBase().addLiteralMapProperty(this.type, makePropertyURI(arg0), resolver.resolveAtomicInstanceType(valueType));
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		} else {
			var prop = resolver.getMapBase().addObjectMapProperty(this.type, makePropertyURI(arg0), ((RDFInstanceType) valueType).getType());
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		}
	}

	@Override
	public PPEPropertyType createSetPropertyType(String arg0, PPEInstanceType arg1) {
		OntRelationalProperty prop = createBasePropertyType(arg0, arg1);
		if (prop != null) {
			return insertAndReturn(prop);
		} else
			return null;
	}
	
	private OntRelationalProperty createBasePropertyType(String arg0, PPEInstanceType arg1) {
		var propUri = makePropertyURI(arg0);
		if (BuildInType.isAtomicType(arg1)) {
			if (this.element.getModel().getDataProperty(propUri) != null)
				return null;
			var prop = this.element.getModel().createDataProperty(propUri);
			prop.addRange(resolver.resolveAtomicInstanceType(arg1));
			prop.addDomain(this.type);	
			return prop;
		} else {
			if (this.element.getModel().getObjectProperty(propUri) != null)
				return null;
			var prop = this.element.getModel().createObjectProperty(propUri);
			prop.addRange(((RDFInstanceType) arg1).getType());
			prop.addDomain(this.type);	
			return prop;
		}
	}
	
	private PPEPropertyType insertAndReturn(OntRelationalProperty prop) {
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		propWrappers.put(prop, propType);
		return propType;
	}

	@Override
	public PPEPropertyType createSinglePropertyType(String arg0, PPEInstanceType arg1) {
		var prop = createBasePropertyType(arg0, arg1);
		if (prop == null)
			return null;
		else {
			if (BuildInType.isAtomicType(arg1)) {
				var maxOneProp = this.element.getModel().createDataMaxCardinality((OntDataProperty) prop, 1, null);
				this.type.addSuperClass(maxOneProp);		
			} else {
				var maxOneProp = this.element.getModel().createObjectMaxCardinality((OntObjectProperty) prop, 1, null);
				this.type.addSuperClass(maxOneProp);		
			}
			return insertAndReturn(prop);
		}
	}

	@Override
	public Set<PPEInstanceType> getAllSubtypesRecursively() {
		return type.subClasses()
			.map(resolver::resolveToType)
			.collect(Collectors.toSet());
	}

	@Override
	public PPEInstanceType getParentType() {
		Optional<OntClass> parent = type.asNamed().superClasses(true)
				.filter(superClass -> !(superClass instanceof CardinalityRestriction))
				.filter(superClass -> !(superClass instanceof ValueRestriction))
				.findFirst();
		return parent.map(resolver::resolveToType).orElse(null);
	}

	@Override
	public List<String> getPropertyNamesIncludingSuperClasses() {
		return type.asNamed().declaredProperties()
			.map(Resource::getLocalName)
			.toList();
	}

	@Override
	public PPEPropertyType getPropertyType(String uri) {
		var optProp = type.asNamed().declaredProperties()
			.filter(prop -> prop.getURI().equals(uri))
			.filter(OntRelationalProperty.class::isInstance)
			.map(OntRelationalProperty.class::cast)
			.findFirst();
		return optProp.map(prop -> propWrappers.computeIfAbsent( prop, k -> new RDFPropertyType((OntRelationalProperty) k, resolver))).orElse(null) ;
	}

	@Override
	public boolean hasPropertyType(String arg0) {
		return getPropertyType(arg0) != null;
	}

	@Override
	public boolean isOfTypeOrAnySubtype(PPEInstanceType instanceToCompareType) {
		if (instanceToCompareType instanceof RDFInstanceType rdfType) {
			if (this.getId().equals(rdfType.getId())) {
					return true;
			} else {
				return type.asNamed().hasSuperClass(rdfType.getType(), false);
			}
		} else {
			return false;
		}
	}



	
	


}

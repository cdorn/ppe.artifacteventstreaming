package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntClass.CardinalityRestriction;
import org.apache.jena.ontapi.model.OntClass.ValueRestriction;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import lombok.Getter;

public class RDFInstanceType extends RDFElement implements PPEInstanceType {

	@Getter
	protected final OntClass type;
	protected final Map<String, RDFPropertyType> propWrappers = new HashMap<>();
	@Getter
	protected final Set<OntClass> allSuperClasses = new HashSet<>();
	
	public RDFInstanceType(OntClass element,  NodeToDomainResolver resolver) {
		super(element, resolver);
		this.type = element;
		// cache super properties, we assume no super properties are added/removed after a subclass has been defined.
		// cacheSuperProperties(); we cant call this here, as when resolving properties, we wont find them yet, node2domain resolver needs to call this
		element.superClasses()
			.filter(superClass -> !(superClass instanceof CardinalityRestriction))
			.filter(superClass -> !(superClass instanceof ValueRestriction))
			.forEach(allSuperClasses::add); // we assume no changes of the super class hierarchy
	}

	public void cacheSuperProperties() { //public to allow reindexing
		var model = type.getModel();
		var predicates = getExplicitlyDeclaredProperties(type, false);		
		predicates.stream().map(res -> model.getProperty(res.getURI()))
			.map(pred -> pred.as(OntRelationalProperty.class))
			.forEach(this::insertAndReturn);
	}

	protected static Collection<Resource> getExplicitlyDeclaredProperties(OntClass type, boolean direct) {
		var local = getExplicitlyDeclaredLocalProperties(type);
		if (!direct) {
			local.addAll(
				type.superClasses()
				.filter(superClass -> !(superClass instanceof CardinalityRestriction))
				.filter(superClass -> !(superClass instanceof ValueRestriction))
				.flatMap(superType -> getExplicitlyDeclaredLocalProperties(superType).stream())
				.toList()
				);
		}
		return local;
	}
	
	private static Collection<Resource> getExplicitlyDeclaredLocalProperties(OntClass type) {
		var model = type.getModel();
		var iter = model.listResourcesWithProperty(RDFS.domain, type);
		List<Resource> predicates = new LinkedList<>();
		while (iter.hasNext()) {
			predicates.add(iter.next());
		}
		return predicates;
	}
	
	
	@Override
	public PPEInstanceType getInstanceType() {
		return resolver.resolveToType(resolver.metaClass);
	}
	
	@Override
	public void setInstanceType(PPEInstanceType arg0) {
		// noop, cannot override instancetype of an InstanceType (i.e., meta type cannot be overridden)
	}
	
//	@Override
//	public PPEInstanceType getInstanceType() {
//		//return BuildInType.METATYPE;
//	}

	@Override
	public void markAsDeleted() {
		//first we remove the hierarchy below, then itself (instances need to be removed via resolver, not our concern here)
		var subclasses = resolver.removeInstancesAndTypeInclSubclassesFromIndex(this); // this removes also instances
		subclasses.forEach(subClass -> 			
			resolver.getCardinalityUtil().deleteOntClassInclOwnedProperties(subClass)
		);
		resolver.getCardinalityUtil().deleteOntClassInclOwnedProperties(type);
		super.markAsDeleted();
	}
	
	@Override
	public PPEPropertyType createListPropertyType(String arg0, PPEInstanceType arg1) {
		if (BuildInType.isAtomicType(arg1)) {
			var prop = resolver.getCardinalityUtil().getListType().addLiteralListProperty(this.type, makePropertyURI(arg0), resolver.resolveAtomicInstanceType(arg1));
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		} else {
			var prop = resolver.getCardinalityUtil().getListType().addObjectListProperty(this.type, makePropertyURI(arg0), ((RDFInstanceType) arg1).getType());
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
			var prop = resolver.getCardinalityUtil().getMapType().addLiteralMapProperty(this.type, makePropertyURI(arg0), resolver.resolveAtomicInstanceType(valueType));
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		} else {
			var prop = resolver.getCardinalityUtil().getMapType().addObjectMapProperty(this.type, makePropertyURI(arg0), (OntClass) resolver.resolveTypeToClassOrDatarange(valueType));
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		}
	}

	@Override
	public PPEPropertyType createSetPropertyType(String arg0, PPEInstanceType arg1) {
		OntRelationalProperty prop =  createBasePropertyType(arg0, arg1);
		if (prop != null) {
			return insertAndReturn(prop);
		} else
			return null;
	}
	
	private OntRelationalProperty createBasePropertyType(String arg0, PPEInstanceType arg1) {
		var propUri = makePropertyURI(arg0);
		if (BuildInType.isAtomicType(arg1)) {
			return resolver.getCardinalityUtil().getSingleType().createBaseDataPropertyType(propUri, this.type, resolver.resolveAtomicInstanceType(arg1));
		} else {
			return resolver.getCardinalityUtil().getSingleType().createBaseObjectPropertyType(propUri, this.type, (OntClass)resolver.resolveTypeToClassOrDatarange(arg1));
		}
	}
	
	private PPEPropertyType insertAndReturn(OntRelationalProperty prop) {
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		if (propType.isValid()) {
			propWrappers.put(prop.getURI(), propType);
			return propType;
		} else {
			return null;
		}
	}

	@Override
	public PPEPropertyType createSinglePropertyType(String arg0, PPEInstanceType arg1) {
		var propUri = makePropertyURI(arg0);
		if (propWrappers.containsKey(propUri)) return propWrappers.get(propUri);
		var prop = BuildInType.isAtomicType(arg1) ? 
				resolver.getCardinalityUtil().getSingleType().createSingleDataPropertyType(propUri, this.type, resolver.resolveAtomicInstanceType(arg1))
				: 
				resolver.getCardinalityUtil().getSingleType().createSingleObjectPropertyType(propUri, this.type, (OntClass)resolver.resolveTypeToClassOrDatarange(arg1));
		if (prop != null)
			return insertAndReturn(prop);
		else 
			return null;
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
		return propWrappers.values().stream().map(propW -> propW.getProperty().getLocalName()).toList(); 
//			getExplicitlyDeclaredProperties(type).stream() // We use cached properties instead
//			.map(Resource::getLocalName)
//			.toList();
	}

	@Override
	public PPEPropertyType getPropertyType(String uri) {
		var pType = findExistingType(uri);
		//if (pType != null) return pType;
		return pType; // as we cached the properties upon creation, 
		//there should be no way we missed any new properties to be added, 
		// unless the super type received new properties!
		
		// this below is very inefficient:
		//Optional<OntRelationalProperty> optProp = NodeToDomainResolver.isValidURL(uri) ? findByURI(uri) : findByLocalName(uri);					
		//return optProp.map(prop -> propWrappers.computeIfAbsent( prop.getURI(), k -> new RDFPropertyType(prop, resolver))).orElse(null) ;
	}

	private RDFPropertyType findExistingType(String uri) {
		return NodeToDomainResolver.isValidURL(uri) ?
				propWrappers.get(uri):
			    propWrappers.values().stream().filter(propType -> propType.getProperty().getLocalName().equals(uri)).findAny().orElse(null);
	}
	
//	private Optional<OntRelationalProperty> findByLocalName(String localName) {
//		return //type.asNamed().declaredProperties()
//				getExplicitlyDeclaredProperties(type, false).stream()
//				.filter(prop -> prop.getLocalName().equals(localName)) 
//				.filter(OntRelationalProperty.class::isInstance)
//				.map(OntRelationalProperty.class::cast)
//				.findFirst();
//	}
//	
//	private Optional<OntRelationalProperty> findByURI(String uri) {
//		return //type.asNamed().declaredProperties()
//				getExplicitlyDeclaredProperties(type, false).stream()
//				.filter(prop -> prop.getURI().equals(uri)) 
//				.filter(OntRelationalProperty.class::isInstance)
//				.map(OntRelationalProperty.class::cast)
//				.findFirst();	
//	}
	
	
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


	@Override
	public String toString() {
		return "RDFInstanceType [" + getId() + "]";
	}
	
	


}

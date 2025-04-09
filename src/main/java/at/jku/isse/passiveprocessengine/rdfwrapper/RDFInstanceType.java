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

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFPropertyType.PrimitiveOrClassType;
import lombok.Getter;

public class RDFInstanceType extends RDFElement {

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

	public RDFInstanceType getInstanceType() {
		return resolver.resolveToType(resolver.metaClass);
	}

	public void setInstanceType(RDFInstanceType arg0) {
		// noop, cannot override instancetype of an InstanceType (i.e., meta type cannot be overridden)
	}

	@Override
	public void delete() {
		// first we remove any collection property values on this class, i.e., any properties at the type level
		super.removeCollectionProperties();
		//then we remove the hierarchy below, then itself (instances need to be removed via resolver, not our concern here)
		var subclasses = resolver.removeInstancesAndTypeInclSubclassesFromIndex(this); // this removes also instances
		subclasses.forEach(subClass -> 			
			resolver.getCardinalityUtil().deleteOntClassInclOwnedProperties(subClass)
		);
		resolver.getCardinalityUtil().deleteOntClassInclOwnedProperties(type);
		this.isDeleted = true;
	}
	

	public RDFPropertyType createListPropertyType(String name, PrimitiveOrClassType type) {
		if (type.isPrimitiveType()) {
			var prop = resolver.getCardinalityUtil().getListType().addLiteralListProperty(this.type, makePropertyURI(name), type.getPrimitiveType());
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		} else {
			var prop = resolver.getCardinalityUtil().getListType().addObjectListProperty(this.type, makePropertyURI(name), type.getClassType());
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		}
	}


	public RDFPropertyType createMapPropertyType(String name, PrimitiveOrClassType valueType) {
		if (valueType.isPrimitiveType()) {
			var prop = resolver.getCardinalityUtil().getMapType().addLiteralMapProperty(this.type, makePropertyURI(name), valueType.getPrimitiveType());
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		} else {
			var prop = resolver.getCardinalityUtil().getMapType().addObjectMapProperty(this.type, makePropertyURI(name), valueType.getClassType());
			if (prop == null) { 
				return null;
			} else {
				return insertAndReturn(prop);
			}
		}
	}


	public RDFPropertyType createSetPropertyType(String name, PrimitiveOrClassType type) {
		OntRelationalProperty prop =  createBasePropertyType(name, type);
		if (prop != null) {
			return insertAndReturn(prop);
		} else
			return null;
	}
	
	private OntRelationalProperty createBasePropertyType(String name, PrimitiveOrClassType type) {
		var propUri = makePropertyURI(name);
		if (type.isPrimitiveType()) {
			return resolver.getCardinalityUtil().getSingleType().createBaseDataPropertyType(propUri, this.type, type.getPrimitiveType());
		} else {
			return resolver.getCardinalityUtil().getSingleType().createBaseObjectPropertyType(propUri, this.type, type.getClassType());
		}
	}
	
	private RDFPropertyType insertAndReturn(OntRelationalProperty prop) {
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		if (propType.isValid()) {
			propWrappers.put(prop.getURI(), propType);
			return propType;
		} else {
			return null;
		}
	}


	public RDFPropertyType createSinglePropertyType(String name, PrimitiveOrClassType type) {
		var propUri = makePropertyURI(name);
		if (propWrappers.containsKey(propUri)) return propWrappers.get(propUri);
		var prop = type.isPrimitiveType() ? 
				resolver.getCardinalityUtil().getSingleType().createSingleDataPropertyType(propUri, this.type, type.getPrimitiveType())
				: 
				resolver.getCardinalityUtil().getSingleType().createSingleObjectPropertyType(propUri, this.type, type.getClassType());
		if (prop != null)
			return insertAndReturn(prop);
		else 
			return null;
	}


	public Set<RDFInstanceType> getAllSubtypesRecursively() {
		return type.subClasses()
			.map(resolver::resolveToType)
			.collect(Collectors.toSet());
	}


	public RDFInstanceType getParentType() {
		Optional<OntClass> parent = type.asNamed().superClasses(true)
				.filter(superClass -> !(superClass instanceof CardinalityRestriction))
				.filter(superClass -> !(superClass instanceof ValueRestriction))
				.findFirst();
		return parent.map(resolver::resolveToType).orElse(null);
	}


	public List<String> getPropertyNamesIncludingSuperClasses() {
		return propWrappers.values().stream().map(propW -> propW.getProperty().getLocalName()).toList(); 
//			getExplicitlyDeclaredProperties(type).stream() // We use cached properties instead
//			.map(Resource::getLocalName)
//			.toList();
	}


	public RDFPropertyType getPropertyType(String uri) {
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
	
	public boolean hasPropertyType(String name) {
		return getPropertyType(name) != null;
	}

	public boolean isOfTypeOrAnySubtype(RDFInstanceType instanceToCompareType) {
		if (this.getId().equals(instanceToCompareType.getId())) {
			return true;
		} else {
			return type.asNamed().hasSuperClass(instanceToCompareType.getType(), false);
		}
	}
	
	public PrimitiveOrClassType getAsPropertyType() {
		return new PrimitiveOrClassType(this.type);
	}

	@Override
	public String toString() {
		return "RDFInstanceType [" + getId() + "]";
	}
	
	


}

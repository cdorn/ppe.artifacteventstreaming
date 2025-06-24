package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.schemasupport.Cardinalities;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResource;
import at.jku.isse.passiveprocessengine.rdfwrapper.collections.ListWrapper;
import at.jku.isse.passiveprocessengine.rdfwrapper.collections.MapWrapper;
import at.jku.isse.passiveprocessengine.rdfwrapper.collections.SetWrapper;
import at.jku.isse.passiveprocessengine.rdfwrapper.collections.TypedCollectionResource;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class RDFElement {

	@Getter
	protected final OntObject element;
	protected RDFInstanceType instanceType;
	protected final NodeToDomainResolver resolver;
	
	public static final String propertyModifiedByPredicate = NodeToDomainResolver.BASE_NS+"modifiedBy";
	private Property owner = ResourceFactory.createProperty(propertyModifiedByPredicate);
	
	// caches are filled as they are accessed
	protected final Map<String, TypedCollectionResource> collectionPropertyCache = new HashMap<>();


	
	protected boolean isDeleted = false; // in case someone holds on to wrapper object

	public String getId() {
		if (element.isAnon())
			return element.getId().toString();
		else
			return element.getURI();
	}

	public String getName() {
		var label = element.getLabel();		
		if (label == null) {
			return element.getLocalName();
		} else {
			return label;
		}
	}
		
	public void setName(String name) {
		element.removeAll(RDFS.label);
		element.addLabel(Objects.toString(name));		
	}

	public void delete() {
		this.isDeleted = true;
		removeCollectionProperties();
		element.removeProperties();		
		//remove from cache is done in childclasses
	}

	protected void removeCollectionProperties() {
		// if collections are never accessed before then they wont be deleted via the collectionPropertyCache
				// , hence we first populate all properties, not very efficient but should work for now
				this.getInstanceType().getPropertyNamesIncludingSuperClasses().stream()
					.map(prop -> getInstanceType().getPropertyType(prop))
					.forEach(prop -> { 
						switch(prop.getCardinality()) {
						case LIST:
							getTypedProperty(prop.getName(), List.class);
							break;
						case MAP:
							getTypedProperty(prop.getName(), Map.class);
							break;
						case SET:
							getTypedProperty(prop.getName(), Set.class);
							break;
						case SINGLE:
							getTypedProperty(prop.getName(), Object.class);
							break;
						default:
							break;
						}
					});
				collectionPropertyCache.values().forEach(coll -> coll.delete()); // ensures that collections are completely removed and dont linger empty without owner in memory
	}

	public boolean isMarkedAsDeleted() {
		//we dont mark anything deleted, we just delete (for now)
		return isDeleted;
	}


	public boolean isOwner(String ownerId) {
		return getOwners().contains(ownerId);
	}


	public void addOwner(String ownerId) {
		element.addProperty(owner, ownerId);
	}


	public Set<String> getOwners() {
		StmtIterator iter = element.listProperties(owner);
		Set<String> owners = new HashSet<>();
		while (iter.hasNext()){
			owners.add(iter.next().getString());
		}
		return owners;
	}
	

	public abstract RDFInstanceType getInstanceType();
	

	/**
	 * Expects fully qualified named properties , i.e., as they are used at the RDF layer
	 */
	public void setSingleProperty(String property, Object value) {		
			var prop = resolveProperty(property, Cardinalities.SINGLE);
			element.removeAll(prop.asProperty());
			if (value instanceof RDFElement inst && value != null) { // then a resource and not a literal
				element.addProperty(prop.asProperty(), inst.getElement());
			} else if (value != null) {
				element.addLiteral(prop.asProperty(),  value);
			}		
	}

	public String makePropertyURI(String propertyLocalName) {
		if (!NodeToDomainResolver.isValidURL(propertyLocalName)) {		
			String ns= element.getNameSpace();
			String elName = element.getLocalName();
			
			return ns.substring(0, ns.length()-1)+"/"+elName+"#"+propertyLocalName;
		} else {
			return propertyLocalName;
		}
	}
	
	protected OntRelationalProperty resolveProperty(String property, Cardinalities... expectedCardinalities) {
		var expCardi = Set.of(expectedCardinalities);
		RDFInstanceType type = getInstanceType();
		if (type == null) { // we are untyped, thus return just the property, consumer needs to know what they are doing
			property = makePropertyURI(property);
			OntRelationalProperty prop = element.getModel().getObjectProperty(property);
			if (prop == null)
				prop = element.getModel().getDataProperty(property);
			if (prop == null) {
				throw new IllegalArgumentException("Property unknown: "+property);
			}
			return prop;
		} else { // check if the cardinality matches
			RDFPropertyType pType = type.getPropertyType(property);
			if (pType == null) {
				throw new IllegalArgumentException("Property unknown: "+property);
			}
			if(expCardi.contains(pType.getCardinality())) {
				return pType.getProperty();
			} else {
				throw new IllegalArgumentException(String.format("Cardinality mismatch for property %s: expected %s but defined as %s", property, expCardi, pType.getCardinality()));
			}
		}
	}
	
	public Optional<RDFPropertyType> resolveToPropertyType(String property) {
		//property = makePropertyURI(property);
		
//		if (this instanceof RDFInstanceType rdfType)
//			return Optional.ofNullable( (RDFPropertyType) rdfType.getPropertyType(property));		
		var ppeType = getInstanceType();		
		if (ppeType != null) {						
			return Optional.ofNullable( ppeType.getPropertyType(property));
		} else
			return Optional.empty();
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getTypedProperty(@NonNull String property, @NonNull Class<T> clazz) {		
		Optional<RDFPropertyType> optProp = resolveToPropertyType(property);
		if (optProp.isEmpty()) 
			throw new IllegalArgumentException(String.format("Property %s does not exist on element %s", property, element.getURI()));
		else {
		// check if single or collection property
			var prop = optProp.get();
			if (prop.getCardinality().equals(Cardinalities.SINGLE)) {
				return (T) getSingleProperty(prop);
			}
			if (Set.class.isAssignableFrom(clazz) && prop.getCardinality().equals(Cardinalities.SET)) {
				return (T) getPropertyAsSet(prop);
			} 
			if (List.class.isAssignableFrom(clazz) && prop.getCardinality().equals(Cardinalities.LIST)) {
				return (T) getPropertyAsList(prop);
			} 
			if (Map.class.isAssignableFrom(clazz) && prop.getCardinality().equals(Cardinalities.MAP)) {
				return (T) getPropertyAsMap(prop);
			}	
			throw new IllegalArgumentException(String.format("Property %s does exist on element %s but of cardinality %s incompatible with clazz %s "
					, property, element.getURI(), prop.getCardinality(), clazz.getName()));
		}
	}
	
	private Object getPropertyAsMap(@NonNull RDFPropertyType prop) {
		var named = (OntObjectProperty.Named) prop.getProperty();
		return collectionPropertyCache.computeIfAbsent(prop.getId(),  k -> new MapWrapper(prop.getValueType().getAsPrimitiveOrClass(), 
																							resolver, 
																							MapResource.asUnsafeMapResource(this.element, named, resolver.getMetaschemata().getMapType()))); 					
	}
	
	public void reloadMapProperty(@NonNull RDFPropertyType prop) {
		var named = (OntObjectProperty.Named) prop.getProperty();
		collectionPropertyCache.put(prop.getId(),  new MapWrapper(prop.getValueType().getAsPrimitiveOrClass(), 
				resolver, 
				MapResource.asUnsafeMapResource(this.element, named, resolver.getMetaschemata().getMapType()))); 
	}

	private Object getPropertyAsList(@NonNull RDFPropertyType prop) {
		var named = (OntObjectProperty.Named) prop.getProperty();
		return collectionPropertyCache.computeIfAbsent(prop.getId(),  k -> new ListWrapper(this.element, named, resolver, prop.getValueType().getAsPrimitiveOrClass()));
	}
	
	private Object getPropertyAsSet(@NonNull RDFPropertyType prop) {
		return collectionPropertyCache.computeIfAbsent(prop.getId(),  k -> new SetWrapper(this.element, prop.getProperty(), resolver, prop.getValueType().getAsPrimitiveOrClass()));
	}
	
	private Object getSingleProperty(@NonNull RDFPropertyType prop) {
		var stmt = element.getProperty(prop.getProperty().asProperty());
		if (stmt == null) return null; // when there is no value available yet.
		if (prop.getValueType().isPrimitiveType() ) { // then a  literal
			return stmt.getLiteral().getValue();
		} else { // a resource
			return resolver.resolveToRDFElement(stmt.getObject());
		}
	}

	public <T> T getTypedProperty(@NonNull String property, @NonNull Class<T> clazz, T defaultValue) {
		T value = getTypedProperty(property, clazz);
		if (value == null) {
			return defaultValue;
		} else {
			return value;
		}
	}
	

	public void put(String property, String key, Object value) {
		var optProp = resolveToPropertyType(property);
		if (optProp.isPresent()) {
			var prop = optProp.get();
			((Map<String, Object>) getPropertyAsMap(prop)).put(key, value);
		} else {
			throw new IllegalArgumentException(String.format("Property %s does not exist on element %s", property, element.getURI()));
		}
	}

	public void add(String property, Object value) {
		var optProp = resolveToPropertyType(property);
		if (optProp.isPresent()) {
			var prop = optProp.get();
			if (prop.getCardinality().equals(Cardinalities.LIST)) {
				((List<Object>) getPropertyAsList(prop)).add(value);
			} else
			if (prop.getCardinality().equals(Cardinalities.SET)) {
				((Set<Object>) getPropertyAsSet(prop)).add(value);
			} else {
				throw new IllegalArgumentException(String.format("Property %s on element %s cannot be called with add()", property, element.getURI()));
			}
		} else {
			throw new IllegalArgumentException(String.format("Property %s does not exist on element %s", property, element.getURI()));
		}
	}

	@Override
	public String toString() {
		return "RDFElement [" + getId() + "]";
	}
}

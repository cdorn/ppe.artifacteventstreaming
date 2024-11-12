package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.CARDINALITIES;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RDFElement {

	@Getter
	protected final OntObject element;
	protected final NodeToDomainResolver resolver;

	private boolean isDeleted = false; // in case someone holds on to wrapper object

	public String getId() {
		return element.getURI();
	}

	public String getName() {
		return element.getLocalName();
	}
	

	public void markAsDeleted() {
		this.isDeleted = true;
		element.removeProperties();		
		//remove from cache needs to be done in childclasses
	}


	public boolean isMarkedAsDeleted() {
		//we dont mark anything deleted, we just delete (for now)
		return isDeleted;
	}


	public boolean isOwner(String ownerId) {
		return getOwners().contains(ownerId);
	}


	public void addOwner(String ownerId) {
		element.addProperty(PPECORE.owner, ownerId);
	}


	public Set<String> getOwners() {
		StmtIterator iter = element.listProperties(PPECORE.owner);
		Set<String> owners = new HashSet<>();
		while (iter.hasNext()){
			owners.add(iter.next().getString());
		}
		return owners;
	}
	

	public PPEInstanceType getInstanceType() {
		var stmt = element.getProperty(RDF.type); // for now an arbitrary one of there are multiple ones
		//FIXME: get all classes and filter out restrictions!
		if (stmt != null) {
			var node = stmt.getObject();
			return resolver.resolveToType(node);
		}
		return null;
	}

	/**
	 * Expects fully qualified named properties , i.e., as they are used at the RDF layer
	 */
	public void setSingleProperty(String property, Object value) {
		var prop = resolveProperty(property, CARDINALITIES.SINGLE);
		if (value instanceof RDFElement inst) { // then a resource and not a literal
			element
				.removeAll(prop.asProperty())
				.addProperty(prop.asProperty(), inst.getElement());
		} else {
			element
				.removeAll(prop.asProperty())
				.addLiteral(prop.asProperty(),  value);
		}
	}

	protected String makePropertyURI(String property) {
		if (!property.startsWith("http")) { //FIXME: proper uri check
			return this.element.getURI()+"#"+property;
		} else {
			return property;
		}
	}
	
	protected OntRelationalProperty resolveProperty(String property, CARDINALITIES... expectedCardinalities) {
		var expCardi = Set.of(expectedCardinalities);
		property = makePropertyURI(property);

		RDFInstanceType type = (RDFInstanceType) getInstanceType();
		if (type == null) { // we are untyped, thus return just the property, consumer needs to know what they are doing
			OntRelationalProperty prop = element.getModel().getObjectProperty(property);
			if (prop == null)
				prop = element.getModel().getDataProperty(property);
			if (prop == null) {
				throw new IllegalArgumentException("Property unknown: "+property);
			}
			return prop;
		} else { // check if the cardinality matches
			RDFPropertyType pType = (RDFPropertyType) type.getPropertyType(property);
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
	
	protected Optional<RDFPropertyType> resolveToPropertyType(String property) {
		property = makePropertyURI(property);
		RDFInstanceType type = (RDFInstanceType) getInstanceType();
		if (type != null) {
			return Optional.ofNullable( (RDFPropertyType) type.getPropertyType(property));
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
			if (prop.getCardinality().equals(CARDINALITIES.SINGLE)) {
				return (T) getSingleProperty(prop);
			}
			if (Set.class.isAssignableFrom(clazz) && prop.getCardinality().equals(CARDINALITIES.SET)) {
				return (T) getPropertyAsSet(prop);
			} 
			if (List.class.isAssignableFrom(clazz) && prop.getCardinality().equals(CARDINALITIES.LIST)) {
				return (T) getPropertyAsList(prop);
			} 
			if (Map.class.isAssignableFrom(clazz) && prop.getCardinality().equals(CARDINALITIES.MAP)) {
				return (T) getPropertyAsMap(prop);
			}	
			throw new IllegalArgumentException(String.format("Property %s does exist on element %s but of cardinality %s incompatible with clazz %s "
					, property, element.getURI(), prop.getCardinality(), clazz.getName()));
		}
	}
	
	private Object getPropertyAsMap(@NonNull RDFPropertyType prop) {
		var named = (OntObjectProperty.Named) prop.getProperty();
		try { // lets not cache anything
			return new MapWrapper(MapResource.asMapResource(this.element, named, resolver.getMapBase(), resolver.resolveTypeToClassOrDatarange(prop.getInstanceType())), resolver);
		} catch (ResourceMismatchException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	private Object getPropertyAsList(@NonNull RDFPropertyType prop) {
		var named = (OntObjectProperty.Named) prop.getProperty();
		return new ListWrapper(this.element, named, resolver);
	}
	
	private Object getPropertyAsSet(@NonNull RDFPropertyType prop) {
		if (BuildInType.isAtomicType(prop.getInstanceType()) ) { // then a set of literals
			
		} else { //set of elements
			
		}
		return null;
	}

	private Object getSingleProperty(@NonNull RDFPropertyType prop) {
		var stmt = element.getProperty(prop.getProperty().asProperty());
		if (stmt == null) return null; // when there is no value available yet.
		if (BuildInType.isAtomicType(prop.getInstanceType()) ) { // then a  literal
			return stmt.getLiteral().getValue();
		} else { // a resource
			return resolver.resolveToRDFElement(stmt.getResource());
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
		// TODO Auto-generated method stub

	}

	public void add(String property, Object value) {
		// TODO Auto-generated method stub

	}
}

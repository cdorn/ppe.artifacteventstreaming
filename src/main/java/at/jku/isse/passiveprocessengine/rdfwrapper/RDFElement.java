package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;

import com.ctc.wstx.shaded.msv_core.util.Uri;

import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import lombok.Getter;
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
		var prop = resolveProperty(property);
		if (value instanceof RDFElement inst) { // then a resource and not a literal
			element
				.removeAll(prop)
				.addProperty(prop, inst.getElement());
		} else {
			element
				.removeAll(prop)
				.addLiteral(prop,  value);
		}
	}

	private OntRelationalProperty.Named resolveProperty(String property) {
		if (Uri.isValid(property)) {			
			var objProp = element.getModel().getObjectProperty(property);
			if (objProp != null)
				return objProp;
			else 
				return element.getModel().getDataProperty(property);
		} else {		
			var objProp = element.getModel().getObjectProperty(this.element.getURI()+"#"+property);
			if (objProp != null)
				return objProp;
			else 
				return element.getModel().getDataProperty(this.element.getURI()+"#"+property);
		}
	}

	public <T> T getTypedProperty(String property, Class<T> clazz) {
		
		OntRelationalProperty prop = resolveProperty(property);
		if (prop == null);
		
		else {
		// check if single or collection property
		var iter = element.listProperties(prop);
		if (Set.class.isAssignableFrom(clazz)) {
			if (iter.hasNext()) {
				
			} else {
				return (T) Collections.emptySet();
			}
		} else 
		if (List.class.isAssignableFrom(clazz)) {
				if (iter.hasNext()) {
					
				} else {
					return (T) Collections.emptyList();
				}
		} else
		if (Map.class.isAssignableFrom(clazz)) {
				if (iter.hasNext()) {
					MapResource.asMapResource(this.element, prop, resolver.getMapBase());
				} else {
					return (T) Collections.emptyMap();
				}
		}
		
		
		// TODO Auto-generated method stub
		return null;
		}
	}

	public <T> T getTypedProperty(String property, Class<T> clazz, T defaultValue) {
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

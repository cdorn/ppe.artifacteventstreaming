package at.jku.isse.artifacteventstreaming.rdf;

import java.util.Set;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.vocabulary.RDF;

import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RDFInstance implements PPEInstance {

	private final OntIndividual instance;
	private final NodeToDomainResolver resolver;
	
	
	@Override
	public String getId() {
		return instance.getURI();
	}

	@Override
	public String getName() {
		return instance.getLabel();
	}

	@Override
	public PPEInstanceType getInstanceType() {
		var stmt = instance.getProperty(RDF.type); // for now an arbitrary one of there are multiple ones
		if (stmt != null) {
			var node = stmt.getObject();
			return resolver.resolveToType(node);
		}
		return null;
	}

	@Override
	public void markAsDeleted() {
		instance.removeProperties();			
	}

	@Override
	public boolean isMarkedAsDeleted() {
		//we dont mark anything deleted, we just delete (for now)
		return false;
	}

	@Override
	public boolean isOwner(String ownerId) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void addOwner(String ownerId) {
		// TODO Auto-generated method stub

	}

	@Override
	public Set<String> getOwners() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSingleProperty(String property, Object value) {
		// TODO Auto-generated method stub

	}

	@Override
	public <T> T getTypedProperty(String property, Class<T> clazz) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T getTypedProperty(String property, Class<T> clazz, T defaultValue) {
		T value = getTypedProperty(property, clazz);
		if (value == null) {
			return defaultValue;
		} else {
			return value;
		}
	}

	@Override
	public void setInstanceType(PPEInstanceType childType) {
		// TODO Auto-generated method stub

	}

	@Override
	public void put(String property, String key, Object value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void add(String property, Object value) {
		// TODO Auto-generated method stub

	}

}

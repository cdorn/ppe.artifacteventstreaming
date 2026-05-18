package at.jku.isse.artifacteventstreaming.schemasupport;

import lombok.NonNull;
import org.apache.jena.ontapi.model.*;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BasePropertyType {
	public final Set<String> propertyUriCache = new HashSet<>();

	public BasePropertyType(OntModel model) {
		fillCache(model);
	}

	private void fillCache(OntModel model) {
		model.properties().forEach(prop -> propertyUriCache.add(prop.getURI()));
	}
	
	public boolean existsPrimaryProperty(String uri) {
		//return model.getGraph().contains(ResourceFactory.createResource(uri).asNode(), RDF.Nodes.type, Node.ANY);
		return propertyUriCache.contains(uri);
	}
	
	public Set<String> getKnownPropertyURIs() {
		return new HashSet<>(propertyUriCache);
	}

	public OntObjectProperty createBaseObjectPropertyType(@NonNull OntModel model, @NonNull String propUri, @NonNull List<OntClass> domains, @NonNull OntClass range ) {
		if (existsPrimaryProperty(propUri))
			return null;
		var prop = model.createObjectProperty(propUri);
		domains.forEach(prop::addDomain);
		prop.addRange(range);
		propertyUriCache.add(propUri);
		return prop;
	}
	
	public OntDataProperty createBaseDataPropertyType(@NonNull OntModel model, @NonNull String propUri, @NonNull List<OntClass> domains, @NonNull OntDataRange range ) {
		if (existsPrimaryProperty(propUri))
			return null;
		var prop = model.createDataProperty(propUri);
		domains.forEach(prop::addDomain);		
		prop.addRange(range);			
		propertyUriCache.add(propUri);
		return prop;	
	}
	
	public void removeBaseProperty(@NonNull OntProperty ontProperty) {
		propertyUriCache.remove(ontProperty.getURI());
		ontProperty.removeProperties();
	}

	public void removePropertyURIfromCache(String propertyURI) {
		propertyUriCache.remove(propertyURI);
	}

	public void addToCache(String propertyURI) {
		propertyUriCache.add(propertyURI);
	}
}
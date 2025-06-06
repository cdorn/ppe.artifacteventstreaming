package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.vocabulary.RDF;

import lombok.NonNull;

public class BasePropertyType {
	public final Set<String> propertyCache = new HashSet<>();

	public BasePropertyType(OntModel model) {
		fillCache(model);
	}

	private void fillCache(OntModel model) {
		var iter = model.listResourcesWithProperty(RDF.type, RDF.Nodes.Property);
		while (iter.hasNext()) {
			propertyCache.add(iter.next().getURI());
		}
	}
	
	public boolean existsPrimaryProperty(String uri) {
		//return model.getGraph().contains(ResourceFactory.createResource(uri).asNode(), RDF.Nodes.type, Node.ANY);
		return propertyCache.contains(uri);
	}
	
	public Set<String> getKnownPropertyURIs() {
		return new HashSet<>(propertyCache);
	}

	public OntObjectProperty createBaseObjectPropertyType(@NonNull String propUri, @NonNull OntClass domain, @NonNull OntClass range ) {
		//if (domain.getModel().getObjectProperty(propUri) != null)
		if (existsPrimaryProperty(propUri))
			return null;
		var prop = domain.getModel().createObjectProperty(propUri);
		prop.addRange(range);
		prop.addDomain(domain);
		propertyCache.add(propUri);
		return prop;
	}
	
	public OntDataProperty createBaseDataPropertyType(@NonNull OntModel model, @NonNull String propUri, @NonNull List<OntClass> domains, @NonNull OntDataRange range ) {				
		//if (model.getDataProperty(propUri) != null)
		if (existsPrimaryProperty(propUri))
			return null;
		var prop = model.createDataProperty(propUri);
		domains.forEach(prop::addDomain);		
		prop.addRange(range);			
		propertyCache.add(propUri);
		return prop;	
	}
	
	public void removeBaseProperty(@NonNull OntProperty ontProperty) {
		propertyCache.remove(ontProperty.getURI());
		ontProperty.removeProperties();
	}

	public void removePropertyURIfromCache(String propertyURI) {
		propertyCache.remove(propertyURI);
	}

	public void addToCache(String propertyURI) {
		propertyCache.add(propertyURI);
	}
}
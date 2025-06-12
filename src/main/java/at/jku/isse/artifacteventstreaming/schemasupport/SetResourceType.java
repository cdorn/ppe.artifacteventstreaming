package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.List;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntObjectProperty;

import lombok.NonNull;

public class SetResourceType {

	private final BasePropertyType primaryType;
	
	public SetResourceType( @NonNull BasePropertyType primaryType) {
		this.primaryType = primaryType;
	}
	
	public OntDataProperty createDataPropertyType( @NonNull String propUri, @NonNull OntClass domain, @NonNull OntDataRange range ) {				
		return primaryType.createBaseDataPropertyType(domain.getModel(), propUri, List.of(domain), range);
//		var prop = domain.getModel().createDataProperty(propUri);
//		prop.addDomain(domain);		
//		prop.addRange(range);			
//		return prop;	
	}

	public OntObjectProperty createObjectPropertyType(@NonNull String propUri, @NonNull OntClass domain, @NonNull OntClass range ) {
		return primaryType.createBaseObjectPropertyType(propUri, domain, range);
//		var prop = domain.getModel().createObjectProperty(propUri);
//		prop.addDomain(domain);		
//		prop.addRange(range);			
//		return prop;	
	}

	public void removePropertyURIfromCache(String propertyURI) {
		// nothing to do, as all properties are removed via base/primary property cache,
		// we do however would have stale subclasses cache entries (not a problem as long as we just override stale entries)
	}
}

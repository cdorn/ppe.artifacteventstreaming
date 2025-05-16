package at.jku.isse.artifacteventstreaming.schemasupport;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntObjectProperty;

import lombok.NonNull;

public class SetResourceType {

	public OntDataProperty createDataPropertyType( @NonNull String propUri, @NonNull OntClass domain, @NonNull OntDataRange range ) {				
		var prop = domain.getModel().createDataProperty(propUri);
		prop.addDomain(domain);		
		prop.addRange(range);			
		return prop;	
	}

	public OntObjectProperty createObjectPropertyType(@NonNull String propUri, @NonNull OntClass domain, @NonNull OntClass range ) {
		var prop = domain.getModel().createObjectProperty(propUri);
		prop.addDomain(domain);		
		prop.addRange(range);			
		return prop;	
	}
}

package at.jku.isse.artifacteventstreaming.schemasupport;

import lombok.NonNull;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntObjectProperty;

import java.util.List;

public class SetResourceType {

	private final BasePropertyType primaryType;
	
	public SetResourceType( @NonNull BasePropertyType primaryType) {
		this.primaryType = primaryType;
	}
	
	public OntDataProperty createDataPropertyType( @NonNull String propUri, @NonNull OntClass domain, @NonNull OntDataRange range ) {				
		return primaryType.createBaseDataPropertyType(domain.getModel(), propUri, List.of(domain), range);
	}

	public OntObjectProperty createObjectPropertyType(@NonNull String propUri, @NonNull OntClass domain, @NonNull OntClass range ) {
		return primaryType.createBaseObjectPropertyType(domain.getModel(), propUri, List.of(domain), range);
	}

	public OntDataProperty createDataPropertyType( @NonNull String propUri, @NonNull List<OntClass> domains, @NonNull OntDataRange range ) {
		return primaryType.createBaseDataPropertyType(domains.getFirst().getModel(), propUri, domains, range);
	}

	public OntObjectProperty createObjectPropertyType(@NonNull String propUri, @NonNull List<OntClass> domains, @NonNull OntClass range ) {
		return primaryType.createBaseObjectPropertyType(domains.getFirst().getModel(), propUri, domains, range);
	}

	public void removePropertyURIfromCache(String propertyURI) {
		// nothing to do, as all properties are removed via base/primary property cache,
	}
}

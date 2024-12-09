package at.jku.isse.artifacteventstreaming.schemasupport;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntObjectProperty;

public class SchemaUtils {
	
	private SingleResourceType singleType;
	

	
	private void initSingleType(OntClass domain) {
		if (singleType == null) {
			singleType = new SingleResourceType(domain.getModel());
		}
	}
	
	
	
}

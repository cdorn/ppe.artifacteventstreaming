package at.jku.isse.artifacteventstreaming;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntObjectProperty;

public class SchemaUtils {

	public static OntDataProperty createBaseDataPropertyType(String propUri, OntClass domain, OntDataRange range ) {
		if (domain.getModel().getDataProperty(propUri) != null)
			return null;
		var prop = domain.getModel().createDataProperty(propUri);
		prop.addDomain(domain);
		prop.addRange(range);			
		return prop;	
	}

	public static OntObjectProperty createBaseObjectPropertyType(String propUri, OntClass domain, OntClass range ) {
		if (domain.getModel().getObjectProperty(propUri) != null)
			return null;
		var prop = domain.getModel().createObjectProperty(propUri);
		prop.addRange(range);
		prop.addDomain(domain);	
		return prop;
	}

	public static OntDataProperty createSingleDataPropertyType(String propURI, OntClass domain, OntDataRange range) {
		var prop = createBaseDataPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = domain.getModel().createDataMaxCardinality((OntDataProperty) prop, 1, null);
			domain.addSuperClass(maxOneProp);
		}
		return prop;
	}
	

	public static OntObjectProperty createSingleObjectPropertyType(String propURI, OntClass domain, OntClass range) {
		var prop = createBaseObjectPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = domain.getModel().createObjectMaxCardinality((OntObjectProperty) prop, 1, null);
			domain.addSuperClass(maxOneProp);
		}
		return prop;
	}
	
}

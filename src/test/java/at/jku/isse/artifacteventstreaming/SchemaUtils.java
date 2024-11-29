package at.jku.isse.artifacteventstreaming;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntObjectProperty;

import at.jku.isse.passiveprocessengine.rdfwrapper.SingleResourceType;

public class SchemaUtils {
	
	private static SingleResourceType singleType;
	
	private static void initSingleType(OntClass domain) {
		if (singleType == null) {
			singleType = new SingleResourceType(domain.getModel());
		}
	}
	
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
		initSingleType(domain);
		var prop = createBaseDataPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = domain.getModel().createDataMaxCardinality((OntDataProperty) prop, 1, null);
			domain.addSuperClass(maxOneProp);
			singleType.getSingleLiteralProperty().addSubProperty(prop);
		}
		return prop;
	}
	

	public static OntObjectProperty createSingleObjectPropertyType(String propURI, OntClass domain, OntClass range) {
		initSingleType(domain);
		var prop = createBaseObjectPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = domain.getModel().createObjectMaxCardinality((OntObjectProperty) prop, 1, null);
			domain.addSuperClass(maxOneProp);
			singleType.getSingleObjectProperty().addSubProperty(prop);
		}
		return prop;
	}
	
}

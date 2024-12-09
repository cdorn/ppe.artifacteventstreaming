package at.jku.isse.artifacteventstreaming.schemasupport;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;

import lombok.Getter;

public class PropertyCardinalityTypes {

	protected final OntModel model;
	@Getter
	private MapResourceType mapType;
	@Getter
	private ListResourceType listType;
	@Getter
	private SingleResourceType singleType;

	public PropertyCardinalityTypes(OntModel model) {
		this.model = model;
		mapType = new MapResourceType(model); //TODO make these from ontology
		listType = new ListResourceType(model);
		singleType = new SingleResourceType(model);
	}

	public OntDataProperty createBaseDataPropertyType(String propUri, OntClass domain, OntDataRange range ) {
		if (domain.getModel().getDataProperty(propUri) != null)
			return null;
		var prop = domain.getModel().createDataProperty(propUri);
		prop.addDomain(domain);
		prop.addRange(range);			
		return prop;	
	}

	public OntObjectProperty createBaseObjectPropertyType(String propUri, OntClass domain, OntClass range ) {
		if (domain.getModel().getObjectProperty(propUri) != null)
			return null;
		var prop = domain.getModel().createObjectProperty(propUri);
		prop.addRange(range);
		prop.addDomain(domain);	
		return prop;
	}



	public OntDataProperty createSingleDataPropertyType(String propURI, OntClass domain, OntDataRange range) {
		var prop = createBaseDataPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = domain.getModel().createDataMaxCardinality(prop, 1, null);
			domain.addSuperClass(maxOneProp);
			singleType.getSingleLiteralProperty().addSubProperty(prop);
		}
		return prop;
	}


	public OntObjectProperty createSingleObjectPropertyType(String propURI, OntClass domain, OntClass range) {
		var prop = createBaseObjectPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = domain.getModel().createObjectMaxCardinality(prop, 1, null);
			domain.addSuperClass(maxOneProp);
			singleType.getSingleObjectProperty().addSubProperty(prop);
		}
		return prop;
	}
}

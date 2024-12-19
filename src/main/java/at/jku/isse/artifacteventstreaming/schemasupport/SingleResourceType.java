package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.List;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.ontapi.impl.OntGraphModelImpl;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.ontapi.model.OntObjectProperty.Named;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;

import lombok.Getter;
import lombok.NonNull;

public class SingleResourceType {
	public static final String SINGLE_NS = "http://at.jku.isse.single#";
	
	public static final String SINGLE_OBJECT_URI = SINGLE_NS+"object";
	public static final String SINGEL_LITERAL_URI = SINGLE_NS+"literal";
	
	private static SingleSchemaFactory factory = new SingleSchemaFactory();
	
	@Getter
	private final OntObjectProperty singleObjectProperty;
	@Getter
	private final OntDataProperty singleLiteralProperty;
	
	public SingleResourceType(OntModel model) {
		factory.addSchemaToModel(model);	
		singleObjectProperty = model.getObjectProperty(SINGLE_OBJECT_URI);
		singleLiteralProperty = model.getDataProperty(SINGEL_LITERAL_URI);
	}
	
	public OntDataProperty createBaseDataPropertyType(String propUri, OntClass domain, OntDataRange range ) {
		return createBaseDataPropertyType(domain.getModel(), propUri, List.of(domain), range);
	}

	public OntDataProperty createBaseDataPropertyType(@NonNull OntModel model, @NonNull String propUri, @NonNull List<OntClass> domains, @NonNull OntDataRange range ) {
		if (model.getDataProperty(propUri) != null)
			return null;
		var prop = model.createDataProperty(propUri);
		domains.forEach(prop::addDomain);		
		prop.addRange(range);			
		return prop;	
	}

	public OntObjectProperty createBaseObjectPropertyType(@NonNull String propUri, @NonNull OntClass domain, @NonNull OntClass range ) {
		if (domain.getModel().getObjectProperty(propUri) != null)
			return null;
		var prop = domain.getModel().createObjectProperty(propUri);
		prop.addRange(range);
		prop.addDomain(domain);	
		return prop;
	}

	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntDataRange range) {
		var prop = createBaseDataPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = getMaxOneDataCardinalityRestriction(domain.getModel(), prop, range);
			domain.addSuperClass(maxOneProp);
			getSingleLiteralProperty().addSubProperty(prop);
		}
		return prop;
	}

	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull List<OntClass> domains, @NonNull OntDataRange range) {
		var localModel = domains.get(0).getModel();
		var prop = createBaseDataPropertyType(localModel, propURI, domains, range);
		if (prop != null) {
			var maxOneProp = getMaxOneDataCardinalityRestriction(localModel, prop, range);
			domains.forEach(domain -> domain.addSuperClass(maxOneProp));			
			getSingleLiteralProperty().addSubProperty(prop);
		}
		return prop;
	}

	public OntObjectProperty createSingleObjectPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntClass range) {
		var prop = createBaseObjectPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = getMaxOneObjectCardinalityRestriction(domain.getModel(), prop, range);
			domain.addSuperClass(maxOneProp);
			getSingleObjectProperty().addSubProperty(prop);
		}
		return prop;
	}

	
	public OntClass getMaxOneObjectCardinalityRestriction(OntModel model, OntProperty onProperty, OntClass type) {
		var restr = createQualifiedMaxOneRestriction(model, onProperty, type);
		return ((OntGraphModelImpl)model).getNodeAs(restr.asNode(), OntClass.ObjectMaxCardinality.class);
	}
	
	public OntClass getMaxOneDataCardinalityRestriction(OntModel model, OntProperty onProperty, OntDataRange type) {
		var restr = createQualifiedMaxOneRestriction(model, onProperty, type);
		return ((OntGraphModelImpl)model).getNodeAs(restr.asNode(), OntClass.DataMaxCardinality.class);
	}
	
	private Resource createQualifiedMaxOneRestriction(OntModel model, OntProperty onProperty, OntObject rangeOrClass) {
		var lit1 = ResourceFactory.createTypedLiteral("1", XSDDatatype.XSDnonNegativeInteger);
		var anonId = new AnonId("anon::"+onProperty.getURI()+"::maxOneCardinality"); //otherwise uri check will succeed, which we dont want.
		var restrRes = model.createResource(anonId)
				.addProperty(RDF.type, OWL2.Restriction) // if we set a uri then Jena cant wrap the node as a datamaxcardinality
				.addProperty(OWL2.onProperty, onProperty);		
		var isQualified = rangeOrClass instanceof OntClass;
		var pred = isQualified ? OWL2.maxQualifiedCardinality : OWL2.maxCardinality;
		model.add(restrRes, pred, lit1);
		if (isQualified) {
			model.add(restrRes, onProperty instanceof OntObjectProperty ? OWL2.onClass : OWL2.onDataRange, rangeOrClass);
		}
		return restrRes;
	}
	
	private static class SingleSchemaFactory extends SchemaFactory {
		
		public static final String SINGLEONTOLOGY = "singlevalueontology";
		private final OntModel model;
		
		public SingleSchemaFactory() {
			this.model = loadOntologyFromFilesystem(SINGLEONTOLOGY);			
			initTypes();			
			super.writeOntologyToFilesystem(model, SINGLEONTOLOGY);
		}				
		
		private void initTypes() {
			var singleObjectProperty = model.getObjectProperty(SINGLE_OBJECT_URI);
			if (singleObjectProperty == null) {
				singleObjectProperty = model.createObjectProperty(SINGLE_OBJECT_URI);
			}
			var singleLiteralProperty = model.getDataProperty(SINGEL_LITERAL_URI);
			if (singleLiteralProperty == null) {
				singleLiteralProperty = model.createDataProperty(SINGEL_LITERAL_URI);
			}
		}

		public void addSchemaToModel(Model modelToAddOntologyTo) {
			modelToAddOntologyTo.add(model);		
		} 
	}
}

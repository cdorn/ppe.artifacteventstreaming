package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import lombok.Getter;
import lombok.NonNull;

public class SingleResourceType {
	public static final String SINGLE_NS = "http://at.jku.isse.single#";
	
	public static final String SINGLE_OBJECT_URI = SINGLE_NS+"object";
	public static final String SINGEL_LITERAL_URI = SINGLE_NS+"literal";
	
	@Getter
	private final OntObjectProperty singleObjectProperty;
	@Getter
	private final OntDataProperty singleLiteralProperty;
	
	private final Set<OntRelationalProperty> objectSubpropertyCache = new HashSet<>();
	private final Set<OntRelationalProperty> dataSubpropertyCache = new HashSet<>();
	
	private final Set<String> propertyCache = new HashSet<>();
	
	public SingleResourceType(OntModel model) {			
		singleObjectProperty = model.getObjectProperty(SINGLE_OBJECT_URI);
		singleLiteralProperty = model.getDataProperty(SINGEL_LITERAL_URI);
		fillCaches(model);		
	}
	
	private void fillCaches(OntModel model) {		
		var iter = model.listResourcesWithProperty(RDF.type, RDF.Nodes.Property);
		while (iter.hasNext()) {
			propertyCache.add(iter.next().getURI());
		}
		singleObjectProperty.subProperties().forEach(objectSubpropertyCache::add);
		singleLiteralProperty.subProperties().forEach(dataSubpropertyCache::add);
	}
	
	public boolean isSingleProperty(OntRelationalProperty prop) {
		return dataSubpropertyCache.contains(prop) || objectSubpropertyCache.contains(prop);
	}
	
	/*
	 * checks if a property exists for that URI, if that property has been created via this object.
	 * */
	public boolean existsPrimaryProperty(String uri) {
		//return model.getGraph().contains(ResourceFactory.createResource(uri).asNode(), RDF.Nodes.type, Node.ANY);
		return propertyCache.contains(uri);
	}
	
	public OntDataProperty createBaseDataPropertyType(String propUri, OntClass domain, OntDataRange range ) {
		return createBaseDataPropertyType(domain.getModel(), propUri, List.of(domain), range);
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

	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntDataRange range) {
		var prop = createBaseDataPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = getMaxOneDataCardinalityRestriction(domain.getModel(), prop, range);
			domain.addProperty(RDFS.subClassOf, maxOneProp);
			singleLiteralProperty.addSubProperty(prop);
			dataSubpropertyCache.add(prop);
		}
		return prop;
	}

	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull List<OntClass> domains, @NonNull OntDataRange range) {
		var localModel = domains.get(0).getModel();
		var prop = createBaseDataPropertyType(localModel, propURI, domains, range);
		if (prop != null) {
			var maxOneProp = getMaxOneDataCardinalityRestriction(localModel, prop, range);
			domains.forEach(domain -> domain.addProperty(RDFS.subClassOf, maxOneProp));			//domain.addSuperClass(maxOneProp)
			singleLiteralProperty.addSubProperty(prop);
			dataSubpropertyCache.add(prop);
		}
		return prop;
	}

	public OntObjectProperty createSingleObjectPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntClass range) {
		var prop = createBaseObjectPropertyType(propURI, domain, range);
		if (prop != null) {
			var maxOneProp = getMaxOneObjectCardinalityRestriction(domain.getModel(), prop, range);
			//domain.addSuperClass(maxOneProp);
			domain.addProperty(RDFS.subClassOf, maxOneProp);			
			singleObjectProperty.addSubProperty(prop);
			objectSubpropertyCache.add(prop);
		}
		return prop;
	}

	
	public Resource getMaxOneObjectCardinalityRestriction(OntModel model, OntProperty onProperty, OntClass type) {
		return createQualifiedMaxOneRestriction(model, onProperty, type);
		//return ((OntGraphModelImpl)model).getNodeAs(restr.asNode(), OntClass.ObjectMaxCardinality.class);
	}
	
	public Resource getMaxOneDataCardinalityRestriction(OntModel model, OntProperty onProperty, OntDataRange type) {
		return createQualifiedMaxOneRestriction(model, onProperty, type);
		//return ((OntGraphModelImpl)model).getNodeAs(restr.asNode(), OntClass.DataMaxCardinality.class);
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
	
	
	
	protected static class SingleSchemaFactory {
		
		private final OntModel model;
		
		public SingleSchemaFactory(OntModel metaOntology) {
			this.model = metaOntology;			
			initTypes();						
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

	}
}

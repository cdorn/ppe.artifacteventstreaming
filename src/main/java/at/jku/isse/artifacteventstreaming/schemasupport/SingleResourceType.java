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
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Property;
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
	public static final String SINGLE_LITERAL_URI = SINGLE_NS+"literal";
	
	@Getter
	private final OntObjectProperty singleObjectProperty;
	@Getter
	private final OntDataProperty singleLiteralProperty;
	
	private final Set<OntProperty> objectSubpropertyCache = new HashSet<>();
	private final Set<OntProperty> dataSubpropertyCache = new HashSet<>();
	
	public final BasePropertyType primaryPropertyType;

	public SingleResourceType(OntModel model, BasePropertyType primaryPropertyType) {	
		this.primaryPropertyType = primaryPropertyType;
		singleObjectProperty = model.getObjectProperty(SINGLE_OBJECT_URI);
		singleLiteralProperty = model.getDataProperty(SINGLE_LITERAL_URI);
		fillCaches();		
	}
	
	private void fillCaches() {		
		singleObjectProperty.subProperties().forEach(objectSubpropertyCache::add);
		singleLiteralProperty.subProperties().forEach(dataSubpropertyCache::add);
	}
	
	public boolean isSingleProperty(OntProperty prop) {
		return dataSubpropertyCache.contains(prop) || objectSubpropertyCache.contains(prop);
	}
	
	public OntDataProperty createBaseDataPropertyType(String propUri, OntClass domain, OntDataRange range ) {
		return primaryPropertyType.createBaseDataPropertyType(domain.getModel(), propUri, List.of(domain), range);
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
		var prop = primaryPropertyType.createBaseDataPropertyType(localModel, propURI, domains, range);
		if (prop != null) {
			var maxOneProp = getMaxOneDataCardinalityRestriction(localModel, prop, range);
			domains.forEach(domain -> domain.addProperty(RDFS.subClassOf, maxOneProp));			//domain.addSuperClass(maxOneProp)
			singleLiteralProperty.addSubProperty(prop);
			dataSubpropertyCache.add(prop);
		}
		return prop;
	}

	public OntObjectProperty createSingleObjectPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntClass range) {
		var prop = primaryPropertyType.createBaseObjectPropertyType(propURI, domain, range);
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
		var anonId = createSingleRestrictionAnonId(onProperty);
		var restrRes = model.createResource(anonId)
				.addProperty(RDF.type, OWL2.Restriction) // if we set a uri then Jena cant wrap the node as a datamaxcardinality
				.addProperty(OWL2.onProperty, onProperty);		
		//var isQualified = rangeOrClass instanceof OntClass;
		var isQualified = isQualified(rangeOrClass);
		var pred = isQualified ? OWL2.maxQualifiedCardinality : OWL2.maxCardinality;
		model.add(restrRes, pred, lit1);
		if (isQualified) {
			model.add(restrRes, onProperty instanceof OntObjectProperty ? OWL2.onClass : OWL2.onDataRange, rangeOrClass);
		}
		return restrRes;
	}

	private AnonId createSingleRestrictionAnonId(OntProperty onProperty) {
		return new AnonId("anon::"+onProperty.getURI()+"::maxOneCardinality");
	}
	
	private boolean isQualified(OntObject rangeOrClass) {
		return !(OWL2.Thing.equals(rangeOrClass) || RDFS.Literal.equals(rangeOrClass));
	}
	
	public void removeSingleProperty(@NonNull OntClass owner, @NonNull OntProperty ontProperty) {
		var model = ontProperty.getModel();
		// first, remove restriction
		var anonId = createSingleRestrictionAnonId(ontProperty);
		var restrRes = model.createResource(anonId); // only way to retrieve anon resource again
		owner.remove(RDFS.subClassOf, restrRes); // remove the restriction from property owning class
		restrRes.removeProperties();
		// remove from cache (we try both)
		objectSubpropertyCache.remove(ontProperty); 
		dataSubpropertyCache.remove(ontProperty);
		// then remove other property predicates
		primaryPropertyType.removeBaseProperty(ontProperty);
	}
	
	public void removePropertyURIfromCache(String propertyURI) {
		objectSubpropertyCache.stream()
		.filter(prop -> prop.getURI().equals(propertyURI))
		.findAny()
		.ifPresent(objectSubpropertyCache::remove);
		dataSubpropertyCache.stream()
		.filter(prop -> prop.getURI().equals(propertyURI))
		.findAny()
		.ifPresent(dataSubpropertyCache::remove);
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
				model.createObjectProperty(SINGLE_OBJECT_URI);
			}
			var singleLiteralProperty = model.getDataProperty(SINGLE_LITERAL_URI);
			if (singleLiteralProperty == null) {
				model.createDataProperty(SINGLE_LITERAL_URI);
			}
		}

	}

	public boolean addIfIsSinglePropertyBasedOnSuperProperty(Property genProp) {
		if (genProp.canAs(OntDataProperty.class)) {
			var ontDataProp = genProp.as(OntDataProperty.class);
			if (ontDataProp.superProperties().anyMatch(prop -> prop.equals(singleLiteralProperty))) {
				dataSubpropertyCache.add(ontDataProp);
				return true;
			} else
				return false; // nothing added
		}
		if (genProp.canAs(OntObjectProperty.class)) {
			var ontObjProp = genProp.as(OntObjectProperty.class);
			if (ontObjProp.superProperties().anyMatch(prop -> prop.equals(singleObjectProperty))) {
				objectSubpropertyCache.add(ontObjProp);
				return true;
			}
				return false; // nothing added
		}
		return false;
	}


}

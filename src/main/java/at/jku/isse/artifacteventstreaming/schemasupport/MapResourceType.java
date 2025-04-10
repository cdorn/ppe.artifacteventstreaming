package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.XSD;

import lombok.Getter;

public class MapResourceType  {

	public static final String OBJECT_VALUE = "objectValue";
	public static final String LITERAL_VALUE = "literalValue";
	private static final String ENTRY_TYPE = "EntryType";
	public static final String MAP_NS = "http://at.jku.isse.map#";	
	public static final String ENTRY_TYPE_URI = MAP_NS+ENTRY_TYPE;
	public static final String KEY_PROPERTY_URI = MAP_NS+"key";
	public static final String LITERAL_VALUE_PROPERTY_URI = MAP_NS+LITERAL_VALUE;
	public static final String OBJECT_VALUE_PROPERTY_URI = MAP_NS+OBJECT_VALUE;
	public static final String CONTAINEROWNER_PROPERTY_URI = MAP_NS+"containerOwnerRef";
	public static final String MAP_REFERENCE_SUPERPROPERTY_URI = MAP_NS+"mapRef";
		
	@Getter
	private final OntDataProperty keyProperty;
	@Getter
	private final OntDataProperty literalValueProperty;
	@Getter
	private final OntObjectProperty.Named objectValueProperty;
	@Getter
	private final OntObjectProperty containerProperty;
	@Getter
	private final OntObjectProperty mapReferenceSuperProperty;
	
	@Getter
	private final OntClass mapEntryClass;
	private final Set<OntClass> subclassesCache = new HashSet<>();
	private SingleResourceType singleType;
	
	public MapResourceType(OntModel model, SingleResourceType singleType) {		
		this.singleType = singleType;
		mapEntryClass = model.getOntClass(ENTRY_TYPE_URI);
		keyProperty = model.getDataProperty(KEY_PROPERTY_URI);
		literalValueProperty = model.getDataProperty(LITERAL_VALUE_PROPERTY_URI);
		objectValueProperty = model.getObjectProperty(OBJECT_VALUE_PROPERTY_URI);
		containerProperty = model.getObjectProperty(CONTAINEROWNER_PROPERTY_URI);
		mapReferenceSuperProperty = model.getObjectProperty(MAP_REFERENCE_SUPERPROPERTY_URI);			
		initHierarchyCache();
	}		
			
	private void initHierarchyCache() {
		mapEntryClass.subClasses().forEach(subclassesCache::add);
	}
	
	public static boolean isEntryProperty(OntProperty property) {
		// better done via super/subproperty check
		return property.getLocalName().endsWith(MapResourceType.LITERAL_VALUE) || property.getLocalName().endsWith(MapResourceType.OBJECT_VALUE);
	}
	
	public boolean isMapEntrySubclass(OntObjectProperty.Named mapEntryProperty) {
		return  mapEntryProperty.ranges(true).anyMatch(rangeClass -> rangeClass.equals(mapEntryClass)
				|| rangeClass.hasSuperClass(mapEntryClass, true));
	}
	
	public boolean isMapContainerReferenceProperty(OntProperty prop) {
		return mapReferenceSuperProperty.subProperties(true).anyMatch(subProp -> subProp.equals(prop));
	}
	
	public OntObjectProperty addLiteralMapProperty(OntClass resource, String propertyURI, OntDataRange valueType) {
		OntModel model = resource.getModel();
		if (singleType.existsPrimaryProperty(propertyURI)) {
			return null;  //as we cannot guarantee that the property that was identified is an OntObjectProperty		
		}
		OntClass mapType = model.createOntClass(generateMapEntryTypeURI(propertyURI));
		mapType.addSuperClass(mapEntryClass);
		subclassesCache.add(mapType);

		OntDataProperty valueProp = model.createDataProperty(propertyURI+LITERAL_VALUE);
		valueProp.addSuperProperty(literalValueProperty);
		valueProp.addDomain(mapType);
		valueProp.addRange(valueType);

		OntObjectProperty hasMap = model.createObjectProperty(propertyURI);
		hasMap.addDomain(resource);
		hasMap.addRange(mapType);
		mapReferenceSuperProperty.addSubProperty(hasMap);			
		return hasMap;
	}

	public OntObjectProperty addObjectMapProperty(OntClass resource, String propertyURI, OntClass valueType) {
		OntModel model = resource.getModel();
		if (singleType.existsPrimaryProperty(propertyURI)) {
			return null;  //as we cannot guarantee that the property that was identified is an OntObjectProperty		
		}
		OntClass mapType = model.createOntClass(generateMapEntryTypeURI(propertyURI));
		mapType.addSuperClass(mapEntryClass);
		subclassesCache.add(mapType);

		OntObjectProperty valueProp = model.createObjectProperty(propertyURI+OBJECT_VALUE);
		valueProp.addSuperProperty(objectValueProperty);
		valueProp.addDomain(mapType);
		valueProp.addRange(valueType);

		OntObjectProperty hasMap = model.createObjectProperty(propertyURI);
		hasMap.addDomain(resource);
		hasMap.addRange(mapType);
		mapReferenceSuperProperty.addSubProperty(hasMap);	
		return hasMap;
	}

	private String generateMapEntryTypeURI(String propertyURI) {
		return propertyURI+ENTRY_TYPE;
	}
	

	/**
	 * @param prop OntProperty to remove from its owning class including the specific map entry type and its value predicate
	 */
	public void removeMapContainerReferenceProperty(OntProperty mapReferenceProperty) {
		var model = mapReferenceProperty.getModel();
		// remove listType:
		var mapType = model.createOntClass(generateMapEntryTypeURI(mapReferenceProperty.getURI()));
		// remove from cache
		subclassesCache.remove(mapType);
		// remove any predicates from any properties that happen to be defined
		MetaModelSchemaTypes.getExplicitlyDeclaredProperties(mapType).forEach(Resource::removeProperties);
		// remove predicates association from mapType itself 
		mapType.removeProperties();
		// remove map reference property
		singleType.removeBaseProperty(mapReferenceProperty);
	}
	
	public boolean isMapEntry(OntIndividual ontInd) {
		return ontInd.classes(true).anyMatch(type -> subclassesCache.contains(type) || type.equals(mapEntryClass));
	}
	
	public boolean wasMapEntry(List<Resource> delTypes) {
		return delTypes.stream().anyMatch(type -> type.getURI().equals(getMapEntryClass().getURI()) || 
				subclassesCache.stream().map(RDFNode::asResource).anyMatch(clazz -> clazz.equals(type))  );
	}

	public List<Property> findMapReferencePropertiesBetween(Resource subject, OntObject mapEntry) {
		List<Property> props = new ArrayList<>();
		var iter = subject.getModel().listStatements(subject, null, mapEntry);
		while (iter.hasNext()) {
			props.add(iter.next().getPredicate());
		}		
		if (props.size() > 1) {
			props.remove(mapReferenceSuperProperty.asProperty());
		}
		return props;
	}
	
	protected static class MapSchemaFactory {
		
		
		private final OntModel model;
		
		public MapSchemaFactory(OntModel metaOntology) {
			this.model = metaOntology;		
			initTypes();			

		}				
		
		private void initTypes() {
			var mapEntryClass = model.getOntClass(ENTRY_TYPE_URI);
			if (mapEntryClass == null) {
				mapEntryClass = model.createOntClass(ENTRY_TYPE_URI);
			}
			
			var keyProp = model.getDataProperty(KEY_PROPERTY_URI);
			if (keyProp == null) {
				keyProp = model.createDataProperty(KEY_PROPERTY_URI);
				keyProp.addDomain(mapEntryClass);
				keyProp.addRange(model.getDatatype(XSD.xstring));
			}
			
			var literalValueProp = model.getDataProperty(LITERAL_VALUE_PROPERTY_URI);
			if (literalValueProp == null) {
				literalValueProp = model.createDataProperty(LITERAL_VALUE_PROPERTY_URI);			
				literalValueProp.addDomain(mapEntryClass);
			}
			
			var objectValueProp = model.getObjectProperty(OBJECT_VALUE_PROPERTY_URI);
			if (objectValueProp == null) {
				objectValueProp = model.createObjectProperty(OBJECT_VALUE_PROPERTY_URI);
				objectValueProp.addDomain(mapEntryClass);
			}
			
			var containerProperty = model.getObjectProperty(CONTAINEROWNER_PROPERTY_URI);
			if (containerProperty == null) {
				containerProperty = model.createObjectProperty(CONTAINEROWNER_PROPERTY_URI);
				containerProperty.addDomain(mapEntryClass);
			}
			
			var mapReferenceSuperProperty = model.getObjectProperty(MAP_REFERENCE_SUPERPROPERTY_URI);
			if (mapReferenceSuperProperty == null) {
				mapReferenceSuperProperty = model.createObjectProperty(MAP_REFERENCE_SUPERPROPERTY_URI);
				mapReferenceSuperProperty.addRange(mapEntryClass);
			}
		}
	}
}

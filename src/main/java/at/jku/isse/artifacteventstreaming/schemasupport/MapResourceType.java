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
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Model;
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
	public static final String CONTAINER_PROPERTY_URI = MAP_NS+"containerRef";
	public static final String MAP_REFERENCE_SUPERPROPERTY_URI = MAP_NS+"mapRef";
	
	private static MapSchemaFactory factory = new MapSchemaFactory();
	
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
	
	private final OntModel localModel;
	
	public MapResourceType(OntModel model) {		
		this.localModel = model;
		factory.addSchemaToModel(model);
				
		mapEntryClass = model.getOntClass(ENTRY_TYPE_URI);
		keyProperty = model.getDataProperty(KEY_PROPERTY_URI);
		literalValueProperty = model.getDataProperty(LITERAL_VALUE_PROPERTY_URI);
		objectValueProperty = model.getObjectProperty(OBJECT_VALUE_PROPERTY_URI);
		containerProperty = model.getObjectProperty(CONTAINER_PROPERTY_URI);
		mapReferenceSuperProperty = model.getObjectProperty(MAP_REFERENCE_SUPERPROPERTY_URI);			
		initHierarchyCache();
	}		
			
	private void initHierarchyCache() {
		mapEntryClass.subClasses().forEach(subclassesCache::add);
	}
	
	public static boolean isEntryProperty(OntRelationalProperty property) {
		// better done via super/subproperty check
		return property.getLocalName().endsWith(MapResourceType.LITERAL_VALUE) || property.getLocalName().endsWith(MapResourceType.OBJECT_VALUE);
	}
	
	public boolean isMapEntrySubclass(OntObjectProperty.Named mapEntryProperty) {
		return  mapEntryProperty.ranges(true).anyMatch(rangeClass -> rangeClass.equals(mapEntryClass)
				|| rangeClass.hasSuperClass(mapEntryClass, true));
	}
	
	public OntObjectProperty addLiteralMapProperty(OntClass resource, String propertyURI, OntDataRange valueType) {
		OntModel model = resource.getModel();
		var p = model.getObjectProperty(propertyURI);
		if (p == null) {
			OntClass mapType = model.createOntClass(propertyURI+ENTRY_TYPE);
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
		else 
			return null; //as we cannot guarantee that the property that was identified is an OntObjectProperty
	}
	
	public OntObjectProperty addObjectMapProperty(OntClass resource, String propertyURI, OntClass valueType) {
		OntModel model = resource.getModel();
		var p = model.getObjectProperty(propertyURI);
		if (p == null) {
			OntClass mapType = model.createOntClass(propertyURI+ENTRY_TYPE);
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
		else 
			return null; //as we cannot guarantee that the property that was identified is an OntObjectProperty
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
	
	private static class MapSchemaFactory extends SchemaFactory {
		
		public static final String MAPONTOLOGY = "mapontology";
		private final OntModel model;
		
		public MapSchemaFactory() {
			this.model = loadOntologyFromFilesystem(MAPONTOLOGY);			
			initTypes();			
			super.writeOntologyToFilesystemn(model, MAPONTOLOGY);
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
			
			var containerProperty = model.getObjectProperty(CONTAINER_PROPERTY_URI);
			if (containerProperty == null) {
				containerProperty = model.createObjectProperty(CONTAINER_PROPERTY_URI);
			}
			
			var mapReferenceSuperProperty = model.getObjectProperty(MAP_REFERENCE_SUPERPROPERTY_URI);
			if (mapReferenceSuperProperty == null) {
				mapReferenceSuperProperty = model.createObjectProperty(MAP_REFERENCE_SUPERPROPERTY_URI);
				mapReferenceSuperProperty.addRange(mapEntryClass);
			}
		}

		public void addSchemaToModel(Model modelToAddOntologyTo) {
			modelToAddOntologyTo.add(model);		
		} 
	}
}

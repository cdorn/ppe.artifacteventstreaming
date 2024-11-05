package at.jku.isse.passiveprocessengine.rdf;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.vocabulary.XSD;

import lombok.Getter;

public class MapResourceType {

	public static final String OBJECT_VALUE = "objectValue";
	public static final String LITERAL_VALUE = "literalValue";
	private static final String ENTRY_TYPE = "EntryType";
	public static String MAP_NS = "http://at.jku.isse.map#";	
	public static final String ENTRY_TYPE_URI = MAP_NS+ENTRY_TYPE;
	public static final String KEY_PROPERTY = MAP_NS+"key";
	public static final String LITERAL_VALUE_PROPERTY_URI = MAP_NS+LITERAL_VALUE;
	public static final String OBJECT_VALUE_PROPERTY_URI = MAP_NS+OBJECT_VALUE;
	
	@Getter
	private final OntDataProperty keyProperty;
	@Getter
	private final OntDataProperty literalValueProperty;
	@Getter
	private final OntObjectProperty.Named objectValueProperty;
	@Getter
	private final OntClass mapEntryClass;
	private final OntModel m;
	
	public MapResourceType(OntModel model) {		
		this.m = model;
		mapEntryClass = createMapEntryClass();
		keyProperty = createKeyProperty();
		literalValueProperty = createLiteralValueProperty();
		objectValueProperty = createObjectValueProperty();
			
	}
	
	private OntClass createMapEntryClass() {
		OntClass mapType = m.createOntClass(ENTRY_TYPE_URI);
		return mapType;
	}
	
	private OntDataProperty createKeyProperty() {
		OntDataProperty keyProp = m.createDataProperty(KEY_PROPERTY);
		keyProp.addDomain(mapEntryClass);
		keyProp.addRange(m.getDatatype(XSD.xstring));
		return keyProp;
	}
	
	private OntDataProperty createLiteralValueProperty() {
		OntDataProperty literalValueProp = m.createDataProperty(LITERAL_VALUE_PROPERTY_URI);
		literalValueProp.addDomain(mapEntryClass);
		return literalValueProp;
	}
	
	private OntObjectProperty.Named createObjectValueProperty() {		
		OntObjectProperty.Named objectValueProp = m.createObjectProperty(OBJECT_VALUE_PROPERTY_URI);
		objectValueProp.addDomain(mapEntryClass);
		return objectValueProp;
	}
	
	public static boolean isEntryProperty(OntRelationalProperty property) {
		// better done via super/subproperty check
		return property.getLocalName().endsWith(MapResourceType.LITERAL_VALUE) || property.getLocalName().endsWith(MapResourceType.OBJECT_VALUE);
	}
	
	public boolean isMapEntrySubclass(OntObjectProperty.Named mapEntryProperty) {
		return mapEntryProperty.ranges(true).anyMatch(rangeClass -> rangeClass.equals(mapEntryClass)
				|| rangeClass.hasSuperClass(mapEntryClass, true));
	}
	
	public OntObjectProperty addLiteralMapProperty(OntClass resource, String propertyURI, OntDataRange valueType) {
		OntModel model = resource.getModel();
		var p = model.getDataProperty(propertyURI);
		if (p == null) {
			OntClass mapType = model.createOntClass(propertyURI+ENTRY_TYPE);
			mapType.addSuperClass(mapEntryClass);

			OntDataProperty valueProp = model.createDataProperty(propertyURI+LITERAL_VALUE);
			valueProp.addSuperProperty(literalValueProperty);
			valueProp.addDomain(mapType);
			valueProp.addRange(valueType);

			OntObjectProperty hasMap = model.createObjectProperty(propertyURI);
			hasMap.addDomain(resource);
			hasMap.addRange(mapType);
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

			OntObjectProperty valueProp = model.createObjectProperty(propertyURI+OBJECT_VALUE);
			valueProp.addSuperProperty(objectValueProperty);
			valueProp.addDomain(mapType);
			valueProp.addRange(valueType);

			OntObjectProperty hasMap = model.createObjectProperty(propertyURI);
			hasMap.addDomain(resource);
			hasMap.addRange(mapType);
			return hasMap;
		}
		else 
			return null; //as we cannot guarantee that the property that was identified is an OntObjectProperty
	}
}

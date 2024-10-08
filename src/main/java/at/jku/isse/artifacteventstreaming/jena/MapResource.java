package at.jku.isse.artifacteventstreaming.jena;


import java.util.HashMap;
import java.util.Map;

import org.apache.jena.graph.FrontsNode;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntObjectProperty.Named;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.XSD;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

public class MapResource {

	
	
	public static String MAP_NS = "http://at.jku.isse.map#";	
	public static final String ENTRY_TYPE = MAP_NS+"EntryType";
	public static final String KEY_PROPERTY = MAP_NS+"key";
	public static final String LITERAL_VALUE_PROPERTY = MAP_NS+"literalValue";
	public static final String OBJECT_VALUE_PROPERTY = MAP_NS+"objectValue";
	
	private final OntIndividual mapOwner;
	private final OntObjectProperty.Named mapEntryProperty;	
	private final Map<String, Statement> map = new HashMap<>();
	
	private final OntModel model;
	private final OntDataProperty keyProp;
	private final OntDataProperty litValueProp;
	private final OntObjectProperty.Named objectValueProp;
	
	private MapResource(OntIndividual mapOwner, Named mapEntryProperty) {
		super();
		this.mapOwner = mapOwner;
		this.mapEntryProperty = mapEntryProperty;
		model = mapOwner.getModel();		
		keyProp = getKeyProperty(model);
		litValueProp = getLiteralValueProperty(model);
		objectValueProp = getObjectValueProperty(model);
		init();
	}

	public static MapResource asResource(OntIndividual mapOwner, OntObjectProperty.Named mapEntryProperty) throws ResourceMismatchException {
		OntClass mapType = getMapEntryClass(mapOwner.getModel());
		if (mapEntryProperty.ranges(true).anyMatch(rangeClass -> rangeClass.equals(mapType) || rangeClass.hasSuperClass(mapType, false))) {
			MapResource map = new MapResource(mapOwner, mapEntryProperty);
			return map;
		}
		else
			throw new ResourceMismatchException("Provided property does not have a map entry resource in range.");		
	}

	public static OntClass getMapEntryClass(OntModel m) {
		OntClass mapType = m.createOntClass(ENTRY_TYPE);
		return mapType;
	}
	
	public static OntDataProperty getKeyProperty(OntModel m) {
		OntDataProperty keyProp = m.createDataProperty(KEY_PROPERTY);
		keyProp.addDomain(getMapEntryClass(m));
		keyProp.addRange(m.getDatatype(XSD.xstring));
		return keyProp;
	}
	
	public static OntDataProperty getLiteralValueProperty(OntModel m) {
		OntDataProperty literalValueProp = m.createDataProperty(LITERAL_VALUE_PROPERTY);
		literalValueProp.addDomain(getMapEntryClass(m));
		return literalValueProp;
	}
	
	public static OntObjectProperty.Named getObjectValueProperty(OntModel m) {		
		OntObjectProperty.Named objectValueProp = m.createObjectProperty(OBJECT_VALUE_PROPERTY);
		objectValueProp.addDomain(getMapEntryClass(m));
		return objectValueProp;
	}
	
	private void init() {
		
		var iter = mapOwner.listProperties(mapEntryProperty);
		
		
		while(iter.hasNext()) {
			var stmt = iter.next();
			var entry = stmt.getObject().asResource();
			// access key
			var keyStmt = entry.getProperty(keyProp);
			if (keyStmt != null) {
				String key = keyStmt.getString();
				// access value (this is an untyped map, so literals and resource can be mixed!)
				var litStmt = entry.getProperty(litValueProp);
				if (litStmt != null) {
					map.put(key, litStmt);
				} else {
					var refStmt = entry.getProperty(objectValueProp);
					if (refStmt != null) {
						map.put(key, refStmt);
					}
				}
			}									
		}
	}
	
	public Object get(String key) {
		Statement stmt = map.get(key);
		if (stmt != null) {
			if (stmt.getPredicate().equals(litValueProp)) {
				Object value = stmt.getLiteral().getValue();
				return value;
			} else if (stmt.getPredicate().equals(objectValueProp)) {
				var node = stmt.getObject(); // we return the rdf node
				return node;
			}
			return null; // should not happen, as we checked before when inserting
		} else
			return null;
	}
	
	public void put(String key, RDFNode node) {
		Statement prevStmt = map.remove(key);
		if (prevStmt != null) {									
			var newStmt = prevStmt.changeObject(node);
			map.put(key,  newStmt);
		} else {
			OntIndividual entry = getMapEntryClass(model).createIndividual();
			entry.addLiteral(keyProp, key);
			Statement newStmt = null;
			if (node.isLiteral()) {
				entry.addLiteral(litValueProp, node);
				newStmt = entry.listProperties(litValueProp).next();
			} else {
				entry.addProperty(objectValueProp, node);
				newStmt = entry.listProperties(objectValueProp).next();
			}			
			map.put(key,  newStmt);
		}
	}
	
	

}

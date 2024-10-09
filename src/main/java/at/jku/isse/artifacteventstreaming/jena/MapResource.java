package at.jku.isse.artifacteventstreaming.jena;


import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.graph.FrontsNode;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntObjectProperty.Named;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.XSD;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

public class MapResource implements Map<String, RDFNode> {	
	
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

	public static MapResource asMapResource(OntIndividual mapOwner, OntObjectProperty.Named mapEntryProperty) throws ResourceMismatchException {
		OntClass mapType = getMapEntryClass(mapOwner.getModel());
		if (mapEntryProperty.ranges(true).anyMatch(rangeClass -> rangeClass.equals(mapType) || rangeClass.hasSuperClass(mapType, false))) {
			MapResource map = new MapResource(mapOwner, mapEntryProperty);
			return map;
		}
		else
			throw new ResourceMismatchException(String.format("Provided property %s is not in range of %s", mapEntryProperty.getURI(), ENTRY_TYPE));		
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
				// now access value (this is an untyped map, so literals and resource can be mixed!)
				var litStmt = entry.getProperty(litValueProp);
				if (litStmt != null) {
					map.put(key, litStmt);
				} else {
					var refStmt = entry.getProperty(objectValueProp);
					if (refStmt != null) {
						map.put(key, refStmt);
					}
				}
			} else {
				// not an entry object
			}
		}
	}
	
	private RDFNode getValueFromStatement(Statement stmt) {
		if (stmt.getPredicate().equals(litValueProp)) {
			var value = stmt.getLiteral();
			return value;
		} else if (stmt.getPredicate().equals(objectValueProp)) {
			var node = stmt.getObject(); // we return the rdf node
			return node;
		}
		return null; // should not happen, as we checked before when inserting
	}
	
	public RDFNode get(Object key) {
		Statement stmt = map.get(key);
		if (stmt != null) {
			return getValueFromStatement(stmt);
		} else
			return null;
	}
	
	public RDFNode remove(Object key) {
		RDFNode oldValue = get(key);
		Statement prevStmt = map.remove(key);
		if (prevStmt != null) {					
			// remove properties of entry
			prevStmt.getSubject().removeProperties();
			// remove the whole anonymous entry, not to have it dangling in the model
			mapOwner.remove(mapEntryProperty, prevStmt.getSubject());			
			return oldValue;
		} else
			return null;
	}
	
	public RDFNode put(String key, RDFNode node) {
		Statement prevStmt = map.remove(key);
		if (prevStmt != null) {												
			var newStmt = prevStmt.changeObject(node);
			map.put(key,  newStmt);
			return getValueFromStatement(prevStmt);
		} else {
			OntIndividual entry = getMapEntryClass(model).createIndividual();
			entry.addLiteral(keyProp, key);
			Statement newStmt = null;
			if (node.isLiteral()) {
				entry.addProperty(litValueProp, node);
				newStmt = entry.listProperties(litValueProp).next();
			} else {
				entry.addProperty(objectValueProp, node);
				newStmt = entry.listProperties(objectValueProp).next();
			}			
			map.put(key,  newStmt);
			mapOwner.addProperty(mapEntryProperty, entry);
			return null;
		}
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public boolean isEmpty() {
		return map.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return map.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.values().stream()
				.map(stmt -> getValueFromStatement(stmt))
				.anyMatch(entryValue -> entryValue.equals(value));
	}

	@Override
	public void putAll(Map<? extends String, ? extends RDFNode> m) {
		m.entrySet().forEach(entry -> put(entry.getKey(), entry.getValue()));
	}

	@Override
	public void clear() {
		map.values().stream().forEach(stmt -> stmt.getSubject().removeProperties());
		mapOwner.removeAll(mapEntryProperty);		
		map.clear();
	}

	@Override
	public Set<String> keySet() {
		return map.keySet();
	}

	@Override
	public Collection<RDFNode> values() {
		return map.values().stream()
			.map(stmt -> getValueFromStatement(stmt))
			.collect(Collectors.toList());
	}

	@Override
	public Set<Entry<String, RDFNode>> entrySet() {
		return map.entrySet().stream()
			.map(entry -> new AbstractMap.SimpleEntry<String, RDFNode>(entry.getKey(), getValueFromStatement(entry.getValue())))
			.collect(Collectors.toSet());		
	}
	
	

}

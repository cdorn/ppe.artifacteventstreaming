package at.jku.isse.passiveprocessengine.rdfwrapper;


import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntObjectProperty.Named;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;

public class MapResource implements Map<String, RDFNode> {	

	private final OntObject mapOwner;
	private final OntObjectProperty.Named mapEntryProperty;	
	private final Map<String, Statement> map = new HashMap<>();
	
	private final MapResourceType mapType;
	
	private MapResource(OntObject mapOwner, Named mapEntryProperty, MapResourceType mapType) {		
		this.mapType = mapType;
		this.mapOwner = mapOwner;
		this.mapEntryProperty = mapEntryProperty;		
		loadMap(mapOwner.listProperties(mapEntryProperty), mapType).stream().forEach(entry -> map.put(entry.getKey(), entry.getValue()));
	}

	public static MapResource asMapResource(OntObject mapOwner, OntObjectProperty.Named mapEntryProperty, MapResourceType mapType) throws ResourceMismatchException {
		
		if (mapType.isMapEntrySubclass(mapEntryProperty)) {
			return new MapResource(mapOwner, mapEntryProperty, mapType);
		}
		else
			throw new ResourceMismatchException(String.format("Provided property %s is not in range of a %s", mapEntryProperty.getURI(), MapResourceType.ENTRY_TYPE_URI));		
	}

	
	
	public static Collection<Entry<String, Statement>> loadMap(StmtIterator iter, MapResourceType mapType) {							
		List<Entry<String, Statement>> entries = new ArrayList<>();
		while(iter.hasNext()) {
			var stmt = iter.next();
			var entry = stmt.getObject().asResource();
			// access key
			var keyStmt = entry.getProperty(mapType.getKeyProperty());
			if (keyStmt != null) {
				String key = keyStmt.getString();
				// now access value (this is an untyped map, so literals and resource can be mixed!)
				var litStmt = entry.getProperty(mapType.getLiteralValueProperty());
				if (litStmt != null) {
					entries.add(new AbstractMap.SimpleEntry<>(key, litStmt));
				} else {
					var refStmt = entry.getProperty(mapType.getObjectValueProperty());
					if (refStmt != null) {
						entries.add(new AbstractMap.SimpleEntry<>(key, refStmt));
					}
				}
			} else {
				// not an entry object
			}
		}
		return entries;
	}
	
	private RDFNode getValueFromStatement(Statement stmt) {
		if (stmt.getPredicate().equals(mapType.getLiteralValueProperty())) {
			var value = stmt.getLiteral();
			return value;
		} else if (stmt.getPredicate().equals(mapType.getObjectValueProperty())) {
			return stmt.getObject();
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
		// check if we are actually supposed to put that node into the hashtable

		Statement prevStmt = map.remove(key);
		if (prevStmt != null) {												
			var newStmt = prevStmt.changeObject(node);
			map.put(key,  newStmt);
			return getValueFromStatement(prevStmt);
		} else {
			OntIndividual entry = mapType.getMapEntryClass().createIndividual(); //FIXME: we should create a childclass instance, not from the base class, but could work
			addBackReference(entry);
			entry.addLiteral(mapType.getKeyProperty(), key);
			Statement newStmt = null;
			if (node.isLiteral()) {
				entry.addProperty(mapType.getLiteralValueProperty(), node);
				newStmt = entry.listProperties(mapType.getLiteralValueProperty()).next();
			} else {
				entry.addProperty(mapType.getObjectValueProperty(), node);
				newStmt = entry.listProperties(mapType.getObjectValueProperty()).next();
			}			
			map.put(key,  newStmt);
			mapOwner.addProperty(mapEntryProperty, entry);
			return null;
		}
	}

	private void addBackReference(OntIndividual entry) {
		entry.addProperty(mapType.getContainerProperty().asProperty(), this.mapOwner);
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
				.map(this::getValueFromStatement)
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
			.map(this::getValueFromStatement)
			.toList();
	}

	@Override
	public Set<Entry<String, RDFNode>> entrySet() {
		return map.entrySet().stream()
			.map(entry -> new AbstractMap.SimpleEntry<String, RDFNode>(entry.getKey(), getValueFromStatement(entry.getValue())))
			.collect(Collectors.toSet());		
	}
	
	

}

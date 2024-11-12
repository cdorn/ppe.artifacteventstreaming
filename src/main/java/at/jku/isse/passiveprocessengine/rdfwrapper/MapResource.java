package at.jku.isse.passiveprocessengine.rdfwrapper;


import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntObjectProperty.Named;
import org.apache.jena.ontapi.model.OntClass.CardinalityRestriction;
import org.apache.jena.ontapi.model.OntClass.ValueRestriction;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;

public class MapResource implements Map<String, RDFNode> {	

	private final OntObject mapOwner;
	private final OntObjectProperty.Named mapEntryProperty;	
	private final Map<String, Statement> map = new HashMap<>();
	private final OntClass objectType;
	private final RDFDatatype literalType;
	private final MapResourceType mapType;
	
	private MapResource(OntObject mapOwner, Named mapEntryProperty, MapResourceType mapType, OntObject classOrDataRange) {
		super();
		this.mapType = mapType;
		this.mapOwner = mapOwner;
		this.mapEntryProperty = mapEntryProperty;		
		if (classOrDataRange instanceof OntClass ontClass) {
			this.objectType = ontClass;
			this.literalType = null;
		} else if (classOrDataRange instanceof OntDataRange ontRange) {
			this.literalType = ontRange.asNamed().toRDFDatatype();
			this.objectType = null;
		} else { // untyped
			this.literalType = null;
			this.objectType = null;
		}
		
		init();
	}

	public static MapResource asMapResource(OntObject mapOwner, OntObjectProperty.Named mapEntryProperty, MapResourceType mapType, OntObject classOrDataRange) throws ResourceMismatchException {
		
		if (mapType.isMapEntrySubclass(mapEntryProperty)) {
			return new MapResource(mapOwner, mapEntryProperty, mapType, classOrDataRange);
		}
		else
			throw new ResourceMismatchException(String.format("Provided property %s is not in range of a %s", mapEntryProperty.getURI(), MapResourceType.ENTRY_TYPE_URI));		
	}

	
	
	private void init() {		
		var iter = mapOwner.listProperties(mapEntryProperty);				
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
					map.put(key, litStmt);
				} else {
					var refStmt = entry.getProperty(mapType.getObjectValueProperty());
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
		if (stmt.getPredicate().equals(mapType.getLiteralValueProperty())) {
			var value = stmt.getLiteral();
			return value;
		} else if (stmt.getPredicate().equals(mapType.getObjectValueProperty())) {
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
		// check if we are actually supposed to put that node into the hashtable
		if (!isAssignable(node) ) { //&& node.asLiteral()
			var allowedType = this.literalType!=null ? this.literalType.getURI() : this.objectType.getURI();
			throw new IllegalArgumentException(String.format("Cannot add %s into a map allowing only values of type %s", node.toString(), allowedType));
		} 
		Statement prevStmt = map.remove(key);
		if (prevStmt != null) {												
			var newStmt = prevStmt.changeObject(node);
			map.put(key,  newStmt);
			return getValueFromStatement(prevStmt);
		} else {
			OntIndividual entry = mapType.getMapEntryClass().createIndividual(); //FIXME: we should create a childclass instance, not from the base class, but could work
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
	
	private boolean isAssignable(RDFNode node) {
		if (this.literalType != null &&  // with have a literal type, but value is either not a literal, or not a compatible literal
				(!node.isLiteral() || !literalType.isValidValue(node)) ) 	{
			return false;
		} 
		if (this.objectType != null) {
			if (node.isLiteral()) { // we have a complex type but provided with a literal
				return false;
			}
			if (!node.asResource().canAs(OntIndividual.class)) {// not a typed resource
				return false;
			}
			var ontInd = node.asResource().as(OntIndividual.class);
			if (getRegularSuperclasses(ontInd).stream().noneMatch(clazz -> clazz.equals(this.objectType))) {// not a valid subclass
				return false;
			}
		}
		return true;
	}
	
	private Set<OntClass> getRegularSuperclasses(OntIndividual ind) {
		return ind.classes(true).flatMap(type -> type.superClasses()
											.filter(superClass -> !(superClass instanceof CardinalityRestriction))
											.filter(superClass -> !(superClass instanceof ValueRestriction))
											  )
						.collect(Collectors.toSet());
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

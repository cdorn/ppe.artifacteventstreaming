package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntObject;

import at.jku.isse.artifacteventstreaming.schemasupport.MapResource;

public class MapWrapper extends TypedCollectionResource implements Map<String, Object>{

	private final MapResource delegate;

	public MapWrapper(OntObject classOrDataRange, NodeToDomainResolver resolver, MapResource delegate) {
		super(classOrDataRange, resolver);
		this.delegate = delegate;
	}

	public Object get(Object key) {
		return resolver.convertFromRDF(delegate.get(key));
	}

	public Object remove(Object key) {
		var node = delegate.remove(key);
		return resolver.convertFromRDF(node);
	}

	public Object put(String key, Object obj) {		
		var node = resolver.convertToRDF(obj);		
		if (!isAssignable(node) ) { //&& node.asLiteral()
			var allowedType = this.literalType!=null ? this.literalType.getURI() : this.objectType.getURI();
			throw new IllegalArgumentException(String.format("Cannot add %s into a map allowing only values of type %s", node.toString(), allowedType));
		} 					

		var old = delegate.put(key, node);
		if (old != null) {
			return resolver.convertFromRDF(old);
		} else
			return null;

	}

	public int size() {
		return delegate.size();
	}

	public boolean isEmpty() {
		return delegate.isEmpty();
	}

	public boolean containsKey(Object key) {
		return delegate.containsKey(key);
	}

	public boolean containsValue(Object value) {
		if (value instanceof RDFElement rdf) {
			return delegate.containsValue(rdf.element);
		}
		return delegate.containsValue(resolver.getModel().createTypedLiteral(value));
	}

	public void putAll(Map<? extends String, ? extends Object> m) {
		m.entrySet().stream().forEach(entry -> put(entry.getKey(), entry.getValue()));
	}

	public void clear() {
		delegate.clear();
	}

	@Override
	public void delete() {
		delegate.clear(); // removes all entries, hence nothing of the map remains
	}
	
	public Set<String> keySet() {
		return delegate.keySet();
	}

	public Collection<Object> values() {
		return delegate.values().stream().map(node -> resolver.convertFromRDF(node)).toList();
	}

	public Set<Entry<String, Object>> entrySet() {
		return delegate.entrySet().stream()
				.map(entry ->  new AbstractMap.SimpleEntry<String, Object>(entry.getKey(), resolver.convertFromRDF(entry.getValue())))
				.collect(Collectors.toSet());	
	}



}

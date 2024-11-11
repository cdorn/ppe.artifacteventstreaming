package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.rdf.model.RDFNode;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MapWrapper implements Map<String, Object>{

	private final MapResource delegate;
	private final NodeToDomainResolver resolver;

	public Object get(Object key) {
		return nodeToObject(delegate.get(key));
	}

	public Object remove(Object key) {
		var node = delegate.remove(key);
		return nodeToObject(node);
	}

	public Object put(String key, Object node) {
		if (node instanceof RDFElement rdf) {
			var old = delegate.put(key, rdf.element);
			if (old != null) {
				return resolver.resolveToRDFElement(old);
			} else
				return old;
		}
		var old = delegate.put(key, resolver.getModel().createTypedLiteral(node));
		if (old != null) {
			return old.asLiteral().getValue();
		} else
			return old;
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
		return delegate.containsValue(value);
	}

	public void putAll(Map<? extends String, ? extends Object> m) {
		m.entrySet().stream().forEach(entry -> put(entry.getKey(), entry.getValue()));
	}

	public void clear() {
		delegate.clear();
	}

	public Set<String> keySet() {
		return delegate.keySet();
	}

	public Collection<Object> values() {
		return delegate.values().stream().map(node -> nodeToObject(node)).toList();
	}

	public Set<Entry<String, Object>> entrySet() {
		return delegate.entrySet().stream()
				.map(entry ->  new AbstractMap.SimpleEntry<String, Object>(entry.getKey(), nodeToObject(entry.getValue())))
				.collect(Collectors.toSet());	
	}

	private Object nodeToObject(RDFNode node) {
		if (node == null)
			return null;
		if (node.isLiteral()) {
			return node.asLiteral().getValue();
		} else {
			return resolver.resolveToRDFElement(node);
		}
	}
	
}

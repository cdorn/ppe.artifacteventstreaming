package at.jku.isse.passiveprocessengine.rdfwrapper.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.StmtIterator;

import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public class SetWrapper extends TypedCollectionResource implements Set<Object> {

	private final OntObject setOwner;
	private final Property setProperty;
	
	public SetWrapper(@NonNull  OntObject setOwner, @NonNull  OntRelationalProperty setProperty, @NonNull NodeToDomainResolver resolver, @NonNull  OntObject classOrDataRange) {
		super(classOrDataRange, resolver);		
		this.setOwner = setOwner;
		this.setProperty = setProperty.asProperty();
	}
	
	
	@Override
	public boolean remove(Object o) {				
		var node = resolver.convertToRDF(o); 		
		var oldValue = setOwner.hasProperty(setProperty, node);
		setOwner.remove(setProperty, node);
		return oldValue;	
	}

	@Override
	public void clear() {
		setOwner.removeAll(setProperty);
	}
	
	@Override
	public void delete() {
		this.clear();
	}
	
	@Override
	public void forEach(Consumer<? super Object> action) {
		throw new RuntimeException("Not supported");
	}

	public int size() {
		var iter = setOwner.listProperties(setProperty);
		int count = 0;
		while(iter.hasNext()) {
			iter.next();
			count++;
		}
		return count;
	}

	public boolean isEmpty() {
		var iter = setOwner.listProperties(setProperty);
		return !iter.hasNext();
	}

	public boolean contains(Object o) {
		var node = resolver.convertToRDF(o);
		return setOwner.hasProperty(setProperty, node);
	}

	public Iterator<Object> iterator() {
		return new IteratorWrapper(setOwner.listProperties(setProperty), resolver);
	}

	public Object[] toArray() {
		return this.stream().toArray();
	}

	public <T> T[] toArray(T[] a) {
		return (T[]) this.stream().toArray();
	}

	public boolean add(Object e) {
		var node = resolver.convertToRDF(e);
		if (!isAssignable(node) ) { 
			var allowedType = this.literalType!=null ? this.literalType.getURI() : this.objectType.getURI();
			throw new IllegalArgumentException(String.format("Cannot add %s into a set allowing only values of type %s", node.toString(), allowedType));
		}				
		var oldValue = setOwner.hasProperty(setProperty, node);
		if (oldValue) {
			return false;
		} else {
			setOwner.addProperty(setProperty, node);
			return true;	
		}
	}

	public boolean containsAll(Collection<?> c) {
		return c.stream().allMatch(this::contains);
	}

	public boolean addAll(Collection<? extends Object> c) {
		if (c.isEmpty()) 
			return false;
		else {
			return c.stream()
					.map(this::add)
					.filter(result -> result)
					.count() > 0;			
		}
	}

	public boolean retainAll(Collection<?> c) {
		var toRemove = this.stream()
				.filter(el -> !c.contains(el)).toList(); 		
		return toRemove.stream().map(this::remove)
				.filter(result -> result) // for true results
				.count() > 0; // count them, 
	}

	public boolean removeAll(Collection<?> c) {
		return c.stream().map(this::remove).filter(result -> result).count() > 0;
	}

	@Override
	public Spliterator<Object> spliterator() {
		throw new RuntimeException("Not supported");
	}

	public <T> T[] toArray(IntFunction<T[]> generator) {
		throw new RuntimeException("Not supported");
	}

	@Override
	public boolean removeIf(Predicate<? super Object> filter) {
		throw new RuntimeException("Not supported");
	}

	@Override
	public Stream<Object> stream() {
		var iter = this.iterator();
		Iterable<Object> iterable = () -> iter;
		return StreamSupport.stream(iterable.spliterator(), false);
	}


	@RequiredArgsConstructor
	public static class IteratorWrapper implements Iterator<Object>{

		private final StmtIterator delegate;
		private final NodeToDomainResolver resolver;
		
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public Object next() {			
			RDFNode nextNode = delegate.next().getObject();
			if (nextNode.isLiteral())
				return nextNode.asLiteral().getValue();
			else
				return resolver.resolveToRDFElement(nextNode);
		}

	}
	
}

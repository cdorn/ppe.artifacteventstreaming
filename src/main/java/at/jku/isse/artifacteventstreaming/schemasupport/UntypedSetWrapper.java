package at.jku.isse.artifacteventstreaming.schemasupport;

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
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.StmtIterator;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public class UntypedSetWrapper implements Set<RDFNode> {

	protected final OntObject setOwner;
	protected final Property setProperty;
	
	public UntypedSetWrapper(@NonNull  OntObject setOwner, @NonNull Property setProperty) {
		this.setOwner = setOwner;
		this.setProperty = setProperty;
	}
	
	@Override
	public boolean remove(Object o) {		
		RDFNode node = (RDFNode)o;
		var oldValue = setOwner.hasProperty(setProperty, node);
		setOwner.remove(setProperty, node);
		return oldValue;	
	}

	@Override
	public void clear() {
		setOwner.removeAll(setProperty);
	}
	
	public void delete() {
		this.clear();
	}
	
	@Override
	public void forEach(Consumer<? super RDFNode> action) {
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
		RDFNode node = (RDFNode)o;
		return setOwner.hasProperty(setProperty, node);
	}

	public Iterator<RDFNode> iterator() {
		return new IteratorWrapper(setOwner.listProperties(setProperty));
	}

	public Object[] toArray() {
		return this.stream().toArray();
	}

	public <T> T[] toArray(T[] a) {
		return (T[]) this.stream().toArray();
	}

	public boolean add(RDFNode node) {		
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

	public boolean addAll(Collection<? extends RDFNode> c) {
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
	public Spliterator<RDFNode> spliterator() {
		throw new RuntimeException("Not supported");
	}

	public <T> T[] toArray(IntFunction<T[]> generator) {
		throw new RuntimeException("Not supported");
	}

	@Override
	public boolean removeIf(Predicate<? super RDFNode> filter) {
		throw new RuntimeException("Not supported");
	}

	@Override
	public Stream<RDFNode> stream() {
		var iter = this.iterator();
		Iterable<RDFNode> iterable = () -> iter;
		return StreamSupport.stream(iterable.spliterator(), false);
	}


	@RequiredArgsConstructor
	public static class IteratorWrapper implements Iterator<RDFNode>{

		private final StmtIterator delegate;
		
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public RDFNode next() {			
			RDFNode nextNode = delegate.next().getObject();
			if (!delegate.hasNext()) {
				delegate.close();
			}
			return nextNode;
		}

	}
}

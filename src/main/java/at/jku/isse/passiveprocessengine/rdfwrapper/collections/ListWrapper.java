package at.jku.isse.passiveprocessengine.rdfwrapper.collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty.Named;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Seq;

import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFElement;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public class ListWrapper extends TypedCollectionResource implements List<Object> {

	private final Seq listContent;
	
	public ListWrapper(@NonNull OntObject owner, @NonNull Named listReferenceProperty, @NonNull NodeToDomainResolver resolver, OntObject classOrDataRange) {
		super(classOrDataRange, resolver);
		this.listContent = resolver.getMetaschemata().getListType().getOrCreateSequenceFor(owner, listReferenceProperty);
	}
	
	@Override
	public int size() {
		return listContent.size();
	}
	@Override
	public boolean isEmpty() {
		return listContent.size() == 0;
	}
	@Override
	public boolean contains(Object o) {
		if (o instanceof RDFElement rdfEl) {
			return listContent.contains(rdfEl.getElement());
		} else { // a literal
			return listContent.contains(listContent.getModel().createTypedLiteral(o));
		}
	}
	
	@Override
	public Iterator<Object> iterator() {
		return new IteratorWrapper(listContent.iterator(), resolver);
	}
	@Override
	public Object[] toArray() {
		throw new RuntimeException("Not supported");
	}
	@Override
	public <T> T[] toArray(T[] a) {
		throw new RuntimeException("Not supported");
	}
	@Override
	public boolean add(Object e) {
		var node = resolver.convertToRDF(e);
		checkOrThrow(node);
		listContent.add(node);							
		return true;
	}
	
	@Override
	public boolean remove(Object o) {
		int pos = this.indexOf(o);
		if (pos >= 0) {
			listContent.remove(pos+1);
			return true;
		} else {
			return false;
		}
	}
	@Override
	public boolean containsAll(Collection<?> c) {
		return c.stream().allMatch(this::contains);
	}
	@Override
	public boolean addAll(Collection<? extends Object> c) {
		c.stream().forEach(this::add);
		return true;
	}
	
	@Override
	public boolean addAll(int index, Collection<? extends Object> c) {
		if (c.isEmpty()) 
			return false;
		else {
			List<Object> list = new ArrayList<>(c);
			Collections.reverse(list);
			list.stream().forEach(element -> add(index, element));
			return true;			
		}
	}
	@Override
	public boolean removeAll(Collection<?> c) {
		return c.stream().map(this::remove).filter(result -> result).count() > 0;
	}
	@Override
	public boolean retainAll(Collection<?> c) {
		return this.subList(0, this.size()).stream()
			.filter(el -> !c.contains(el))
			.map(this::remove)
			.filter(result -> result) // for true results
			.count() > 0; // count them, 
	}
	
	@Override
	public void clear() {		
		var size = listContent.size();
		for (int i = size; i > 0 ; i--) { // if we remove from the start, all the remaining elements are moved down one step, --> very inefficient
			listContent.remove(i); 
		}
	}
	
	@Override
	public void delete() {
		listContent.removeProperties();
	}
	
	@Override
	public Object get(int index) {
		var content = listContent.getObject(index+1); //RDF lists are 1-based
		return resolver.convertFromRDF(content);
	}
	
	@Override
	public Object set(int index, Object e) {
		var priorObj = get(index);
		var node = resolver.convertToRDF(e);
		checkOrThrow(node);
		listContent.set(index+1, node);	
		return priorObj;
	}
	
	@Override
	public void add(int index, Object e) {		
		var node = resolver.convertToRDF(e);
		checkOrThrow(node);
		listContent.add(index+1, node);	
	}
	
	private void checkOrThrow(RDFNode node) {
		if (!isAssignable(node) ) { //&& node.asLiteral()
			var allowedType = this.literalType!=null ? this.literalType.getURI() : this.objectType.getURI();
			throw new IllegalArgumentException(String.format("Cannot add %s into a list allowing only values of type %s", node.toString(), allowedType));
		}			
	}
	
	@Override
	public Object remove(int index) {
		var priorObj = get(index);
		listContent.remove(index+1);
		return priorObj;
	}
	@Override
	public int indexOf(Object e) {
		if (e instanceof RDFElement rdfEl) {
			return listContent.indexOf(rdfEl.getElement())-1;
		} else { // a literal
			return listContent.indexOf(listContent.getModel().createTypedLiteral(e))-1;
		}
	}
	@Override
	public int lastIndexOf(Object o) {
		throw new RuntimeException("Not supported");
	}
	@Override
	public ListIterator<Object> listIterator() {
		throw new RuntimeException("Not supported");
	}
	@Override
	public ListIterator<Object> listIterator(int index) {
		throw new RuntimeException("Not supported");
	}
	@Override
	public List<Object> subList(int fromIndex, int toIndex) {
		List<Object> sublist = new LinkedList<>();
		for (int i = fromIndex ; i < toIndex;  i++ ) {
			sublist.add(get(i)); // no need to incl index as get(index) will do that to 1-based index
		}
		return sublist;
	}
	
	@Override
	public Stream<Object> stream() {
		var iter = this.iterator();
		Iterable<Object> iterable = () -> iter;
		return StreamSupport.stream(iterable.spliterator(), false);
	}
	
	@RequiredArgsConstructor
	public static class IteratorWrapper implements Iterator<Object>{

		private final NodeIterator delegate;
		private final NodeToDomainResolver resolver;
		
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public Object next() {
			RDFNode nextNode = delegate.next();
			if (nextNode.isLiteral())
				return nextNode.asLiteral().getValue();
			else
				return resolver.resolveToRDFElement(nextNode);
		}

	}


	
}

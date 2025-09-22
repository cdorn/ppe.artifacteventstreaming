package at.jku.isse.artifacteventstreaming.schemasupport;

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
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Seq;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public class UntypedListWrapper implements List<RDFNode> {

	protected final Seq listContent;
	
	public UntypedListWrapper(@NonNull OntObject owner, @NonNull OntRelationalProperty listReferenceProperty, @NonNull ListResourceType listType) {
		this.listContent = listType.getOrCreateSequenceFor(owner, listReferenceProperty);
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
		RDFNode node = (RDFNode)o;
		return listContent.contains(node);
	}
	
	@Override
	public Iterator<RDFNode> iterator() {
		return new IteratorWrapper(listContent.iterator());
	}
	@Override
	public Object[] toArray() {
		return this.stream().toArray();
	}
	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] a) {
		return (T[]) this.stream().toArray();
	}
	@Override
	public boolean add(RDFNode node) {
		listContent.add(node);							
		return true;
	}
	
	@Override
	public boolean remove(Object o) {
		RDFNode node = (RDFNode)o;
		int pos = this.indexOf(node);
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
	public boolean addAll(Collection<? extends RDFNode> c) {
		c.stream().forEach(this::add);
		return true;
	}
	
	@Override
	public boolean addAll(int index, Collection<? extends RDFNode> c) {
		if (c.isEmpty()) 
			return false;
		else {
			List<RDFNode> list = new ArrayList<>(c);
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
	
	public void delete() {
		listContent.removeProperties();
	}
	
	@Override
	public RDFNode get(int index) {
		return listContent.getObject(index+1); //RDF lists are 1-based
	}
	
	@Override
	public RDFNode set(int index, RDFNode node) {
		var priorObj = get(index);
		listContent.set(index+1, node);	
		return priorObj;
	}
	
	@Override
	public void add(int index, RDFNode node) {		
		listContent.add(index+1, node);	
	}

	
	@Override
	public RDFNode remove(int index) {
		var priorObj = get(index);
		listContent.remove(index+1);
		return priorObj;
	}
	@Override
	public int indexOf(Object o) {
		RDFNode node = (RDFNode)o;
		return listContent.indexOf(node)-1;
	}
	@Override
	public int lastIndexOf(Object o) {
		throw new RuntimeException("Not supported");
	}
	@Override
	public ListIterator<RDFNode> listIterator() {
		throw new RuntimeException("Not supported");
	}
	@Override
	public ListIterator<RDFNode> listIterator(int index) {
		throw new RuntimeException("Not supported");
	}
	@Override
	public List<RDFNode> subList(int fromIndex, int toIndex) {
		List<RDFNode> sublist = new LinkedList<>();
		for (int i = fromIndex ; i < toIndex;  i++ ) {
			sublist.add(get(i)); // no need to incl index as get(index) will do that to 1-based index
		}
		return sublist;
	}
	
	@Override
	public Stream<RDFNode> stream() {
		var iter = this.iterator();
		Iterable<RDFNode> iterable = () -> iter;
		return StreamSupport.stream(iterable.spliterator(), false);
	}
	
	@RequiredArgsConstructor
	public static class IteratorWrapper implements Iterator<RDFNode>{

		private final NodeIterator delegate;
		
		@Override
		public boolean hasNext() {
			return delegate.hasNext();
		}

		@Override
		public RDFNode next() {
			RDFNode nextNode = delegate.next();
			if (!delegate.hasNext()) {
				delegate.close();
			}
			return nextNode;
		}

	}

}

package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty.Named;
import org.apache.jena.rdf.model.Seq;

import lombok.NonNull;

public class ListWrapper implements List<Object> {

	private final NodeToDomainResolver resolver;
	private final Seq listContent;
	
	public ListWrapper(@NonNull OntObject owner, @NonNull Named listReferenceProperty, @NonNull NodeToDomainResolver resolver) {
		super();
		this.resolver = resolver;
		var seq = owner.getPropertyResourceValue(listReferenceProperty);
		if( seq == null || seq.canAs(Seq.class)) {
			owner.removeAll(listReferenceProperty);
			seq = owner.getModel().createSeq(owner.getURI()+"#"+listReferenceProperty.getLocalName());
			owner.addProperty(listReferenceProperty, seq);
		} 
		this.listContent = seq.as(Seq.class);
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
		return this.stream().toArray(Object[]::new);
	}
	@Override
	public <T> T[] toArray(T[] a) {
		throw new RuntimeException("Not supported");
	}
	@Override
	public boolean add(Object e) {
		if (e instanceof RDFElement rdfEl) {
			listContent.add(rdfEl.getElement());
		} else { // a literal
			listContent.add(listContent.getModel().createTypedLiteral(e));
		}
		return true;
	}
	@Override
	public boolean remove(Object o) {
		int pos = this.indexOf(listContent);
		if (pos > 0) {
			listContent.remove(pos);
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
		listContent.removeProperties();
	}
	@Override
	public Object get(int index) {
		var content = listContent.getObject(index);
		if (content != null) {
			if (content.isLiteral()) {
				return content.asLiteral().getValue();
			} else {
				return resolver.resolveToRDFElement(content.asResource());
			}
		} else 
			return null;
	}
	
	@Override
	public Object set(int index, Object e) {
		var priorObj = get(index);
		if (e instanceof RDFElement rdfEl) {
			listContent.set(index+1, rdfEl.getElement());
		} else { // a literal
			listContent.set(index+1, listContent.getModel().createTypedLiteral(e));
		}
		return priorObj;
	}
	@Override
	public void add(int index, Object e) {
		if (e instanceof RDFElement rdfEl) {
			listContent.add(index+1, rdfEl.getElement());
		} else { // a literal
			listContent.add(index+1, listContent.getModel().createTypedLiteral(e));
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
		for (int i = fromIndex+1 ; i < toIndex+1;  i++ ) {
			sublist.add(get(i));
		}
		return sublist;
	}
	@Override
	public Stream<Object> stream() {
		var iter = this.iterator();
		Iterable<Object> iterable = () -> iter;
		return StreamSupport.stream(iterable.spliterator(), false);
	}
	
	
	
}

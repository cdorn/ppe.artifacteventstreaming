package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.Iterator;

import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.RDFNode;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class IteratorWrapper implements Iterator<Object>{

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

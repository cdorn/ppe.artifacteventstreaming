package at.jku.isse.artifacteventstreaming.replay;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Alt;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;

import at.jku.isse.artifacteventstreaming.api.ContainedStatement;

public class ContainedStatementImpl implements ContainedStatement {

	private final Statement delegate;
	private final Resource container;
	private final Property containmentProperty;
	
	public ContainedStatementImpl(Statement delegate, Resource container, Property containmentProperty) {
		super();
		this.delegate = delegate;
		this.container = container;
		this.containmentProperty = containmentProperty;
	}	
	
	public ContainedStatementImpl(Statement delegate) {
		super();
		this.delegate = delegate;
		this.container = null;
		this.containmentProperty = null;
	}

	@Override
	public Resource getContainerOrSubject() {
		if (container == null) {
			return delegate.getSubject();
		} else {
			return container;
		}
	}

	@Override
	public Property getContainmentPropertyOrPredicate() {
		if (containmentProperty == null) {
			return delegate.getPredicate();
		} else {
			return containmentProperty;
		}
	}	
	
	@Override
	public String toString() {
		if (container == null) {
			return delegate.toString();
		} else {
			return "<"+container.toString() +","+ containmentProperty.toString() +"> contain " + delegate.toString() ;
		}
	}
	
	//delegate methods:
	public Triple asTriple() {
		return delegate.asTriple();
	}

	public boolean equals(Object o) {
		return delegate.equals(o);
	}

	public int hashCode() {
		return delegate.hashCode();
	}

	public Resource getSubject() {
		return delegate.getSubject();
	}

	public Property getPredicate() {
		return delegate.getPredicate();
	}

	public RDFNode getObject() {
		return delegate.getObject();
	}

	public Statement getProperty(Property p) {
		return delegate.getProperty(p);
	}

	public Statement getStatementProperty(Property p) {
		return delegate.getStatementProperty(p);
	}

	public Resource getResource() {
		return delegate.getResource();
	}

	public Literal getLiteral() {
		return delegate.getLiteral();
	}

	public boolean getBoolean() {
		return delegate.getBoolean();
	}

	public byte getByte() {
		return delegate.getByte();
	}

	public short getShort() {
		return delegate.getShort();
	}

	public int getInt() {
		return delegate.getInt();
	}

	public long getLong() {
		return delegate.getLong();
	}

	public char getChar() {
		return delegate.getChar();
	}

	public float getFloat() {
		return delegate.getFloat();
	}

	public double getDouble() {
		return delegate.getDouble();
	}

	public String getString() {
		return delegate.getString();
	}

	public Bag getBag() {
		return delegate.getBag();
	}

	public Alt getAlt() {
		return delegate.getAlt();
	}

	public Seq getSeq() {
		return delegate.getSeq();
	}

	public RDFList getList() {
		return delegate.getList();
	}

	public String getLanguage() {
		return delegate.getLanguage();
	}

	public Statement changeLiteralObject(boolean o) {
		return delegate.changeLiteralObject(o);
	}

	public Statement changeLiteralObject(long o) {
		return delegate.changeLiteralObject(o);
	}

	public Statement changeLiteralObject(int o) {
		return delegate.changeLiteralObject(o);
	}

	public Statement changeLiteralObject(char o) {
		return delegate.changeLiteralObject(o);
	}

	public Statement changeLiteralObject(float o) {
		return delegate.changeLiteralObject(o);
	}

	public Statement changeLiteralObject(double o) {
		return delegate.changeLiteralObject(o);
	}

	public Statement changeObject(String o) {
		return delegate.changeObject(o);
	}

	public Statement changeObject(String o, String l) {
		return delegate.changeObject(o, l);
	}

	public Statement changeObject(RDFNode o) {
		return delegate.changeObject(o);
	}

	public Statement remove() {
		return delegate.remove();
	}

	public Model getModel() {
		return delegate.getModel();
	}
	
}

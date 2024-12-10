package at.jku.isse.artifacteventstreaming.api;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

public interface ContainedStatement extends Statement {
	
	/**
	 * Currently exclusively used to associate statements about lists and hashmap entries to their containing resource.
	 * @return the resource that contains the underlying statement's subject. Returns that underlying subject when no container is applicable. 
	 */
	public Resource getContainerOrSubject();
	
	/**
	 * Currently exclusively used to associate statements about lists and hashmap entries to their containing resource.
	 * @return the property that points to the contained resource that is the subject of the underlying statement, returns that statement's predicate when no containmentProperty is set. 
	 */
	public Property getContainmentPropertyOrPredicate();
	
}

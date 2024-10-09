package at.jku.isse.artifacteventstreaming.jena;

import org.apache.jena.rdf.listeners.StatementListener;
import org.apache.jena.rdf.model.Statement;


public class ChangeListener extends StatementListener {

	@Override
	public void addedStatement(Statement s) {
		System.out.println("ADDED "+s.toString());
	}

	@Override
	public void removedStatement(Statement s) {
		System.out.println("REMOVED "+s.toString());
	}

}

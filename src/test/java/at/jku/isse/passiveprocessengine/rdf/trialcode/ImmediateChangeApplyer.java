package at.jku.isse.passiveprocessengine.rdf.trialcode;

import lombok.RequiredArgsConstructor;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.listeners.StatementListener;
import org.apache.jena.rdf.model.Statement;

@RequiredArgsConstructor
public class ImmediateChangeApplyer extends StatementListener {

	private final OntModel targetModel;
	
	
	@Override
	public void addedStatement(Statement s) {
		System.out.println("ADDED "+s.toString());
		targetModel.add(s);
	}

	@Override
	public void removedStatement(Statement s) {
		System.out.println("REMOVED "+s.toString());
		targetModel.remove(s);
	}

}

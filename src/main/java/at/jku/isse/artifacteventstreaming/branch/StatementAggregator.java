package at.jku.isse.artifacteventstreaming.branch;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.graph.Graph;
import org.apache.jena.ontapi.UnionGraph;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.listeners.StatementListener;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.reasoner.InfGraph;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class StatementAggregator extends StatementListener {

	private final Set<Statement> addedStatements = new LinkedHashSet<>();
	private final Set<Statement> removedStatements = new LinkedHashSet<>();

	@Override
	public void addedStatement(Statement s) {		
		// check first if this is an undo
		if (removedStatements.isEmpty()) {
			addedStatements.add(s);
		} else {
			// if the remove set contains exactly this statement, then we wont add this statement, as the two are inverse
			if (!removedStatements.remove(s)) {
				addedStatements.add(s);
			} else {
				//noop
			}
		}			
	}

	@Override
	public void removedStatement(Statement s) {
		if (addedStatements.isEmpty()) {		
			removedStatements.add(s);
		} 
		else {
			// if the added set contains exactly this statement, then we wont add this statement, as the two are inverse
			if (!addedStatements.remove(s)) {
				removedStatements.add(s);
			} else {
				//noop
			}
		}
	}
	
	public boolean hasAdditions() {
		return !addedStatements.isEmpty();
	}

	public boolean hasRemovals() {
		return !removedStatements.isEmpty();
	}

	
	public Set<Statement> retrieveAddedStatements() {		
		var set = addedStatements.stream().collect(Collectors.toSet());
		addedStatements.clear();
		return set;
	}
	
	public Set<Statement> retrieveRemovedStatements() {
		var set = removedStatements.stream().collect(Collectors.toSet());
		removedStatements.clear();
		return set;
	}
	
	public void registerWithModel(OntModel model) {
		// when using inference, this does not register the listener at the right graph:	https://github.com/apache/jena/issues/2868
		model.register(this);
		if (model.getGraph() instanceof InfGraph infG) {
            Graph raw = infG.getRawGraph();
            if (raw instanceof UnionGraph ugraph) {
            	ugraph.getEventManager().register(((ModelCom)model).adapt(this));
            }            
        }   
	}
}

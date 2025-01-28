package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.Set;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.listeners.StatementListener;
import org.apache.jena.rdf.model.Statement;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.replay.ContainedStatementImpl;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TransactionalChangeApplyer extends StatementListener {

	private final OntModel targetModel;
	private Commit transactionScope = new StatementCommitImpl("main", "test", "none", 0);
	
	@Override
	public void addedStatement(Statement s) {
		System.out.println("ADDED TO TRANSACTION"+s.toString());
		transactionScope.appendAddedStatements(Set.of(new ContainedStatementImpl(s)));
	}

	@Override
	public void removedStatement(Statement s) {
		System.out.println("REMOVED TRANSACTION"+s.toString());
		transactionScope.appendRemovedStatement(Set.of(new ContainedStatementImpl(s)));
	}

	
	public void commitTransaction(String commitMsg) {
		Commit scope = transactionScope;
		transactionScope = new StatementCommitImpl("main", commitMsg, "none", 0);
		targetModel.add(scope.getAddedStatements().stream().map(Statement.class::cast).toList());
		targetModel.remove(scope.getRemovedStatements().stream().map(Statement.class::cast).toList());
	}
	
	public Commit abortTransaction() {
		Commit scope = transactionScope;
		transactionScope = new StatementCommitImpl("main", "test", "none", 0);
		return scope;
	}
}

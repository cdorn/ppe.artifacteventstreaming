package at.jku.isse.artifacteventstreaming.jena;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.listeners.StatementListener;
import org.apache.jena.rdf.model.Statement;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TransactionalChangeApplyer extends StatementListener {

	private final OntModel targetModel;
	private StatementCommit transactionScope = new StatementCommit();
	
	@Override
	public void addedStatement(Statement s) {
		System.out.println("ADDED TO TRANSACTION"+s.toString());
		transactionScope.addStatement(s);
	}

	@Override
	public void removedStatement(Statement s) {
		System.out.println("REMOVED TRANSACTION"+s.toString());
		transactionScope.removeStatement(s);
	}

	
	public void commitTransaction(String commitMsg) {
		var scope = transactionScope;
		scope.setCommitMessage(commitMsg);
		transactionScope = new StatementCommit();
		targetModel.add(scope.getAddedStatements());
		targetModel.remove(scope.getRemovedStatements());
	}
	
	public StatementCommit abortTransaction() {
		var scope = transactionScope;
		transactionScope = new StatementCommit(); 
		return scope;
	}
}

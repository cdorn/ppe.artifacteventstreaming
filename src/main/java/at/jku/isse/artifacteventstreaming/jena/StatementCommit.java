package at.jku.isse.artifacteventstreaming.jena;

import java.util.LinkedList;
import java.util.List;

import org.apache.jena.rdf.model.Statement;

import lombok.Getter;
import lombok.Setter;

public class StatementCommit {

	@Getter
	private final List<Statement> addedStatements = new LinkedList<>();
	@Getter
	private final List<Statement> removedStatements = new LinkedList<>();
	@Getter @Setter
	private String commitMessage;
	
	public void addStatement(Statement stmt) {
		addedStatements.add(stmt);
	}
	
	public void removeStatement(Statement stmt) {
		removedStatements.add(stmt);
	}
	
}

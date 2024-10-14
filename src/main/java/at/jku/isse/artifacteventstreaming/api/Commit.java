package at.jku.isse.artifacteventstreaming.api;

import java.util.List;

import org.apache.jena.rdf.model.Statement;

public interface Commit {

	String getCommitMessage();

	String getCommitId();

	String getPrecedingCommitId();

	String getOriginatingBranchId();

	void appendAddedStatements(List<Statement> stmts);

	void appendRemovedStatement(List<Statement> stmts);

	List<Statement> getAddedStatements();

	List<Statement> getRemovedStatements();

}
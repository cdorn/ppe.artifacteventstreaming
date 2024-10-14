package at.jku.isse.artifacteventstreaming.rdf;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.jena.rdf.model.Statement;

import at.jku.isse.artifacteventstreaming.api.Commit;
import lombok.Getter;
import lombok.Setter;

public class StatementCommitImpl implements Commit {

	private final List<Statement> addedStatements = new LinkedList<>();
	private final List<Statement> removedStatements = new LinkedList<>();
	@Getter
	private final String commitMessage;
	@Getter
	private final String commitId;
	@Getter
	private final String precedingCommitId;
	@Getter
	private final String originatingBranchId;
	
	public StatementCommitImpl(String branchId, String commitMsg, String precedingCommitId) {
		this.commitMessage = commitMsg;
		this.originatingBranchId = branchId;
		this.commitId = generateUUID();
		this.precedingCommitId = precedingCommitId;
	}
	
	public StatementCommitImpl(String branchId, String commitMsg, String precedingCommitId, List<Statement> addedStatements, List<Statement> removedStatements) {
		this(branchId, commitMsg, precedingCommitId);
		this.addedStatements.addAll(addedStatements);
		this.removedStatements.addAll(removedStatements);
	}
	
	protected StatementCommitImpl(String branchId, String mergedCommitId, String commitMsg, String precedingCommitId, List<Statement> addedStatements, List<Statement> removedStatements) {
		this.commitMessage = commitMsg;
		this.originatingBranchId = branchId;
		this.commitId = mergedCommitId;
		this.precedingCommitId = precedingCommitId;
		addedStatements.addAll(addedStatements);
		removedStatements.addAll(removedStatements);
	}
	
	private String generateUUID() {
		return UUID.randomUUID().toString();
	}
	
	@Override
	public void appendAddedStatements(List<Statement> stmts) {
		addedStatements.addAll(stmts);
	}
	
	@Override
	public void appendRemovedStatement(List<Statement> stmts) {
		removedStatements.addAll(stmts);
	}
	
	@Override
	public List<Statement> getAddedStatements() {
		return new ArrayList<>(addedStatements);
	}
	
	@Override
	public List<Statement> getRemovedStatements() {
		return new ArrayList<>(removedStatements);
	}
	
}

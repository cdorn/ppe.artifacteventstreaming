package at.jku.isse.artifacteventstreaming.rdf;

import java.util.Collections;
import java.util.List;

import org.apache.jena.rdf.model.Statement;

import at.jku.isse.artifacteventstreaming.api.Commit;

public class PoisonPillCommit implements Commit {

	public static final PoisonPillCommit POISONPILL = new PoisonPillCommit();
	
	private PoisonPillCommit() {
		
	}
	
	@Override
	public String getCommitMessage() {
		return "";
	}

	@Override
	public String getCommitId() {
		return "";
	}

	@Override
	public String getPrecedingCommitId() {
		return "";
	}

	@Override
	public String getOriginatingBranchId() {
		return "";
	}

	@Override
	public void appendAddedStatements(List<Statement> stmts) {
		//no op
	}

	@Override
	public void appendRemovedStatement(List<Statement> stmts) {
		// no op
	}

	@Override
	public List<Statement> getAddedStatements() {
		return Collections.emptyList();
	}

	@Override
	public List<Statement> getRemovedStatements() {
		return Collections.emptyList();
	}

}

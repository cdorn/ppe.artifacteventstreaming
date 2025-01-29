package at.jku.isse.artifacteventstreaming.branch;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;

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
	public void appendAddedStatements(Set<? extends ContainedStatement> stmts) {
		//no op
	}

	@Override
	public void appendRemovedStatement(Set<? extends ContainedStatement> stmts) {
		// no op
	}

	@Override
	public List<ContainedStatement> getAddedStatements() {
		return Collections.emptyList();
	}

	@Override
	public List<ContainedStatement> getRemovedStatements() {
		return Collections.emptyList();
	}
	
	@Override
	public Set<ContainedStatement> getAddedStatementsAsSet() {
		return Collections.emptySet();
	}

	@Override
	public Set<ContainedStatement> getRemovedStatementsAsSet() {
		return Collections.emptySet();
	}

	@Override
	public int getAdditionCount() {
		return 0;
	}

	@Override
	public int getRemovalCount() {
		return 0;
	}

	@Override
	public boolean isEmpty() {		
		return true;
	}

	@Override
	public void removeEffectlessStatements(int baseAdds, int baseRemoves) {
		//noop
	}

	@Override
	public long getTimeStamp() {		
		return -1;
	}



}

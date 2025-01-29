package at.jku.isse.artifacteventstreaming.api;

import java.util.List;
import java.util.Set;

public interface Commit {

	String getCommitMessage();

	String getCommitId();

	String getPrecedingCommitId();

	String getOriginatingBranchId();

	void appendAddedStatements(Set<? extends ContainedStatement> stmts);

	void appendRemovedStatement(Set<? extends ContainedStatement> stmts);

	List<ContainedStatement> getAddedStatements();

	List<ContainedStatement> getRemovedStatements();
	
	Set<ContainedStatement> getAddedStatementsAsSet();

	Set<ContainedStatement> getRemovedStatementsAsSet();
	
	int getAdditionCount();
	
	int getRemovalCount();

	boolean isEmpty();

	void removeEffectlessStatements(int baseAdds, int baseRemoves);

	long getTimeStamp();

}
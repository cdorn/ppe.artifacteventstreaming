package at.jku.isse.artifacteventstreaming.api;

import java.util.List;

public interface PerBranchEventStore {

	
	public List<Commit> loadAllCommits() throws Exception;
	
	public List<Commit> loadAllIncomingCommitsForBranchFromCommitIdOnward(String fromCommitIdOnwards) throws Exception;
	
	public void appendCommit(Commit commit) throws Exception;
	
	public void appendCommitDelivery(CommitDeliveryEvent event) throws Exception;
	
}

package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import lombok.NonNull;

import java.util.List;

public interface PerBranchEventStore {

	
	public List<Commit> loadAllCommits() throws PersistenceException;
	
	public List<Commit> loadAllIncomingCommitsForBranchFromCommitIdOnward(String fromCommitIdOnwards) throws PersistenceException;
	
	public void appendCommit(@NonNull Commit commit) throws PersistenceException;
	
	public void appendCommitDelivery(@NonNull CommitDeliveryEvent event) throws PersistenceException;
	
}

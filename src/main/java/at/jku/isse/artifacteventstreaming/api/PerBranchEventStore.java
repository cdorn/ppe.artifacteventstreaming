package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import lombok.NonNull;

import java.util.List;

public interface PerBranchEventStore {

	
	List<Commit> loadAllCommits() throws PersistenceException;
	
	List<Commit> loadAllIncomingCommitsForBranchFromCommitIdOnward(String fromCommitIdOnwards) throws PersistenceException;
	
	void appendCommit(@NonNull Commit commit) throws PersistenceException;
	
	void appendCommitDelivery(@NonNull CommitDeliveryEvent event) throws PersistenceException;
	
}

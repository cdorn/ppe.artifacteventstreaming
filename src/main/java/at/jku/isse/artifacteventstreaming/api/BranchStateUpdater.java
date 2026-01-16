package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

import java.util.List;
import java.util.Optional;

/**
 * @author Christoph Mayr-Dorn
 *
 * Keeps track of state (which commits have been produced, seen, etc,) 
 * but does not restart processing, --> done by branch impl
 *
 */
public interface BranchStateUpdater extends BranchStateKeeper {

	/**
	 * @throws Exception when loading the history from the event database failed
	 */
    void loadState() throws PersistenceException;
	
	//public void beforeServices(Commit commit) throws PersistenceException;
	
	void afterServices(Commit commit) throws PersistenceException;
	
	void beforeMerge(Commit commit) throws PersistenceException;
	
	List<Commit> getNonMergedCommits() throws PersistenceException;
	
	void finishedMerge(Commit commit) throws PersistenceException;
	


	void afterForwarded(Commit commit) throws PersistenceException;
	
	List<Commit> getNonForwardedCommits() throws PersistenceException;
	
	Optional<String> getLastForwardedCommitId() throws PersistenceException;

	

	

	
	
	
	
}

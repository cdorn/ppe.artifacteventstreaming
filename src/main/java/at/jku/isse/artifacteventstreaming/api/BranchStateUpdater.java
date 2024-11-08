package at.jku.isse.artifacteventstreaming.api;

import java.util.List;
import java.util.Optional;

import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

/**
 * @author Christoph Mayr-Dorn
 *
 * Keeps track of state (which commits have been produced, seen, etc,) 
 * but does not restart processing, --> done by branch impl
 *
 */
public interface BranchStateUpdater extends BranchStateKeeper {

	/**
	 * @return any preliminary commit that was persisted but not completely processed by any service
	 * @throws Exception when loading the history from the event database failed
	 */
	public Commit loadState() throws PersistenceException;
	
	public void beforeServices(Commit commit) throws PersistenceException;
	
	public void afterServices(Commit commit) throws PersistenceException;
	
	public void beforeMerge(Commit commit) throws PersistenceException;
	
	public List<Commit> getNonMergedCommits() throws PersistenceException;
	
	public void finishedMerge(Commit commit) throws PersistenceException;
	
	//public Optional<String> getLastMergedCommitId();

	public void afterForwarded(Commit commit) throws PersistenceException;
	
	public List<Commit> getNonForwardedCommits() throws PersistenceException;
	
	public Optional<String> getLastForwardedCommitId() throws PersistenceException;

	

	

	
	
	
	
}

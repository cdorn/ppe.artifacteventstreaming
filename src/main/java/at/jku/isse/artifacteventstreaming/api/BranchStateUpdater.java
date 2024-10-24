package at.jku.isse.artifacteventstreaming.api;

import java.util.List;
import java.util.Optional;

import org.apache.jena.ontapi.model.OntModel;

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
	public Commit loadState() throws Exception;
	
	public void beforeServices(Commit commit) throws Exception;
	
	public void afterServices(Commit commit) throws Exception;
	
	public void beforeMerge(Commit commit) throws Exception;
	
	public List<Commit> getNonMergedCommits() throws Exception;
	
	public void finishedMerge(Commit commit) throws Exception;
	
	public Optional<String> getLastMergedCommitId();

	public void afterForwarded(Commit commit) throws Exception;
	
	public List<Commit> getNonForwardedCommits();
	
	public Optional<String> getLastForwardedCommitId();

	

	

	
	
	
	
}

package at.jku.isse.artifacteventstreaming.api;

import java.util.List;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;

import lombok.NonNull;


public interface Branch {

	public OntModel getModel();
	public Dataset getDataset();
	public OntIndividual getBranchResource();
	public String getBranchId();
	public String getBranchName();
	public String getRepositoryURI();
	public Commit getLastCommit();
	
	//public BlockingQueue<Commit> getInQueue();	
	//public BlockingQueue<Commit> getOutQueue();
	
	/**
	 * @param commitMsg
	 *  * Upon commit transaction, it does the following: 
	 *  0) creates a commit
	 *  // PRE service augmentation phase:
 *    1) it commits the transaction to the event db (temporal persistance to survive restart of application, not needed upon persistance of augmented commit) 
 *    2) it stores which commit id has been produced in the ehcache
 *    3) it commits the changes to the backend model db / the authoritative source of the model state
 *    // Service augmentation phase
 *    4) it starts the cycle of commit augmentation by forwarding the commit to the first service
 *     service need to ensure they leave the model in a consistent state, resp add only consistent statements to the commit
 *     when no more statements are added (service exceptions are caught and ignored):
 *     // POST service augmentation phase
 *    5) it commits the augmented transaction to the event db (router skips steps 1 to 4 when no services are configured)
 *    6) it store the commit id as being augmented in the ehcache
 *    7) it commits the changes to the backend model (internal transaction)
 *    8) it puts the augmented commit into the branch's outqueue.
 *    @return the newly created/augmented commit
	 * @throws Exception 
	 */
	public Commit commitChanges(String commitMsg) throws Exception;
	
	/**
	 * @param mergedCommit 
	 * behaves like for a local commitTransaction, except that it takes the merged commit content as base, 
	 * and before persisting splits the commit into base commit and local augmentation
	 * originating branch becomes the local branch (as this is now in the history of the local branch)
	 * @return the augmentation commit by any service additions, if no augmentation, returns merged commit
	 * @throws Exception 
	 */
	public Commit commitMergeOf(Commit mergedCommit) throws Exception;
	
	/**
	 * drops all currently cached changes/statements
	 * and aborts the model transaction (undoes the changes)
	 */
	public void undoNoncommitedChanges();
	
		
	
	
	/**
	 * @param commit
	 * routes the commit through any available filters and processors (statements within the commit are not changes, just the decision which ones are applied)
	 * before applying the remaining statements onto this model within a transaction
	 * whenever there is a filter a replacement commit is created, to document that not the original commit was applied
	 * @throws Exception  when persisting in queue fails, then must assume commit has not been processed 
	 */
	public void enqueueIncomingCommit(Commit commit) throws Exception;
	

	/**
	 * @param handler
	 * adds this to the end of the chain of handlers that process an incoming commit.
	 * If this handler is already in the list, then it moves that handler to the current end of the chain
	 */
	public void appendIncomingCommitHandler(CommitHandler handler);
	
	
	public List<OntIndividual> getIncomingCommitHandlerConfig();
	
	/**
	 * @param handler
	 * removes the handler from the chain. When no handlers remain, any incoming commits are dropped/ignored.
	 */
	public void removeIncomingCommitHandler(CommitHandler handler);
	
	
	
	/**
	 * @param services
	 * adds this service to the end of the chain of services that process local changes.
	 * If this service is already in the list, then it moves that service to the current end of the chain
	 */
	public void appendCommitService(@NonNull BranchInternalCommitHandler service) ;
	
	/**
	 * @param service
	 * removes the service from the chain. When no services remain, any commit is put directly into the out queue.
	 */
	public void removeCommitService(@NonNull BranchInternalCommitHandler service) ;
	
	public List<OntIndividual> getLocalCommitServiceConfig();

	
	public void appendOutgoingCrossBranchCommitHandler(@NonNull CommitHandler crossBranchHandler);
	
	public void removeOutgoingCrossBranchCommitHandler(@NonNull CommitHandler crossBranchHandler);
	
	public List<OntIndividual> getOutgoingCommitDistributerConfig();
	
	
	/**
	 * @param unfinishedPreliminaryCommit any leftover commit to be processed by services upon restart from before a crash, can be null
	 * To signal that init of the branch is complete (incl state and/or history loading) and the commit handlers for in and out commits can start
	 * @throws Exception when handling of preliminary commit or any other replaying to get up to date fails
	 */
	void startCommitHandlers(Commit unfinishedPreliminaryCommit) throws Exception;
}

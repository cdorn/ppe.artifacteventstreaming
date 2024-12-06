package at.jku.isse.artifacteventstreaming.branch.outgoing;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.PoisonPillCommit;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CrossBranchStreamer implements Runnable {

	private final String branchId;
	private final BranchStateUpdater stateKeeper;
	private final BlockingQueue<Commit> sourceQueue;	
	private final Set<CommitHandler> outgoingCommitProcessors = new HashSet<>();
	
	/**
	 * for any commits not forwarded yet, re-add them to the out queue
	 */
	public void recoverState() throws PersistenceException {
		var nonForwarded = stateKeeper.getNonForwardedCommits();
		sourceQueue.addAll(nonForwarded);
	}
	
	@Override
	public void run() {
		try {
            while (true) {            	
            	Commit commit = sourceQueue.take();            	            	
                if (commit == PoisonPillCommit.POISONPILL) { // shutdown signal
                	log.info(String.format("Received shutdown command for branch %s", branchId));
                	return;
                } else {
                	forwardCommit(commit);
                	try {
						stateKeeper.afterForwarded(commit);
					} catch (Exception e) {
						log.warn("Error storing state: "+e.getMessage());
						// but we continue, perhaps not a permanent error, otherwise, if we crash, we would re-forward a commit, which should not have any side effects except for processing time.
					}
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	}
	
	protected void forwardCommit(Commit commit) {
		// persist which commit have been handed over 
		// --> better: streamer need to keep track of which commits they have seen and thus were to continue, 
		// notification here is only a mechanism to avoid for them to poll
		outgoingCommitProcessors.stream().forEach(processor -> {                    	
    		// each processor has the duty to add the commit reliably then to their configured branch's inqueue
    		try {
				processor.handleCommit(createShallowClone(commit));
			} catch (Exception e) {
				log.info(String.format("Forwarding Commit %s from %s failed for processor %s due to %s", commit.getCommitId(), branchId, processor.toString(), e.getMessage()));
			}                            		
    	});   
	}
	
	public void addOutgoingCommitHandler(CommitHandler handler) {
		outgoingCommitProcessors.add(handler);
	}
	
	public void removeOutgoingCommitHandler(CommitHandler handler) {
		outgoingCommitProcessors.remove(handler);
	}
	
	public int getHandlerCount() {
		return outgoingCommitProcessors.size();
	}
	
	/**
	 * @param cloneSource
	 * @return a commit with the same id, message, preceding commit, and branch, with separate statement lists, but references back to the same statements, i.e., no statement cloning occurs
	 */
	private Commit createShallowClone(Commit cloneSource) {
		StatementCommitImpl clone = new StatementCommitImpl(cloneSource.getOriginatingBranchId()
				, cloneSource.getCommitId()
				, cloneSource.getCommitMessage()
				, cloneSource.getPrecedingCommitId()
				, cloneSource.getTimeStamp()
				, new LinkedHashSet<>(cloneSource.getAddedStatements())
				, new LinkedHashSet<>(cloneSource.getRemovedStatements()));
		return clone;
	}
}

package at.jku.isse.artifacteventstreaming.branch.outgoing;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.branch.PoisonPillCommit;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class CrossBranchStreamer implements Runnable {

	private final String branchId;
	private final BlockingQueue<Commit> sourceQueue;	
	private final Set<CommitHandler> outgoingCommitProcessors = new HashSet<>();
	
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
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	}
	
	protected void forwardCommit(Commit commit) {
		outgoingCommitProcessors.stream().forEach(processor -> {                    	
    		// each processor has the duty to add the commit then to their configured branch's inqueue
    		processor.handleCommit(deepClone(commit));                            		
    	});   
	}
	
	public void addOutgoingCommitHandler(CommitHandler handler) {
		outgoingCommitProcessors.add(handler);
	}
	
	public void removeOutgoingCommitHandler(CommitHandler handler) {
		outgoingCommitProcessors.remove(handler);
	}
	
	/**
	 * @param cloneFrom
	 * @return a commit with the same id, message, preceding commit, and branch, with separate statement lists, but references back to the same statements, i.e., no statement cloning occurs
	 */
	private Commit deepClone(Commit cloneFrom) {
		StatementCommitImpl clone = new StatementCommitImpl(cloneFrom.getOriginatingBranchId()
				, cloneFrom.getCommitId()
				, cloneFrom.getCommitMessage()
				, cloneFrom.getPrecedingCommitId()
				, new LinkedHashSet<>(cloneFrom.getAddedStatements())
				, new LinkedHashSet<>(cloneFrom.getRemovedStatements()));
		return clone;
	}
}

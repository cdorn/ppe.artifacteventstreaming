package at.jku.isse.artifacteventstreaming.branch.outgoing;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.branch.PoisonPillCommit;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

@RequiredArgsConstructor
@Slf4j
public class BasicCrossBranchStreamer implements Runnable {

    protected final BlockingQueue<Commit> sourceQueue;
    private final String branchId;
    private final Set<CommitHandler> outgoingCommitProcessors = new HashSet<>();

    @Override
    public void run() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                Commit commit = sourceQueue.take();
                if (commit == PoisonPillCommit.POISONPILL) { // shutdown signal
                    log.info("Received shutdown command for branch {}", branchId);
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
        // persist which commit have been handed over
        // --> better: streamer need to keep track of which commits they have seen and thus were to continue,
        // notification here is only a mechanism to avoid for them to poll
        outgoingCommitProcessors.stream().forEach(processor -> {
            // each processor has the duty to add the commit reliably then to their configured branch's inqueue
            try {
                processor.handleCommit(createShallowClone(commit));
            } catch (Exception e) {
                log.info("Forwarding Commit {} from {} failed for processor {} due to {}", commit.getCommitId(), branchId, processor.toString(), e.getMessage());
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
    protected Commit createShallowClone(Commit cloneSource) {
        return new StatementCommitImpl(cloneSource.getOriginatingBranchId()
                , cloneSource.getCommitId()
                , cloneSource.getCommitMessage()
                , cloneSource.getPrecedingCommitId()
                , cloneSource.getTimeStamp()
                , new LinkedHashSet<>(cloneSource.getAddedStatements())
                , new LinkedHashSet<>(cloneSource.getRemovedStatements())
                , cloneSource.getMergedCommitId()
                , cloneSource.getMergedFromBranchURI());
    }
}

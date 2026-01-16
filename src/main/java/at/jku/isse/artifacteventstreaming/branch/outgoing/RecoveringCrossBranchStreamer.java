package at.jku.isse.artifacteventstreaming.branch.outgoing;

import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.BlockingQueue;


@Slf4j
public class RecoveringCrossBranchStreamer extends BasicCrossBranchStreamer {

	protected final BranchStateUpdater stateKeeper;

	public RecoveringCrossBranchStreamer(BlockingQueue<Commit> sourceQueue, String branchId, @NonNull BranchStateUpdater stateKeeper) {
		super(sourceQueue, branchId);
		this.stateKeeper = stateKeeper;
	}

	protected void forwardCommit(Commit commit) {
		super.forwardCommit(commit);
		try {
			stateKeeper.afterForwarded(commit);
		} catch (Exception e) {
			log.warn("Error storing state: {}", e.getMessage());
			// but we continue, perhaps not a permanent error, otherwise, if we crash, we would re-forward a commit, which should not have any side effects except for processing time.
		}
	}

	/**
	 * for any commits not forwarded yet, re-add them to the out queue
	 */
	public void recoverState() throws PersistenceException {
		var nonForwarded = stateKeeper.getNonForwardedCommits();
		sourceQueue.addAll(nonForwarded);
	}

}

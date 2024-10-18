package at.jku.isse.artifacteventstreaming.rdf.distribution;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class OnDemandCatchUpStreamer implements CommitHandler {

	private final Branch destinationBranch;
	
	// reads persisted commits from source branch
	// reads cache entry from desitantionbranch latest merge
	// obtains all commit since last merge
	// appends them to inqueue of destination branch
	// finishes
	
	
	@Override
	public void handleCommit(Commit commit) {
		destinationBranch.enqueueIncomingCommit(commit);
	}

}

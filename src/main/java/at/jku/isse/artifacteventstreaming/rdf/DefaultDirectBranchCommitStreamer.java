package at.jku.isse.artifacteventstreaming.rdf;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultDirectBranchCommitStreamer implements CommitHandler {

	private final Branch destinationBranch;
	
	@Override
	public void handleCommit(Commit commit) {
		destinationBranch.enqueueIncomingCommit(commit);
	}

}

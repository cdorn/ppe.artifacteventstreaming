package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.LinkedList;
import java.util.List;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import lombok.Getter;

public abstract class CommitLoggingService implements IncrementalCommitHandler {

	@Getter final List<Commit> receivedCommits = new LinkedList<>();
	
	@Override
	public void handleCommit(Commit commit) {
		receivedCommits.add(commit);
	}
	
	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {		
		receivedCommits.add(commit);		
	}
}

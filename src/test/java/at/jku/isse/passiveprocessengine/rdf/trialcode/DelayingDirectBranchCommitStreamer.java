package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.LinkedList;
import java.util.List;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.branch.outgoing.DefaultDirectBranchCommitStreamer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DelayingDirectBranchCommitStreamer extends DefaultDirectBranchCommitStreamer {

	private final int sleepInMillis;
	private final String name;
	private Thread currentThread;
	@Getter final List<Commit> receivedCommits = new LinkedList<>();
	
	public DelayingDirectBranchCommitStreamer(Branch sourceBranch, Branch destinationBranch, BranchStateCache cache, int sleepInMillis, String name) {
		super(sourceBranch, destinationBranch, cache);
		this.sleepInMillis = sleepInMillis;
		this.name = name;
	}

	@Override
	public void handleCommit(Commit commit) {
		super.handleCommit(commit);
		try {
			log.debug(name+" Starting to 'work'");
			currentThread = Thread.currentThread();
			Thread.currentThread().sleep(sleepInMillis);
			super.handleCommit(commit);
			log.debug(name+" Ending 'work'");
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException("Faking crash");
		}
	}

	public void interrupt() {
		currentThread.interrupt();
	}
	
}

package at.jku.isse.passiveprocessengine.rdf.trialcode;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchInternalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.branch.outgoing.DefaultDirectBranchCommitStreamer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DelayingDirectBranchCommitStreamer extends DefaultDirectBranchCommitStreamer {

	private final int sleepInMillis;
	private final String name;
	private Thread currentThread;
	
	public DelayingDirectBranchCommitStreamer(Branch sourceBranch, Branch destinationBranch, BranchStateCache cache, int sleepInMillis, String name) {
		super(sourceBranch, destinationBranch, cache);
		this.sleepInMillis = sleepInMillis;
		this.name = name;
	}

	@Override
	public void handleCommit(Commit commit) {
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

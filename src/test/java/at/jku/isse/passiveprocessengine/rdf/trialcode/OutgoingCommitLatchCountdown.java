package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.concurrent.CountDownLatch;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class OutgoingCommitLatchCountdown implements CommitHandler {

	private final OntModel model;
	private final CountDownLatch latch;
	
	
	@Override
	public OntIndividual getConfigResource() {
		return model.createIndividual(AES.getURI()+this.getClass().getName());
	}

	@Override
	public void handleCommit(Commit commit) {
		log.info("Counting down after receiving commit: "+commit.getCommitMessage());
		latch.countDown();
	}
}

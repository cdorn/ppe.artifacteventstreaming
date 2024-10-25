package at.jku.isse.passiveprocessengine.rdf.trialcode;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class LongRunningNoOpLocalService extends CommitLoggingService {

	private final int sleepInMillis;
	private OntModel repoModel = OntModelFactory.createModel();
	
	@Override
	public void handleCommit(Commit commit) {
		super.handleCommit(commit);
		try {
			log.debug("Starting to 'work'");
			Thread.currentThread().sleep(sleepInMillis);
			log.debug("Ending 'work'");
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException("Faking crash");
		}
	}

	@Override
	public OntIndividual getConfigResource() {
		return repoModel.createIndividual(AES.getURI()+this.getClass().getName());
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		handleCommit(commit);
	}

}

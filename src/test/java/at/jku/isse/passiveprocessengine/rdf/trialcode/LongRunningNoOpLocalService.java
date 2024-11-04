package at.jku.isse.passiveprocessengine.rdf.trialcode;

import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LongRunningNoOpLocalService extends CommitLoggingService {

	public LongRunningNoOpLocalService(OntModel branchModel, int sleepInMillis) {
		super(branchModel);
		this.sleepInMillis = sleepInMillis;
	}
	
	public LongRunningNoOpLocalService(String name, OntModel branchModel, int sleepInMillis) {
		super(name, branchModel);
		this.sleepInMillis = sleepInMillis;
	}

	private final int sleepInMillis;
	
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
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		handleCommit(commit);
	}

	@Override
	protected String getServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+this.getClass().getSimpleName();
	}

}

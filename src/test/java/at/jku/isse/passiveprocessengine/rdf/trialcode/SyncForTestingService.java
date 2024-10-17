package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.concurrent.CountDownLatch;

import at.jku.isse.artifacteventstreaming.api.BranchInternalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.Commit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SyncForTestingService implements BranchInternalCommitHandler {
	
	final String serviceName;
	final CountDownLatch latch;
	
	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {		
		latch.countDown();
		log.debug(String.format("%s counted down to %s", serviceName, latch.getCount()));
	}

	@Override
	public String toString() {
		return "SyncForTestingService [serviceName=" + serviceName + "]";
	}
	
	

}

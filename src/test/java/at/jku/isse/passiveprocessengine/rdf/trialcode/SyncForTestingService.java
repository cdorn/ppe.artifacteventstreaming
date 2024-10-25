package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SyncForTestingService extends CommitLoggingService {
	
	public static final String SERVICE_TYPE_URI = CommitHandler.serviceTypeBaseURI+SyncForTestingService.class.getSimpleName();
	
	final String serviceName;
	final CountDownLatch latch;
	final OntModel model;
	
	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {		
		super.handleCommitFromOffset(commit, indexOfNewAddition, indexOfNewRemoval);

		log.debug(String.format("%s counted down to %s upon commit %s", serviceName, latch.getCount(), commit.getCommitMessage()));
		latch.countDown();
	}

	@Override
	public String toString() {
		return "SyncForTestingService [serviceName=" + serviceName + "]";
	}
	
	
	@Override
	public OntIndividual getConfigResource() {
		OntIndividual config =  model.createIndividual(AES.getURI()+this.getClass().getSimpleName());
		config.addProperty(AES.isConfigForServiceType, model.createResource(SERVICE_TYPE_URI));
		return config;
	}
	
	public static ServiceFactory getServiceFactory(String serviceName, CountDownLatch latch, OntModel model) {
		if (factory == null) {
			factory = new DefaultServiceFactory(serviceName, latch, model);
		}
		return factory;
	}
	
	private static DefaultServiceFactory factory = null;
	
	@RequiredArgsConstructor
	public static class DefaultServiceFactory implements ServiceFactory {
		
		final String serviceName;
		final CountDownLatch latch;
		final OntModel model;
		
		@Override
		public CommitHandler getCommitHandlerInstanceFor(Branch sourceBranch, OntIndividual serviceConfigEntryPoint) throws Exception {
			// simple, as we dont have any config to do
			return new SyncForTestingService(serviceName, latch, model);
		}
	}
}

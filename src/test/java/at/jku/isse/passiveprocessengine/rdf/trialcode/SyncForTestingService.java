package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchInternalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import at.jku.isse.artifacteventstreaming.rdf.BranchRepository;
import at.jku.isse.artifacteventstreaming.rdf.distribution.DefaultDirectBranchCommitStreamer;
import at.jku.isse.artifacteventstreaming.rdf.distribution.DefaultDirectBranchCommitStreamer.DefaultServiceFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class SyncForTestingService implements BranchInternalCommitHandler {
	
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
		latch.countDown();
		log.debug(String.format("%s counted down to %s", serviceName, latch.getCount()));
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

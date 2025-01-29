package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.concurrent.CountDownLatch;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SyncForTestingService extends CommitLoggingService {
	
	final CountDownLatch latch;	
	
	public SyncForTestingService(String serviceName, CountDownLatch latch, OntModel branchModel) {
		super(serviceName, branchModel);
		this.latch = latch;
	}

	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {		
		super.handleCommitFromOffset(commit, indexOfNewAddition, indexOfNewRemoval);

		log.debug(String.format("%s counting down from %s upon commit %s", serviceName, latch.getCount(), commit.getCommitMessage()));
		latch.countDown();
	}

	@Override
	public String toString() {
		return "SyncForTestingService [serviceName=" + serviceName + "]";
	}
	
	@Override
	public String getServiceTypeURI() {
		return getWellknownServiceTypeURI();
	}
	
	public static String getWellknownServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+SyncForTestingService.class.getSimpleName();
	}
	
//	@Override
//	public OntIndividual getConfigResource() {
//		OntIndividual config =  model.createIndividual(AES.getURI()+this.getClass().getSimpleName()+"#"+serviceName);
//		config.addProperty(AES.isConfigForServiceType, model.createResource(SERVICE_TYPE_URI));
//		config.addProperty(RDFS.label, serviceName);
//		return config;
//	}
	
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

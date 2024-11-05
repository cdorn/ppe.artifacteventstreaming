package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.branch.BranchRepository;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryDatasetLoader;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryStateKeeperFactory;
import at.jku.isse.passiveprocessengine.rdf.trialcode.LongRunningNoOpLocalService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestServiceRegistration {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repoServiceRegistering");
		
	
	
	@Test
	void testUnregisterInternalService() throws Exception {						
		BranchRepository repo = new BranchRepository(repoURI, new InMemoryDatasetLoader(), new InMemoryStateKeeperFactory(), new ServiceFactoryRegistry());
		
		OntModel repoModel = repo.getRepositoryModel(); //OntModelFactory.createModel();
		// add two services, ensure both get a commit
		// remove first service, ensure only later one gets commit
		
		CountDownLatch latch = new CountDownLatch(1);
		var outService = new SyncForTestingService("Out1", latch, repoModel);
		var localService1 = new LongRunningNoOpLocalService("Local1", repoModel, 500);
		var localService2 = new LongRunningNoOpLocalService("Local2", repoModel, 500);
		BranchImpl branch = (BranchImpl) repo.getInitializedBranchBuilder()
				.addBranchInternalCommitService(localService1)
				.addBranchInternalCommitService(localService2)
				.addOutgoingCommitDistributer(outService)
				.build();
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit1");
		boolean success = latch.await(5, TimeUnit.SECONDS);
		assert(success);
		assertEquals(1, localService1.getReceivedCommits().size());
		assertEquals(1, localService2.getReceivedCommits().size());
		assertEquals(1, outService.getReceivedCommits().size());
				
		RDFDataMgr.write(System.out, branch.getBranchResource().getModel(), Lang.TURTLE) ;
		
		latch = new CountDownLatch(1);
		var outService2 = new SyncForTestingService("Out2", latch, repoModel);
		branch.removeBranchInternalCommitService(localService1);
		branch.removeOutgoingCommitDistributer(outService);
		branch.appendOutgoingCommitDistributer(outService2);
				
		model.add(testResource, RDFS.label, model.createTypedLiteral(2));
		Commit commit2 = branch.commitChanges("TestCommit2");
		boolean success2 = latch.await(5, TimeUnit.SECONDS);
		
		RDFDataMgr.write(System.out, branch.getBranchResource().getModel(), Lang.TURTLE) ;
		assert(success2);
		assertEquals(1, localService1.getReceivedCommits().size());
		assertEquals(2, localService2.getReceivedCommits().size());
		assertEquals(1, outService.getReceivedCommits().size());
		assertEquals(1, outService2.getReceivedCommits().size());
	}
}

package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.incoming.CompleteCommitMerger;
import at.jku.isse.passiveprocessengine.rdf.trialcode.AllUndoService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.LongRunningNoOpLocalService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.OutgoingCommitLatchCountdown;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SimpleService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestServiceRegistration {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repoServiceRegistring");
		
	
	
	@Test
	void testUnregisterInternalService() throws Exception {						
		OntModel repoModel = OntModelFactory.createModel();
		// add two services, ensure both get a commit
		// remove first service, ensure only later one gets commit
		
		CountDownLatch latch = new CountDownLatch(1);
		var outService = new SyncForTestingService("Out1", latch, repoModel);
		var localService1 = new LongRunningNoOpLocalService(500);
		var localService2 = new LongRunningNoOpLocalService(500);
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.addBranchInternalCommitHandler(localService1)
				.addBranchInternalCommitHandler(localService2)
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
		branch.removeCommitService(localService1);
		branch.removeOutgoingCrossBranchCommitHandler(outService);
		branch.appendOutgoingCrossBranchCommitHandler(outService2);
				
		model.add(testResource, RDFS.label, model.createTypedLiteral(2));
		Commit commit2 = branch.commitChanges("TestCommit2");
		boolean success2 = latch.await(5, TimeUnit.SECONDS);
		assert(success2);
		assertEquals(1, localService1.getReceivedCommits().size());
		assertEquals(2, localService2.getReceivedCommits().size());
		assertEquals(1, outService.getReceivedCommits().size());
		assertEquals(1, outService2.getReceivedCommits().size());
	}
}

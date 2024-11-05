package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.URI;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryStateKeeperFactory;
import at.jku.isse.passiveprocessengine.rdf.trialcode.DelayingDirectBranchCommitStreamer;
import at.jku.isse.passiveprocessengine.rdf.trialcode.LongRunningNoOpLocalService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestRecoverBranchState {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/branchRecoveryTest");
	
	@Test
	void testRecoverPrelimCommit() throws Exception {
		// everything done inmemory!
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		//OntModel branchModel = OntModelFactory.createModel();
		StateKeeperFactory stateFactory = new InMemoryStateKeeperFactory();
		URI branchURI = URI.create(BranchBuilder.generateBranchURI(repoURI, "main"));
		BranchStateUpdater stateKeeper = stateFactory.createStateKeeperFor(branchURI);
		Branch branch = new BranchBuilder(repoURI, repoDataset, repoModel)
				.setStateKeeper(stateKeeper)				
				.addBranchInternalCommitService(new LongRunningNoOpLocalService(repoModel, 2000))
				.build();		
		OntModel model = branch.getModel();
		stateKeeper.loadState();
		branch.startCommitHandlers(null);
		// for setup create commit in a separate thread, 
		// have a longrunning local commit service, 
		// shutdown/interrupt thread
		Runnable task = () -> {
			branch.getDataset().begin();
			try {
				Resource testResource = model.createResource(repoURI+"#art1");
				model.add(testResource, RDFS.label, model.createTypedLiteral(1));
				branch.commitChanges("TestCommit");
				assert(false);
			} catch (Exception e) {
				assert(true);
			}
		};
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.execute(task);
		Thread.sleep(500); // enought to "persist" in memory
		executor.shutdownNow(); // interrupt service
		branch.deactivate();
		try {
			branch.getDataset().end();
			branch.getDataset().begin();
			Resource testResource = model.createResource(repoURI+"#art1");
			model.add(testResource, RDFS.label, model.createTypedLiteral(1));
			branch.commitChanges("IgnoredTestCommit");
			assert(false);
		} catch (Exception e) {			
			assert(true);
		}		
		
		CountDownLatch latch = new CountDownLatch(1);
		// recreate branch, ensure that commit is created by checking that commit is created
		// note that we are not testing recovery of service config, we set that manually here, just that the recovery mechanism works
		Dataset repoDataset2 = DatasetFactory.createTxnMem();
		OntModel repoModel2 =  OntModelFactory.createModel(repoDataset2.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		BranchStateUpdater stateKeeper2 = stateFactory.createStateKeeperFor(branchURI);
		Branch branch2 = new BranchBuilder(repoURI, repoDataset2, repoModel2)
				.setStateKeeper(stateKeeper2)				
				.addBranchInternalCommitService(new LongRunningNoOpLocalService(repoModel2, 2000))
				.addOutgoingCommitDistributer(new SyncForTestingService("Out1", latch, repoModel2))
				.build();		
		OntModel model2 = branch2.getModel();
		Commit unfinishedCommit = stateKeeper2.loadState();
		assertNotNull(unfinishedCommit);
		branch2.startCommitHandlers(unfinishedCommit);
		
		latch.await();
		assertEquals(1, stateKeeper2.getHistory().size());
	}

	@Test
	void testNonDeliveredCommitToMerge() throws Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		StateKeeperFactory stateFactory = new InMemoryStateKeeperFactory();
		URI branchURI = URI.create(BranchBuilder.generateBranchURI(repoURI, "main"));
		BranchStateUpdater stateKeeper = stateFactory.createStateKeeperFor(branchURI);
		CountDownLatch latch = new CountDownLatch(1);
		SyncForTestingService service1 = new SyncForTestingService("Out1", latch, repoModel);
		Branch branch = new BranchBuilder(repoURI, repoDataset, repoModel)
				.setStateKeeper(stateKeeper)				
				.addIncomingCommitMerger(new LongRunningNoOpLocalService(repoModel, 2000))
				.addOutgoingCommitDistributer(service1)
				.build();		
		OntModel model = branch.getModel();
		stateKeeper.loadState();
		branch.startCommitHandlers(null);

		Runnable task = () -> {
			// create commit to merge
			Resource testResource = ResourceFactory.createResource(repoURI+"#art1");
			Statement stmt = model.createStatement(testResource, RDFS.label, model.createTypedLiteral(1));
			Commit commit = new StatementCommitImpl(repoURI.toString()+"#someBranch", "Commit to Merge", null, Set.of(stmt), Collections.emptySet());
			//branch.getDataset().begin();
			try {
				 branch.enqueueIncomingCommit(commit);
			} catch (Exception e) {
				e.printStackTrace();
			}
		};
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.execute(task);
		Thread.sleep(500); // enought to "persist" in memory
		executor.shutdownNow(); // interrupt service
		assertEquals(0, service1.getReceivedCommits().size());
		
		// now 'restart' branch and check if commit is there
		CountDownLatch latch2 = new CountDownLatch(1);
		Dataset repoDataset2 = DatasetFactory.createTxnMem();
		OntModel repoModel2 =  OntModelFactory.createModel(repoDataset2.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		SyncForTestingService service2 = new SyncForTestingService("Out2", latch2, repoModel2);
		BranchStateUpdater stateKeeper2 = stateFactory.createStateKeeperFor(branchURI);
		Branch branch2 = new BranchBuilder(repoURI, repoDataset2, repoModel2)
				.setStateKeeper(stateKeeper2)		
				.addIncomingCommitMerger(new LongRunningNoOpLocalService(repoModel2, 500))		
				.addOutgoingCommitDistributer(service2)
				.build();		
		OntModel model2 = branch2.getModel();
		Commit unfinishedCommit = stateKeeper2.loadState();
		assertNull(unfinishedCommit);
		branch2.startCommitHandlers(unfinishedCommit);
		
		latch2.await();
		assertEquals(1, service2.getReceivedCommits().size());
	}
	
	@Test
	void testNonForwardedCommit() throws Exception {
		// setup two branches, interrupt forwarding
		StateKeeperFactory stateFactory = new InMemoryStateKeeperFactory();
		URI branchURI = URI.create(BranchBuilder.generateBranchURI(repoURI, "source"));
		BranchStateUpdater stateKeeper = stateFactory.createStateKeeperFor(branchURI);
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setBranchLocalName("source")
				.setStateKeeper(stateKeeper)				
				.build();		
		stateKeeper.loadState();
		
		URI branchURI2 = URI.create(BranchBuilder.generateBranchURI(repoURI, "dest"));
		BranchStateUpdater stateKeeper2 = stateFactory.createStateKeeperFor(branchURI2);
		Branch branch2 = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(stateKeeper2)	
				.setBranchLocalName("dest")
				.build();		
		stateKeeper2.loadState();	
		BranchStateCache cache = new InMemoryBranchStateCache();
		DelayingDirectBranchCommitStreamer streamer = new DelayingDirectBranchCommitStreamer(branch, branch2, cache, 2000, "Old");
		streamer.init();
		branch.appendOutgoingCommitDistributer(streamer);
		branch.startCommitHandlers(null);
		branch2.startCommitHandlers(null);
		
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("Commit to Forward");
		Thread.sleep(500); // enought to "persist" in memory
		streamer.interrupt();
		//branch.removeOutgoingCrossBranchCommitHandler(streamer);
		
		Thread.sleep(2000);
		assert(branch2.getLastCommit() == null); 
		assert(branch.getLastCommit() != null); 
		
		// now build the copy:
		BranchStateUpdater stateKeeperNew = stateFactory.createStateKeeperFor(branchURI);
		Branch branchNew = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setBranchLocalName("source")
				.setStateKeeper(stateKeeperNew)				
				.build();		
		stateKeeperNew.loadState();
		
		CountDownLatch latch = new CountDownLatch(2);
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		SyncForTestingService service1 = new SyncForTestingService("InNew", latch, repoModel);
		SyncForTestingService service2 = new SyncForTestingService("Out2New", latch, repoModel);
		BranchStateUpdater stateKeeperNew2 = stateFactory.createStateKeeperFor(branchURI2);
		Branch branchNew2 = new BranchBuilder(repoURI, repoDataset, repoModel)
				.setStateKeeper(stateKeeperNew2)	
				.setBranchLocalName("dest")
				.addIncomingCommitMerger(service1)
				.addOutgoingCommitDistributer(service2)
				.build();		
		stateKeeperNew2.loadState();	
		DelayingDirectBranchCommitStreamer streamer2 = new DelayingDirectBranchCommitStreamer(branchNew, branchNew2, cache, 500, "New");
		streamer2.init();
		branchNew.appendOutgoingCommitDistributer(streamer2);
		branchNew.startCommitHandlers(null);
		branchNew2.startCommitHandlers(null);
		
		boolean success = latch.await(5, TimeUnit.SECONDS);
		assert(success);
		assertEquals(1, service1.getReceivedCommits().size());
		assertEquals(1, service2.getReceivedCommits().size());
		assert(branchNew2.getLastCommit() != null); 
		
	}
	
}

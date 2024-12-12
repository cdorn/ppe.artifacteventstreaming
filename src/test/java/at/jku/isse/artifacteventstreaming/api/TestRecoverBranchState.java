package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.net.URI;
import java.net.URISyntaxException;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.incoming.CompleteCommitMerger;
import at.jku.isse.artifacteventstreaming.branch.outgoing.DefaultDirectBranchCommitStreamer;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryStateKeeperFactory;
import at.jku.isse.passiveprocessengine.rdf.trialcode.DelayingDirectBranchCommitStreamer;
import at.jku.isse.passiveprocessengine.rdf.trialcode.LongRunningNoOpLocalService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestRecoverBranchState {

	private static final String SOURCE = "source";
	private static final String DEST = "dest";

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/branchRecoveryTest");
	
	private static URI branchURI;
	private static URI branchURI2;
	
	@BeforeAll
	void setup() throws URISyntaxException {
		branchURI = BranchBuilder.generateBranchURI(repoURI, SOURCE);
		branchURI2 = BranchBuilder.generateBranchURI(repoURI, DEST);
	}
	
	@Test
	void testRecoverPrelimCommit() throws Exception {
		// everything done inmemory!
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		//OntModel branchModel = OntModelFactory.createModel();
		StateKeeperFactory stateFactory = new InMemoryStateKeeperFactory();
		URI branchURI = BranchBuilder.generateBranchURI(repoURI, "main");
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
			//branch.getDataset().end();
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
		
		boolean success = latch.await(2, TimeUnit.SECONDS);
		assert(success);
		assertEquals(1, stateKeeper2.getHistory().size());
	}

	@Test
	void testNonDeliveredCommitToMerge() throws Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		StateKeeperFactory stateFactory = new InMemoryStateKeeperFactory();
		URI branchURI = BranchBuilder.generateBranchURI(repoURI, "main");
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
			Commit commit = new StatementCommitImpl(repoURI.toString()+"#someBranch", "Commit to Merge", null, 0, Set.of(stmt), Collections.emptySet());
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
		
		boolean success = latch2.await(2, TimeUnit.SECONDS);
		assert(success);
		assertEquals(1, service2.getReceivedCommits().size());
	}
	
	@Test
	void testNonForwardedCommit() throws Exception {
		// setup two branches, interrupt forwarding
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		StateKeeperFactory stateFactory = new InMemoryStateKeeperFactory();
		
		BranchStateUpdater stateKeeper = stateFactory.createStateKeeperFor(branchURI);
		Branch branch = new BranchBuilder(repoURI, repoDataset, repoModel)
				.setBranchLocalName(SOURCE)
				.setStateKeeper(stateKeeper)				
				.build();		
		stateKeeper.loadState();
		
		BranchStateUpdater stateKeeper2 = stateFactory.createStateKeeperFor(branchURI2);
		Branch branch2 = new BranchBuilder(repoURI, repoDataset, repoModel)
				.setStateKeeper(stateKeeper2)	
				.setBranchLocalName(DEST)
				.build();		
		stateKeeper2.loadState();	
		BranchStateCache cache = new InMemoryBranchStateCache();
		DelayingDirectBranchCommitStreamer streamer = new DelayingDirectBranchCommitStreamer(branch, branch2, cache, 2000, "Old");
		streamer.init();
		branch.appendOutgoingCommitDistributer(streamer);
		branch.startCommitHandlers(null);
		branch2.startCommitHandlers(null);
		
		OntModel model = branch.getModel();
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("Commit to Forward");
		Thread.sleep(500); // enought to "persist" in memory
		streamer.interrupt();
		//branch.removeOutgoingCrossBranchCommitHandler(streamer);
		
		Thread.sleep(2000);
		assert(branch2.getLastCommit() == null); 
		assert(branch.getLastCommit() != null); 
		
		ensureForwarded(stateFactory, repoDataset, repoModel, cache, 1);							
	}
	
	@Test
	void testForwardedCommitToCatchUp() throws Exception {
		// we want to init the DefaultDirectBranchCommitStreamer with at least one previously forwarded commit.
		// setup two branches, interrupt forwarding after the second commit
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		StateKeeperFactory stateFactory = new InMemoryStateKeeperFactory();
		BranchStateUpdater stateKeeper = stateFactory.createStateKeeperFor(branchURI);
		Branch branch = new BranchBuilder(repoURI, repoDataset, repoModel)
				.setBranchLocalName(SOURCE)
				.setStateKeeper(stateKeeper)				
				.build();		
		stateKeeper.loadState();
		
		BranchStateUpdater stateKeeper2 = stateFactory.createStateKeeperFor(branchURI2);		
		Branch branch2 = new BranchBuilder(repoURI, repoDataset, repoModel)
				.setStateKeeper(stateKeeper2)				
				.setBranchLocalName(DEST)
				.build();		
		stateKeeper2.loadState();	
		var merger = new CompleteCommitMerger(branch2);
		branch2.appendIncomingCommitMerger(merger);
		
		BranchStateCache cache = new InMemoryBranchStateCache();		
		DelayingDirectBranchCommitStreamer streamer = new DelayingDirectBranchCommitStreamer(branch, branch2, cache, 1000, "Old");
		streamer.init();
		branch.appendOutgoingCommitDistributer(streamer);
		branch.startCommitHandlers(null);
		branch2.startCommitHandlers(null);

		OntModel model = branch.getModel();
		branch.getDataset().begin();
		//first commit to go through
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("First Commit to Forward");		
		
		//second commit to abort
		branch.getDataset().begin();
		model.add(testResource, RDFS.label, model.createTypedLiteral(2));
		Commit commit2 = branch.commitChanges("Second Commit to Forward");
		Thread.sleep(1500); // to "persist" in memory the first but not second 
		streamer.interrupt();		

		Thread.sleep(2000);
		assertEquals(commit.getCommitId(), branch2.getLastCommit().getCommitId()); 
		assertEquals(commit2.getCommitId(), branch.getLastCommit().getCommitId()); 

		ensureForwarded(stateFactory, repoDataset, repoModel, cache, 1);	
	}
	
	@Test
	void testForwardedCommitOnTrack() throws Exception{
		// setup two branches, dont interrupt after forwarding
				Dataset repoDataset = DatasetFactory.createTxnMem();
				OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
				StateKeeperFactory stateFactory = new InMemoryStateKeeperFactory();
				BranchStateUpdater stateKeeper = stateFactory.createStateKeeperFor(branchURI);
				Branch branch = new BranchBuilder(repoURI, repoDataset, repoModel)
						.setBranchLocalName(SOURCE)
						.setStateKeeper(stateKeeper)				
						.build();		
				stateKeeper.loadState();
				
				BranchStateUpdater stateKeeper2 = stateFactory.createStateKeeperFor(branchURI2);		
				Branch branch2 = new BranchBuilder(repoURI, repoDataset, repoModel)
						.setStateKeeper(stateKeeper2)				
						.setBranchLocalName(DEST)
						.build();		
				stateKeeper2.loadState();	
				var merger = new CompleteCommitMerger(branch2);
				branch2.appendIncomingCommitMerger(merger);
				
				BranchStateCache cache = new InMemoryBranchStateCache();		
				DefaultDirectBranchCommitStreamer streamer = new DefaultDirectBranchCommitStreamer(branch, branch2, cache);
				streamer.init();
				branch.appendOutgoingCommitDistributer(streamer);
				branch.startCommitHandlers(null);
				branch2.startCommitHandlers(null);

				OntModel model = branch.getModel();
				branch.getDataset().begin();
				//first commit to go through
				Resource testResource = model.createResource(repoURI+"#art1");
				model.add(testResource, RDFS.label, model.createTypedLiteral(1));
				Commit commit = branch.commitChanges("First Commit to Forward");		
				
				//second commit to go through as well
				branch.getDataset().begin();
				model.add(testResource, RDFS.label, model.createTypedLiteral(2));
				Commit commit2 = branch.commitChanges("Second Commit to Forward");
				Thread.sleep(1000); // to "persist" all in memory  
								
				assertEquals(commit2.getCommitId(), branch2.getLastCommit().getCommitId()); 
				assertEquals(commit2.getCommitId(), branch.getLastCommit().getCommitId()); 

				ensureForwarded(stateFactory, repoDataset, repoModel, cache, 0);	
	}
	
	
	private void ensureForwarded(StateKeeperFactory stateFactory, Dataset repoDataset, OntModel repoModel, BranchStateCache cache, int expCountReceived) throws Exception {
		// now we recreate branches		
		BranchStateUpdater stateKeeperNew = stateFactory.createStateKeeperFor(branchURI);
		Branch branchNew = new BranchBuilder(repoURI, repoDataset, repoModel)
				.setBranchLocalName(SOURCE)
				.setStateKeeper(stateKeeperNew)				
				.build();		
		stateKeeperNew.loadState();
		
		CountDownLatch latch = new CountDownLatch(2);		
		SyncForTestingService service1 = new SyncForTestingService("InNew", latch, repoModel);
		SyncForTestingService service2 = new SyncForTestingService("Out2New", latch, repoModel);
		BranchStateUpdater stateKeeperNew2 = stateFactory.createStateKeeperFor(branchURI2);
		Branch branchNew2 = new BranchBuilder(repoURI, repoDataset, repoModel)
				.setStateKeeper(stateKeeperNew2)	
				.setBranchLocalName(DEST)
				.addIncomingCommitMerger(service1)
				.addOutgoingCommitDistributer(service2)
				.build();		
		stateKeeperNew2.loadState();	
		DefaultDirectBranchCommitStreamer streamer2 = new DefaultDirectBranchCommitStreamer(branchNew, branchNew2, cache);
		streamer2.init();
		branchNew.appendOutgoingCommitDistributer(streamer2);
		branchNew.startCommitHandlers(null);
		branchNew2.startCommitHandlers(null);
		
		boolean success = latch.await(1, TimeUnit.SECONDS);
		//assert(success);
		assertEquals(expCountReceived, service1.getReceivedCommits().size());
		assertEquals(expCountReceived, service2.getReceivedCommits().size());
		assert(branchNew2.getLastCommit() != null); 	
	}
	

	
}

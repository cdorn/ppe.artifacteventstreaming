package at.jku.isse.artifacteventstreaming.api;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
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
import at.jku.isse.passiveprocessengine.rdf.trialcode.MockLazyLoadingService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SimpleService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestCommitHandling {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repo1");
		
	@Test
	void testCreateBranch() throws Exception {
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.build();
		System.out.println(branch.toString());
		assertEquals(branch.getBranchName(), "main");
		assertEquals(branch.getRepositoryURI(), repoURI.toString());		
	}
	
	
	
	@Test
	void testTwoServicesBranch() throws Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.addBranchInternalCommitService(new SimpleService("Service1", false, repoModel))
				.addBranchInternalCommitService(new SimpleService("Service2", true, repoModel))
				.build();
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(1, commit.getAddedStatements().size()); // we remove effectless changes, hence only the last change is kept.
		
		int lastLabel = testResource.getProperty(RDFS.label).getInt();
		assertEquals(21, lastLabel);
	}
	
	@Test
	void testAbortCommit() throws Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.addBranchInternalCommitService(new SimpleService("Service1", false, repoModel))
				.addBranchInternalCommitService(new SimpleService("Service2", true, repoModel))
				.build();
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		// now lets change and undo it
		branch.getDataset().begin();
		model.remove(testResource, RDFS.label, model.createTypedLiteral(21))
			 .add(testResource, RDFS.label, model.createTypedLiteral(-1));
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		branch.undoNoncommitedChanges();
		System.out.println("undo now:");
		int lastLabel = testResource.getProperty(RDFS.label).getInt();
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(21, lastLabel);
		Commit nullCommit = branch.commitChanges("NoChanges");
		assertNull(nullCommit);
	}

	@Test
	void testLoopControl() throws Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.build();
		OntModel model = branch.getModel();
		branch.appendBranchInternalCommitService(new MockLazyLoadingService("Loader", true, repoModel, model, 3));
		branch.appendBranchInternalCommitService(new MockLazyLoadingService("LoopController", false, repoModel, model, 4));
		branch.startCommitHandlers(null);
		
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		model.remove(testResource, RDFS.label, model.createTypedLiteral(2));
		model.remove(testResource, RDFS.label, model.createTypedLiteral(2));
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(2, model.size());
		assertEquals(1, commit.getAddedStatements().size());
		assertEquals(2, commit.getRemovedStatements().size());
	}
	
	@Test
	void testTrueChanges() throws Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.build();
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		model.remove(testResource, RDFS.label, model.createTypedLiteral(2)); // this should not result in an event, as there is no change to the model
		model.remove(testResource, RDFS.label, model.createTypedLiteral(2));
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(1, model.size());
		assertEquals(1, commit.getAddedStatements().size());
		//FIXME: JENA notification does not work correctly as events are provided for non-effective changes, e.g., removing something that is not in the model should not pop up
		assertEquals(0, commit.getRemovedStatements().size());
		
		branch.getDataset().begin();
		model.add(testResource, RDFS.label, model.createTypedLiteral(1)); // this should not result in an event, as there is no change to the model
		Commit commit2 = branch.commitChanges("TestCommit2");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(1, model.size());
		//FIXME: JENA notification does not work correctly as events are provided for non-effective changes
		assertEquals(0, commit2.getAddedStatements().size());
		assertEquals(0, commit2.getRemovedStatements().size());
	}
	
	@Test
	void testLoopDetection() throws Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		CountDownLatch latch = new CountDownLatch(1);
		var service = new SyncForTestingService("Out1", latch, repoModel);
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.addBranchInternalCommitService(new SimpleService("Service1", false, repoModel))
				.addBranchInternalCommitService(new SimpleService("Service2", true, repoModel))
				.addOutgoingCommitDistributer(service)
				.build();
		CommitHandler merger = new CompleteCommitMerger(branch);
		branch.appendIncomingCommitMerger(merger);
		branch.startCommitHandlers(null);
		
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		boolean success = latch.await(1,  TimeUnit.SECONDS);
		assertEquals(0, branch.getOutQueue().size());	
		assertEquals(1, service.getReceivedCommits().size());
		branch.removeOutgoingCommitDistributer(service); // replace by new one for with new latch
		
		latch = new CountDownLatch(1);
		service = new SyncForTestingService("Out2", latch, repoModel);
		branch.appendOutgoingCommitDistributer(service);
		
		branch.enqueueIncomingCommit(commit);		
		success = latch.await(1, TimeUnit.SECONDS); //this sometimes fails 
		assertFalse(success);
		assertEquals(0, branch.getInQueue().size());
		assertEquals(0, service.getReceivedCommits().size());
	}
	
	@Test
	void testSameCommitHandling() throws Exception {
		CountDownLatch latch = new CountDownLatch(2);
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.addBranchInternalCommitService(new SimpleService("Service1", false, repoModel))
				.addBranchInternalCommitService(new SimpleService("Service2", true, repoModel))
				.addOutgoingCommitDistributer(new SyncForTestingService("Out1", latch, repoModel))
				.build();
		Dataset dataset = branch.getDataset();
		CommitHandler merger = new CompleteCommitMerger(branch);
		branch.appendIncomingCommitMerger(merger);
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		
		
		Commit again = new StatementCommitImpl("blabl", "commitcopy", "", 0
				, new HashSet<>(commit.getAddedStatements())
				, new HashSet<>(commit.getRemovedStatements()));
		branch.enqueueIncomingCommit(again);
		//branch.enqueueIncomingCommit(commit); lets go around async queue
//		if (dataset.isInTransaction())
//			dataset.abort();
//		dataset.begin(ReadWrite.WRITE);
//		merger.handleCommit(commit);
//		// we need to mimick transaction management 
//		dataset.commit();
//		dataset.end();
//		dataset.begin();
//		branch.commitMergeOf(commit);
		boolean success = latch.await(5, TimeUnit.SECONDS);
		assert(success);
		assertEquals(0, branch.getOutQueue().size()); // queue gets emptied right away
	}
	
	@Test
	void testCancelingOutLiteralStatements() throws Exception {
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())				
				.build();
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		testResource.removeAll(RDFS.label);
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertNull(commit);
	}
	
	@Test
	void testCancelingOutResourceStatements() throws Exception {
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())				
				.build();
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		Resource art2 = model.createResource(repoURI+"#art2");
		model.add(testResource, RDFS.seeAlso, art2);
		testResource.removeAll(RDFS.seeAlso);
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertNull(commit);
	}
	
	@Test
	void testNonCancelingOutLiteralStatements() throws Exception {
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())				
				.build();
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		Literal lit1 = model.createTypedLiteral(1);		
		model.add(testResource, RDFS.label, lit1);
		Literal lit2 = model.createTypedLiteral(1.0);
		var stmt = model.createStatement(testResource, RDFS.label, lit2);
		model.remove(stmt);		
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertNotNull(commit);
	}
	
	@Test
	void testUndoServiceStatements() throws Exception {
		OntModel repoModel = OntModelFactory.createModel();
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.addBranchInternalCommitService(new AllUndoService("UndoService1", repoModel))
				.build();
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		branch.getDataset().begin();
		var modelSize = model.size();
		Resource testResource = model.createResource(repoURI+"#art1");
		Literal lit1 = model.createTypedLiteral(1);		
		model.add(testResource, RDFS.label, lit1);
		Literal lit2 = model.createTypedLiteral(1.0);
		model.add(testResource, RDFS.label, lit2);		
		Resource art2 = model.createResource(repoURI+"#art2");
		model.add(testResource, RDFS.seeAlso, art2);
		
		Commit commit = branch.commitChanges("TestCommit");
		var modelSizeAfter = model.size();				
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(modelSize, modelSizeAfter);
		assertTrue(commit.isEmpty());
	}
	
	@Test
	void testCommitComparison() {
		StatementCommitImpl commit1 = new StatementCommitImpl("", "", "", "", 0, Collections.emptySet(), Collections.emptySet());
		StatementCommitImpl commit2 = new StatementCommitImpl("", "", "", "", 0, Collections.emptySet(), Collections.emptySet());
		StatementCommitImpl commit3 = new StatementCommitImpl("", "1", "", "", 0, Collections.emptySet(), Collections.emptySet());
		assertEquals(commit1, commit2);
		assertNotEquals(commit1, commit3);
				
		commit1.toString();
		assertEquals(commit1.hashCode(), commit2.hashCode());
				
	}

}

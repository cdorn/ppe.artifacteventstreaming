package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.rdf.BranchBuilder;
import at.jku.isse.artifacteventstreaming.rdf.CompleteCommitMerger;
import at.jku.isse.passiveprocessengine.rdf.trialcode.AllUndoService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SimpleService;

class TestCommitHandling {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repo1");
		
	@Test
	void testCreateBranch() throws Exception {
		Branch branch = new BranchBuilder(repoURI)
				.build();
		assertEquals(branch.getBranchName(), "main");
	}
	
	@Test
	void testTwoServicesBranch() throws Exception {
		Branch branch = new BranchBuilder(repoURI)
				.addBranchInternalCommitHandler(new SimpleService("Service1", false))
				.addBranchInternalCommitHandler(new SimpleService("Service2", true))
				.build();
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(21, commit.getAddedStatements().size());
		
		int lastLabel = testResource.getProperty(RDFS.label).getInt();
		assertEquals(21, lastLabel);
	}
	
	@Test
	void testAbortCommit() throws Exception {
		Branch branch = new BranchBuilder(repoURI)
				.addBranchInternalCommitHandler(new SimpleService("Service1", false))
				.addBranchInternalCommitHandler(new SimpleService("Service2", true))
				.build();
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		// now lets change and undo it
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
	void testLoopDetection() throws Exception {
		Branch branch = new BranchBuilder(repoURI)
				.addBranchInternalCommitHandler(new SimpleService("Service1", false))
				.addBranchInternalCommitHandler(new SimpleService("Service2", true))
				.build();
		Dataset dataset = branch.getDataset();
		CommitHandler merger = new CompleteCommitMerger(branch);
		branch.appendIncomingCommitHandler(merger);
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		assertEquals(1, branch.getOutQueue().size());
		branch.enqueueIncomingCommit(commit);
		assertEquals(0, branch.getInQueue().size());
		assertEquals(1, branch.getOutQueue().size());
	}
	
	@Test
	void testSameCommitHandling() throws Exception {
		Branch branch = new BranchBuilder(repoURI)
				.addBranchInternalCommitHandler(new SimpleService("Service1", false))
				.addBranchInternalCommitHandler(new SimpleService("Service2", true))
				.build();
		Dataset dataset = branch.getDataset();
		CommitHandler merger = new CompleteCommitMerger(branch);
		branch.appendIncomingCommitHandler(merger);
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		//branch.enqueueIncomingCommit(commit); lets go around async queue
		if (dataset.isInTransaction())
			dataset.abort();
		dataset.begin(ReadWrite.WRITE);
		merger.handleCommit(commit);
		// we need to mimick transaction management 
		dataset.commit();
		dataset.end();
		dataset.begin();
		branch.commitMergeOf(commit);
		assertEquals(2, branch.getOutQueue().size());
	}
	
	@Test
	void testCancelingOutLiteralStatements() throws Exception {
		Branch branch = new BranchBuilder(repoURI)				
				.build();
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		testResource.removeAll(RDFS.label);
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertNull(commit);
	}
	
	@Test
	void testCancelingOutResourceStatements() throws Exception {
		Branch branch = new BranchBuilder(repoURI)				
				.build();
		OntModel model = branch.getModel();
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
		Branch branch = new BranchBuilder(repoURI)				
				.build();
		OntModel model = branch.getModel();
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
		Branch branch = new BranchBuilder(repoURI)	
				.addBranchInternalCommitHandler(new AllUndoService("UndoService1"))
				.build();
		OntModel model = branch.getModel();
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
}

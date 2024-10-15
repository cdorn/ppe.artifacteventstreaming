package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.process.normalize.StreamCanonicalLiterals;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.rdf.BranchBuilder;
import at.jku.isse.artifacteventstreaming.rdf.CompleteCommitMerger;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SimpleService;

class TestEventPersisting {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repo1");
		
	@Test
	void testCreateBranch() throws URISyntaxException, IOException {
		Branch branch = new BranchBuilder(repoURI)
				.build();
		assertEquals(branch.getBranchName(), "main");
	}
	
	@Test
	void testTwoServicesBranch() throws URISyntaxException, IOException {
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
	void testAbortCommit() {
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
	void testLoopDetection() {
		Branch branch = new BranchBuilder(repoURI)
				.addBranchInternalCommitHandler(new SimpleService("Service1", false))
				.addBranchInternalCommitHandler(new SimpleService("Service2", true))
				.build();
		Dataset dataset = branch.getDataset();
		CommitHandler merger = new CompleteCommitMerger(branch, dataset);
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
	void testSameCommitHandling() {
		Branch branch = new BranchBuilder(repoURI)
				.addBranchInternalCommitHandler(new SimpleService("Service1", false))
				.addBranchInternalCommitHandler(new SimpleService("Service2", true))
				.build();
		Dataset dataset = branch.getDataset();
		CommitHandler merger = new CompleteCommitMerger(branch, dataset);
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
		dataset.commit();
		dataset.end();
		branch.commitMergeOf(commit);
		assertEquals(2, branch.getOutQueue().size());
	}
}

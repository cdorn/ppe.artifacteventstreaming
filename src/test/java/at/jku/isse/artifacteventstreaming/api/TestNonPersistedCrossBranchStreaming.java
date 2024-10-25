package at.jku.isse.artifacteventstreaming.api;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.branch.incoming.CompleteCommitMerger;
import at.jku.isse.artifacteventstreaming.branch.outgoing.DefaultDirectBranchCommitStreamer;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestNonPersistedCrossBranchStreaming {

	public static URI repoURI1 = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/streaming1");							
	public static URI repoURI2 = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/streaming2");
	public static URI repoURI3 = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/streaming3");
	
	@Test 
	void testStreamFrom1to2Branches() throws Exception {	
		
		int commitRounds = 1;
		int changeCount = 10;
		CountDownLatch latch = new CountDownLatch(commitRounds*2);
		OntModel repoModel = OntModelFactory.createModel();
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI1, DatasetFactory.createTxnMem())	
				.setBranchLocalName("branch1")
				.build();		
		OntModel model = branch.getModel();
		
		Branch branch2 = new BranchBuilder(repoURI2, DatasetFactory.createTxnMem())
				.setBranchLocalName("branch2")
				.addBranchInternalCommitHandler(new SyncForTestingService("Branch2Signaller", latch, repoModel))
				.build();		
		branch2.appendIncomingCommitHandler(new CompleteCommitMerger(branch2));

		Branch branch3 = new BranchBuilder(repoURI3, DatasetFactory.createTxnMem())
				.setBranchLocalName("branch3")
				.addBranchInternalCommitHandler(new SyncForTestingService("Branch3Signaller", latch, repoModel))
				.build();				
		branch3.appendIncomingCommitHandler(new CompleteCommitMerger(branch3));


		branch.appendOutgoingCrossBranchCommitHandler(new DefaultDirectBranchCommitStreamer(branch, branch2, new InMemoryBranchStateCache()));
		branch.appendOutgoingCrossBranchCommitHandler(new DefaultDirectBranchCommitStreamer(branch, branch3, new InMemoryBranchStateCache()));
		branch.startCommitHandlers(null);
		branch2.startCommitHandlers(null);
		branch3.startCommitHandlers(null);
		
		
		for (int j = 0; j < commitRounds; j++) {
			Resource testResource = model.createResource(repoURI1+"#art"+j);
			for (int i = 0; i<changeCount ; i++) {
				model.add(testResource, RDFS.label, model.createTypedLiteral(i));	
			}				
			Commit commit = branch.commitChanges("TestCommit"+j);			
		}
				
		latch.await();
		
		branch.getDataset().end();
		OntModel model2 = branch2.getModel();
		branch2.getDataset().end();
		OntModel model3 = branch3.getModel();
		branch3.getDataset().end();
		//why are models 2 and 3 sizes not updated, some content, while logging says statements have been added
		//hmm, apparently we need to have dataset.end()
		System.out.println("Model 1 size: "+model.size());
		System.out.println("Model 2 size: "+model2.size());
		System.out.println("Model 3 size: "+model3.size());
		
		System.out.println("Source Model: ------------------------------------");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		System.out.println("Dest Model 2: ------------------------------------");
		RDFDataMgr.write(System.out, model2, Lang.TURTLE) ;
		System.out.println("Dest Model 3: ------------------------------------");
		RDFDataMgr.write(System.out, model3, Lang.TURTLE) ;
				
		//cannot compare full model anymore as the branch info is different
		assertEquals(model.size(), model2.size());
		assertEquals(model3.size(), model2.size());
	}
	
	
}

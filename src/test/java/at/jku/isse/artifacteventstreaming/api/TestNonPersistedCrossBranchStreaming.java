package at.jku.isse.artifacteventstreaming.api;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
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
		
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
				
		BranchImpl branch = (BranchImpl) new BranchBuilder(repoURI1, repoDataset, repoModel )	
				.setBranchLocalName("branch1")
				.build();		
		OntModel model = branch.getModel();
		
		var branch2signaller = new SyncForTestingService("Branch2Signaller", latch, repoModel);
		BranchImpl branch2 = (BranchImpl) new BranchBuilder(repoURI2, DatasetFactory.createTxnMem())
				.setBranchLocalName("branch2")
				.addBranchInternalCommitService(branch2signaller)
				.build();		
		var merger2 = new CompleteCommitMerger(branch2);
		branch2.appendIncomingCommitMerger(merger2);

		Branch branch3 = new BranchBuilder(repoURI3, DatasetFactory.createTxnMem())
				.setBranchLocalName("branch3")
				.addBranchInternalCommitService(new SyncForTestingService("Branch3Signaller", latch, repoModel))
				.build();				
		branch3.appendIncomingCommitMerger(new CompleteCommitMerger(branch3));


		branch.appendOutgoingCommitDistributer(new DefaultDirectBranchCommitStreamer(branch, branch2, new InMemoryBranchStateCache()));
		branch.appendOutgoingCommitDistributer(new DefaultDirectBranchCommitStreamer(branch, branch3, new InMemoryBranchStateCache()));
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
				
		boolean success = latch.await(2, TimeUnit.SECONDS);
		assert(success);
		
		branch.getDataset().end();
		branch.getDataset().begin();
		OntModel model2 = branch2.getModel();
		branch2.getDataset().end();
		//branch2.getDataset().begin();
		OntModel model3 = branch3.getModel();
		branch3.getDataset().end();
		//branch3.getDataset().begin();
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
		
		// now lets test unregistering an inservice:
		branch2.removeIncomingCommitMerger(branch2signaller); // should have no effect
		branch2.removeIncomingCommitMerger(merger2);
		latch = new CountDownLatch(1);
		branch.appendOutgoingCommitDistributer(new SyncForTestingService("Branch1OutSignaller", latch, repoModel));
		
		Resource testResource = model.createResource(repoURI1+"#artFinaly");
		model.add(testResource, RDFS.label, model.createTypedLiteral(-1));
		Commit commit = branch.commitChanges("FinalTestCommit");
		success = latch.await(2, TimeUnit.SECONDS);
		assert(branch2.getInQueue().size() == 1);
	}
	
	
}

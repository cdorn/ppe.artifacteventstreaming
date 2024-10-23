package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.branch.BranchRepository;
import at.jku.isse.artifacteventstreaming.branch.incoming.CompleteCommitMerger;
import at.jku.isse.artifacteventstreaming.branch.outgoing.DefaultDirectBranchCommitStreamer;
import at.jku.isse.artifacteventstreaming.branch.persistence.FilebasedDatasetRepository;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryDatasetLoader;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryStateKeeper;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SimpleService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestBranchRepository {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/branchRepoTest");
	
	
	//DatasetRepository dataLoader = new FilebasedDatasetRepository();
	
	
	
	@Test
	void testCreateRepo() throws Exception {
		CountDownLatch latch = new CountDownLatch(1);
		DatasetRepository dataLoader = new InMemoryDatasetLoader();
		ServiceFactoryRegistry factoryRegistry = new ServiceFactoryRegistry(); // not used for the first repo
		StateKeeperFactory stateFactory = InMemoryStateKeeper.getStateKeeperFactory();
		
		BranchRepository repo = new BranchRepository(repoURI, dataLoader, stateFactory , factoryRegistry);
		OntModel repoModel = repo.getRepositoryModel();
		factoryRegistry.register(DefaultDirectBranchCommitStreamer.SERVICE_TYPE_URI, new DefaultDirectBranchCommitStreamer.DefaultServiceFactory(repo));
		BranchImpl branchSource = (BranchImpl) new BranchBuilder(repoURI, repo.getRepositoryDataset())
				.setBranchLocalName("source")
				.build();
		repo.registerBranch(branchSource); // not really necessary here
		branchSource.startCommitHandlers(null);
		BranchImpl branchDestination = (BranchImpl) new BranchBuilder(repoURI, repo.getRepositoryDataset())
				.setBranchLocalName("destination")
				.addBranchInternalCommitHandler(new SyncForTestingService("BranchDestinationSignaller", latch, repoModel))
				.build();
		branchSource.appendOutgoingCrossBranchCommitHandler(new DefaultDirectBranchCommitStreamer(branchSource, branchDestination));
		branchDestination.appendIncomingCommitHandler(new CompleteCommitMerger(branchDestination));
		repo.registerBranch(branchDestination);
		branchDestination.startCommitHandlers(null);
		
		
		OntModel model = branchSource.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branchSource.commitChanges("TestCommit");
		branchDestination.getDataset().end();
		latch.await();
		RDFDataMgr.write(System.out, repoModel, Lang.TURTLE) ;
			
//		////// ---- now we check if we can replicate that branch structure and print the destination 2 model content
//		
		// typically this is only done once per JVM, but here duplicated to simulate persistence at the level of datasets.
		latch = new CountDownLatch(1);
		ServiceFactoryRegistry factoryRegistry2 = new ServiceFactoryRegistry();
		BranchRepository repo2 = new BranchRepository(repoURI, dataLoader, stateFactory, factoryRegistry2);
		OntModel repoModel2 = repo2.getRepositoryModel();
		factoryRegistry2.register(DefaultDirectBranchCommitStreamer.SERVICE_TYPE_URI, new DefaultDirectBranchCommitStreamer.DefaultServiceFactory(repo2));
		factoryRegistry2.register(CompleteCommitMerger.getServiceTypeURI(), CompleteCommitMerger.getServiceFactory());
		factoryRegistry2.register(SyncForTestingService.SERVICE_TYPE_URI, SyncForTestingService.getServiceFactory("BranchCopySignaller", latch, repoModel2));
		
		Branch sourceBranch2 = repo2.getOrLoadBranch(URI.create(repoURI+"#source"));
		Branch destinationBranch2 = repo2.getOrLoadBranch(URI.create(repoURI+"#destination"));
		assertNotNull(destinationBranch2);
		assertNotNull(sourceBranch2);
		OntModel model2 = sourceBranch2.getModel();
		Resource testResource2 = model2.createResource(repoURI+"#art1");
		model2.add(testResource2, RDFS.label, model2.createTypedLiteral(1));
		Commit commit2 = sourceBranch2.commitChanges("TestCommit2");
		latch.await();
		destinationBranch2.getDataset().end();
		System.out.println("Initial Source");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		System.out.println("Initial Destination");
		RDFDataMgr.write(System.out, branchDestination.getModel(), Lang.TURTLE) ;
		System.out.println("New Source");
		RDFDataMgr.write(System.out, sourceBranch2.getModel(), Lang.TURTLE) ;
		System.out.println("New Destination");
		RDFDataMgr.write(System.out, destinationBranch2.getModel(), Lang.TURTLE) ;
		
		assert(destinationBranch2.getModel().containsAll(model)); // if we do the same commit, then there should the the same setup, and hence, the destination2 branch having the same content as the original sourceModel
	}
	
	

}

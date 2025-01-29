package at.jku.isse.artifacteventstreaming.api.manualtests;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.eventstore.dbclient.DeleteStreamOptions;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.PerBranchEventStore;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.RocksDBFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;

class TestEventPerformance {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repo-performance");
				
	
	private static RocksDBFactory cacheFactory;
	private static EventStoreFactory factory = new EventStoreFactory();
	private static BranchStateCache branchCache;
			
	@BeforeEach
	void removeStream() {
		try {
			cacheFactory = new RocksDBFactory("./branchStatusTestCache/");
			cacheFactory.resetCache();
			branchCache = cacheFactory.getCache();
			factory.getClient().getStreamMetadata(repoURI.toString()); //throws exception if doesn't exist, then we wont need to delete
			factory.getClient().deleteStream(repoURI.toString(), DeleteStreamOptions.get()).get();
		}catch (Exception e) {
			// ignore
		}		
	}
	
	@AfterEach
	void clearCache() {
		cacheFactory.closeCache();
	}
	
	
	@Test @Disabled
	void test10KEvents10CommitPersistence() throws Exception {	
		PerBranchEventStore client = factory.getEventStore(repoURI.toString());
		BranchStateUpdater stateKeeper = new StateKeeperImpl(repoURI, branchCache, client);
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(stateKeeper)				
				.build();		
		OntModel model = branch.getModel();
		stateKeeper.loadState();		
		branch.startCommitHandlers(null);
		
		long start = System.currentTimeMillis();
		for (int j = 0; j < 10; j++) {
			branch.getDataset().begin();
			Resource testResource = model.createResource(repoURI+"#art"+j);
			for (int i = 0; i<1000 ; i++) {
				model.add(testResource, RDFS.label, model.createTypedLiteral(i));	
			}				
			Commit commit = branch.commitChanges("TestCommit"+j);
		}
		System.out.println("Model1 size:"+model.size());
		long middle = System.currentTimeMillis();
		
		BranchStateUpdater stateKeeper2 = new StateKeeperImpl(repoURI, branchCache, client);
		Branch branch2 = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(stateKeeper2)				
				.build();		
		OntModel model2 = branch2.getModel();
		long initModel2Size = model2.size();
		stateKeeper2.loadState();
		branch.startCommitHandlers(null);
		System.out.println("Model2 size:"+model2.size());
		assertTrue(model2.size() == initModel2Size);
				
		// now we do manual application
		stateKeeper2.getHistory().stream().forEach(pastCommit -> {
			model2.add(pastCommit.getRemovedStatements().stream().map(Statement.class::cast).toList());
			model2.add(pastCommit.getAddedStatements().stream().map(Statement.class::cast).toList());
		});
		long end = System.currentTimeMillis();
		
		System.out.println("Model1 size:"+model.size());
		System.out.println("Model2 size:"+model2.size());		
		assert(model2.containsAll(model));
		assert(model.containsAll(model2));				
		System.out.println("Read only: "+(end-middle));
		System.out.println("End to End: "+(end-start));
	}
	
	@Test @Disabled
	void test1KCommitPersistence() throws Exception {	
		PerBranchEventStore client = factory.getEventStore(repoURI.toString());
		BranchStateUpdater stateKeeper = new StateKeeperImpl(repoURI, branchCache, client);
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(stateKeeper)				
				.build();				
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		
		long start = System.currentTimeMillis();
		Resource testResource = model.createResource(repoURI+"#art1");
		for (int i = 0; i<1000 ; i++) {
			branch.getDataset().begin();
			model.add(testResource, RDFS.label, model.createTypedLiteral(i));
			Commit commit = branch.commitChanges("TestCommit"+i);
		}						
		
		long midway = System.currentTimeMillis();
		System.out.println("Now replaying after: "+(midway-start));
		
		BranchStateUpdater stateKeeper2 = new StateKeeperImpl(repoURI, branchCache, client);
		Branch branch2 = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(stateKeeper2)				
				.build();		
		OntModel model2 = branch2.getModel();
		stateKeeper2.loadState();
		branch2.startCommitHandlers(null);
		// now we do manual application
		stateKeeper2.getHistory().stream().forEach(pastCommit -> {
			model2.add(pastCommit.getRemovedStatements().stream().map(Statement.class::cast).toList());
			model2.add(pastCommit.getAddedStatements().stream().map(Statement.class::cast).toList());
		});
		long end = System.currentTimeMillis();
		assert(model2.containsAll(model));
		assert(model.containsAll(model2));
						

		System.out.println("Read only: "+(end-midway));
		System.out.println("End to End: "+(end-start));
	}

}

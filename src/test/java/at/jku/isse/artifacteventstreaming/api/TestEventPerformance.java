package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.eventstore.dbclient.DeleteStreamOptions;
import com.eventstore.dbclient.EventStoreDBClient;
import at.jku.isse.artifacteventstreaming.rdf.BranchBuilder;
import at.jku.isse.artifacteventstreaming.rdf.persistence.DBBasedStateKeeper;
import at.jku.isse.artifacteventstreaming.rdf.persistence.EHCacheFactory;
import at.jku.isse.artifacteventstreaming.rdf.persistence.EventStoreFactory;

class TestEventPerformance {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repo-performance");
				
	
	private static EventStoreDBClient client = new EventStoreFactory().getClient();
			
	@BeforeAll
	static void removeStream() {
		try {
			client.getStreamMetadata(repoURI.toString()); //throws exception if doesn't exist, then we wont need to delete
			client.deleteStream(repoURI.toString(), DeleteStreamOptions.get()).get();
		}catch (Exception e) {
			// ignore
		}		
	}
	
	
	@Test @Disabled
	void test10KEvents10CommitPersistence() throws Exception {	
		StateKeeper stateKeeper = new DBBasedStateKeeper(repoURI, new EHCacheFactory().getCache(), client);
		Branch branch = new BranchBuilder(repoURI)
				.setStateKeeper(stateKeeper)				
				.build();		
		OntModel model = branch.getModel();
		stateKeeper.loadState(model);		
		
		long start = System.currentTimeMillis();
		for (int j = 0; j < 10; j++) {
			Resource testResource = model.createResource(repoURI+"#art"+j);
			for (int i = 0; i<1000 ; i++) {
				model.add(testResource, RDFS.label, model.createTypedLiteral(i));	
			}				
			Commit commit = branch.commitChanges("TestCommit"+j);
		}
		System.out.println("Model1 size:"+model.size());
		long middle = System.currentTimeMillis();
		
		StateKeeper stateKeeper2 = new DBBasedStateKeeper(repoURI, new EHCacheFactory().getCache(), new EventStoreFactory().getClient());
		Branch branch2 = new BranchBuilder(repoURI)
				.setStateKeeper(stateKeeper2)				
				.build();		
		OntModel model2 = branch2.getModel();
		long initModel2Size = model2.size();
		stateKeeper2.loadState(model2);
		System.out.println("Model2 size:"+model2.size());
		assertTrue(model2.size() == initModel2Size);
		
		// now we do manual application
		stateKeeper2.getHistory().stream().forEach(pastCommit -> {
			model2.add(pastCommit.getRemovedStatements());
			model2.add(pastCommit.getAddedStatements());
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
		StateKeeper stateKeeper = new DBBasedStateKeeper(repoURI, new EHCacheFactory().getCache(), client);
		Branch branch = new BranchBuilder(repoURI)
				.setStateKeeper(stateKeeper)				
				.build();				
		OntModel model = branch.getModel();
		
		long start = System.currentTimeMillis();
		Resource testResource = model.createResource(repoURI+"#art1");
		for (int i = 0; i<1000 ; i++) {
			model.add(testResource, RDFS.label, model.createTypedLiteral(i));
			Commit commit = branch.commitChanges("TestCommit"+i);
		}						
		
		long midway = System.currentTimeMillis();
		System.out.println("Now replaying after: "+(midway-start));
		
		StateKeeper stateKeeper2 = new DBBasedStateKeeper(repoURI, new EHCacheFactory().getCache(), new EventStoreFactory().getClient());
		Branch branch2 = new BranchBuilder(repoURI)
				.setStateKeeper(stateKeeper2)				
				.build();		
		OntModel model2 = branch2.getModel();
		stateKeeper2.loadState(model2);
		// now we do manual application
		stateKeeper2.getHistory().stream().forEach(pastCommit -> {
			model2.add(pastCommit.getRemovedStatements());
			model2.add(pastCommit.getAddedStatements());
		});
		long end = System.currentTimeMillis();
		assert(model2.containsAll(model));
		assert(model.containsAll(model2));
						

		System.out.println("Read only: "+(end-midway));
		System.out.println("End to End: "+(end-start));
	}

}

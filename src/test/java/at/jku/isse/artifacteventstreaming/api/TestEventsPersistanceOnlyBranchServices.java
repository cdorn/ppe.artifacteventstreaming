package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

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
import org.ehcache.Cache;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.eventstore.dbclient.DeleteStreamOptions;
import com.eventstore.dbclient.EventStoreDBClient;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;

import at.jku.isse.artifacteventstreaming.rdf.BranchBuilder;
import at.jku.isse.artifacteventstreaming.rdf.CompleteCommitMerger;
import at.jku.isse.artifacteventstreaming.rdf.DBBasedStateKeeper;
import at.jku.isse.artifacteventstreaming.rdf.EHCacheFactory;
import at.jku.isse.artifacteventstreaming.rdf.EventStoreFactory;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SimpleService;

class TestEventsPersistanceOnlyBranchServices {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repo3");
				
	
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
	
	
	@Test
	void testSimpleCommitPersistence() throws URISyntaxException, IOException {	
		var stateKeeper = new DBBasedStateKeeper(repoURI, new EHCacheFactory().getCache(), client);
		Branch branch = new BranchBuilder(repoURI)
				.setStateKeeper(stateKeeper)				
				.build();		
		OntModel model = branch.getModel();
		stateKeeper.loadState(model);
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(1, commit.getAddedStatements().size());
		
		int lastLabel = testResource.getProperty(RDFS.label).getInt();
		assertEquals(1, lastLabel);
	}
	
	@Test
	void testReadAndApplyCommits() throws StreamReadException, DatabindException, IOException {
		var stateKeeper = new DBBasedStateKeeper(repoURI, new EHCacheFactory().getCache(), new EventStoreFactory().getClient());
		Branch branch = new BranchBuilder(repoURI)
				.setStateKeeper(stateKeeper)				
				.build();		
		OntModel model = branch.getModel();
		stateKeeper.loadState(model);
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		
		var stateKeeper2 = new DBBasedStateKeeper(repoURI, new EHCacheFactory().getCache(), new EventStoreFactory().getClient());
		Branch branch2 = new BranchBuilder(repoURI)
				.setStateKeeper(stateKeeper2)				
				.build();		
		OntModel model2 = branch2.getModel();
		stateKeeper2.loadState(model2);
		assert(stateKeeper2.getHistory().size() > 0);
	}
	
	
	@Test
	void testReplayViaEvents() throws URISyntaxException, IOException {	
		Branch branch = new BranchBuilder(repoURI)
				.setStateKeeper(new DBBasedStateKeeper(repoURI, new EHCacheFactory().getCache(), new EventStoreFactory().getClient()))				
				.build();
		OntModel model = branch.getModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit1");
		
		model.add(testResource, RDFS.label, model.createTypedLiteral(2));
		commit = branch.commitChanges("TestCommit2");
		
		model.add(testResource, RDFS.label, model.createTypedLiteral(3));
		commit = branch.commitChanges("TestCommit3");
		
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(1, commit.getAddedStatements().size());				
		
		var stateKeeper2 = new DBBasedStateKeeper(repoURI, new EHCacheFactory().getCache(), new EventStoreFactory().getClient());
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
		
		assert(model2.containsAll(model));
		assert(model.containsAll(model2));
		assert(stateKeeper2.getHistory().size() > 2);
	}
}

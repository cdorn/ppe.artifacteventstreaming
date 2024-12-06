package at.jku.isse.artifacteventstreaming.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.List;
import java.util.Set;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.eventstore.dbclient.DeleteStreamOptions;
import com.fasterxml.jackson.databind.json.JsonMapper;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.outgoing.DefaultDirectBranchCommitStreamer;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory.EventStoreImpl;
import at.jku.isse.artifacteventstreaming.branch.persistence.RocksDBFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonDeserializer;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonSerializer;

class TestEventsPersistanceOnlyBranchServices {

	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repo3");
				
	private static RocksDBFactory cacheFactory;
	private static EventStoreFactory factory = new EventStoreFactory();
	private static BranchStateCache branchCache;
			
	@BeforeEach
	void removeStream() {
		try {
			cacheFactory = new RocksDBFactory("./branchStatusTestCache/");
			cacheFactory.resetCache();
		}catch (Exception e) {
			e.printStackTrace();
		}
		branchCache = cacheFactory.getCache();
		try {
			factory.getClient().getStreamMetadata(repoURI.toString()); //throws exception if doesn't exist, then we wont need to delete
			factory.getClient().deleteStream(repoURI.toString(), DeleteStreamOptions.get()).get();
		}catch (Exception e) {
			e.printStackTrace();
		} try {	
			factory.getClient().getStreamMetadata(repoURI.toString()+EventStoreImpl.INCOMING_COMMITS_STREAM_POSTFIX); //throws exception if doesn't exist, then we wont need to delete
			factory.getClient().deleteStream(repoURI.toString()+EventStoreImpl.INCOMING_COMMITS_STREAM_POSTFIX, DeleteStreamOptions.get()).get();
		}catch (Exception e) {
			e.printStackTrace();
		}		
	}
	
	@AfterEach
	void clearCache() {
		cacheFactory.closeCache();
	}
	
	
	@Test
	void testSerializeStatements() throws Exception{
		JsonMapper jsonMapper = new JsonMapper();
		StatementJsonSerializer.registerSerializationModule(jsonMapper);	
		StatementJsonDeserializer.registerDeserializationModule(jsonMapper);
		OntModel model = OntModelFactory.createModel();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		
		Commit commit = new StatementCommitImpl("", "", "", 0);
		commit.appendAddedStatements(Set.of(model.createStatement(testResource, RDFS.seeAlso, testResource)));
		
		String json = jsonMapper.writeValueAsString(commit);
		StatementCommitImpl commit2 = jsonMapper.readValue(json, StatementCommitImpl.class);
		assertEquals(commit2, commit);
		assertEquals(commit2.getAddedStatements().get(0), commit.getAddedStatements().get(0));
	}
	
	@Test
	void testSimpleCommitPersistence() throws Exception {	
		BranchStateUpdater stateKeeper = new StateKeeperImpl(repoURI, branchCache, factory.getEventStore(repoURI.toString()));
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(stateKeeper)				
				.build();		
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		stateKeeper.loadState();
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(1, commit.getAddedStatements().size());
		
		int lastLabel = testResource.getProperty(RDFS.label).getInt();
		assertEquals(1, lastLabel);
	}
	
	@Test
	void testReadAndApplyCommits() throws Exception {
		BranchStateUpdater stateKeeper = new StateKeeperImpl(repoURI, branchCache, factory.getEventStore(repoURI.toString()));
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(stateKeeper)				
				.build();		
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		stateKeeper.loadState();
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit");
		
		BranchStateUpdater stateKeeper2 = new StateKeeperImpl(repoURI, branchCache, factory.getEventStore(repoURI.toString()));
		Branch branch2 = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(stateKeeper2)				
				.build();		
		OntModel model2 = branch2.getModel();
		stateKeeper2.loadState();
		assert(stateKeeper2.getHistory().size() > 0);
	}
	
	
	@Test
	void testReplayViaEvents() throws Exception {	
		Branch branch = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(new StateKeeperImpl(repoURI, branchCache, factory.getEventStore(repoURI.toString())))				
				.build();
		branch.startCommitHandlers(null);
		OntModel model = branch.getModel();
		branch.getDataset().begin();
		Resource testResource = model.createResource(repoURI+"#art1");
		model.add(testResource, RDFS.label, model.createTypedLiteral(1));
		Commit commit = branch.commitChanges("TestCommit1");
		
		branch.getDataset().begin();
		model.add(testResource, RDFS.label, model.createTypedLiteral(2));
		commit = branch.commitChanges("TestCommit2");
		
		branch.getDataset().begin();
		model.add(testResource, RDFS.label, model.createTypedLiteral(3));
		commit = branch.commitChanges("TestCommit3");
		
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		assertEquals(1, commit.getAddedStatements().size());				
		
		BranchStateUpdater stateKeeper2 = new StateKeeperImpl(repoURI, branchCache, factory.getEventStore(repoURI.toString()));
		Branch branch2 = new BranchBuilder(repoURI, DatasetFactory.createTxnMem())
				.setStateKeeper(stateKeeper2)				
				.build();		
		OntModel model2 = branch2.getModel();
		stateKeeper2.loadState();
		branch2.getDataset().begin();
		// now we do manual application of history onto model 
		stateKeeper2.getHistory().stream().forEach(pastCommit -> {
			System.out.println("Adding commit: "+pastCommit.getCommitMessage());
			model2.add(pastCommit.getRemovedStatements());
			model2.add(pastCommit.getAddedStatements());
		});
		branch2.getDataset().commit();
		branch2.getDataset().end();
		System.out.println("Model2 contains now: "+model2.size());
		RDFDataMgr.write(System.out, model2, Lang.TURTLE) ;
		assert(model2.containsAll(model));
		assert(model.containsAll(model2));
		assert(stateKeeper2.getHistory().size() > 2);
	}
	
	@Test
	void testEventStoreBackedReadCommits() throws Exception {
		PerBranchEventStore eventStore = factory.getEventStore(repoURI.toString());
		var c1 = new StatementCommitImpl("b1", "msg1", null, 0);
		var c2 = new StatementCommitImpl("b1", "msg2", c1.getCommitId(), 0);
		var c3 = new StatementCommitImpl("b1", "msg3", c2.getCommitId(), 0);
		var cde1 = new CommitDeliveryEvent(c1.getCommitId(), c1, "b1", "b2");
		var cde2 = new CommitDeliveryEvent(c2.getCommitId(), c2, "b1", "b2");
		var cde3 = new CommitDeliveryEvent(c3.getCommitId(), c3, "b1", "b2");
		eventStore.appendCommitDelivery(cde1);
		eventStore.appendCommitDelivery(cde2);
		eventStore.appendCommitDelivery(cde3);
		
		var result = eventStore.loadAllIncomingCommitsForBranchFromCommitIdOnward(null);
		assert(result.containsAll(List.of(c1, c2, c3)));
		
		result = eventStore.loadAllIncomingCommitsForBranchFromCommitIdOnward(c2.getCommitId());
		assert(result.containsAll(List.of(c3)));
		
		result = eventStore.loadAllIncomingCommitsForBranchFromCommitIdOnward(c3.getCommitId());
		assert(result.isEmpty());
		
		
	}
}

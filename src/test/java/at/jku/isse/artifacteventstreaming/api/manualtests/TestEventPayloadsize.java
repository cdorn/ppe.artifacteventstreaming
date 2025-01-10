package at.jku.isse.artifacteventstreaming.api.manualtests;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.RocksDBFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;

class TestEventPayloadsize {

	public static URI NS = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repopayload");
								
	@BeforeEach
	void removeStream() {
		try {			
			EventStoreFactory factory = new EventStoreFactory();
			factory.getClient().getStreamMetadata(NS.toString()); //throws exception if doesn't exist, then we wont need to delete
			factory.getClient().deleteStream(NS.toString(), DeleteStreamOptions.get()).get();
		} catch (Exception e) {
			// ignore
		}		
	}		
	
	
	@Test 
	void test10KStmtCommitSplitting() throws Exception {	
		
		OntModel m = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF);
		var listener = new StatementAggregator();
		listener.registerWithModel(m);
		
		var schemaUtils = new PropertyCardinalityTypes(m); 
		var props = new LinkedList<OntRelationalProperty>();
		var types = new LinkedList<OntClass>();
		
		// create large schema, 100 classes with 100 properties each
		for (int i = 0; i < 100; i++) {
			var type = m.createOntClass(NS+"type"+i);
			types.add(type);
			for (int j = 0; j < 100; j++) {
				props.add(schemaUtils.getListType().addObjectListProperty(type, NS+"prop"+i+"-"+j, type));				
			}			
		}
		var commit = new StatementCommitImpl(NS.toString(), "TestCommit", null, 0, listener.retrieveAddedStatements(), listener.retrieveRemovedStatements());
		EventStoreFactory factory = new EventStoreFactory();
		var store = factory.getEventStore(NS.toString());
		store.appendCommit(commit);
		
		List<Commit> allCommits = store.loadAllCommits();
		assertTrue(allCommits.size() > 0);
	}
	
	
}

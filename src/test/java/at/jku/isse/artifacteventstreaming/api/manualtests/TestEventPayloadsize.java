package at.jku.isse.artifacteventstreaming.api.manualtests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.eventstore.dbclient.DeleteStreamOptions;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;

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
	
	
	@Test // @Disabled
	void test190KStmtCommitSplitting() throws Exception {	
		
		OntModel m = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF);
		var listener = new StatementAggregator();
		listener.registerWithModel(m);
		
		var metaModel = MetaModelOntology.buildInMemoryOntology(); 		
		var schemaUtils = new MetaModelSchemaTypes(m, metaModel); 
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
		System.out.println("Added Statements: "+commit.getAdditionCount());
		EventStoreFactory factory = new EventStoreFactory();
		var store = factory.getEventStore(NS.toString());
		store.appendCommit(commit);
		
		List<Commit> allCommits = store.loadAllCommits();
		assertEquals(1, allCommits.size());
		var fetchedCommit = allCommits.get(0);
		System.out.println("Retrieved Statements: "+fetchedCommit.getAdditionCount());
		assertEquals(commit.getAdditionCount(), fetchedCommit.getAdditionCount());
	}
	
	
}

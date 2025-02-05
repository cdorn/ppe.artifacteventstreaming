package at.jku.isse.artifacteventstreaming.replay;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.junit.jupiter.api.BeforeEach;

import com.eventstore.dbclient.DeleteStreamOptions;

import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.rule.MockSchema;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResource;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;

public class TestPersistedReplay extends TestContainmentReplay{
		
	EventStoreFactory factory = new EventStoreFactory();
	
	@Override
	@BeforeEach
	void setupListener() throws Exception {
		removeStream(branchURI);		
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_RDFS_INF );
		var metaModel = MetaModelOntology.buildInMemoryOntology(); 
		new RuleSchemaFactory(metaModel); // add rule schema to meta model		
		schemaUtil = new MetaModelSchemaTypes(m, metaModel);
		schema = new MockSchema(m, schemaUtil);
		aggr = new StatementAggregator();
		aggr.registerWithModel(m);
		
		EventStoreBackedHistoryRepository historyRepo = new EventStoreBackedHistoryRepository(factory.getClient(), factory.getProjectionClient(), factory.getJsonMapper());
		collector = new ReplayEntryCollectorFromHistory(historyRepo, branchURI );
		CommitContainmentAugmenter augmenter = new CommitContainmentAugmenter(branchURI, m, schemaUtil);

		issue1 = schema.createIssue("Issue1");
		var seq = schemaUtil.getListType().getOrCreateSequenceFor(issue1, schema.getLabelProperty());
		var map = MapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType());
		
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(1L));
		issue1.removeAll(schema.getStateProperty());
		issue1.addProperty(schema.getStateProperty(), m.createTypedLiteral("InProgress"));
		seq.add(1, "First");
		map.put("First", m.createTypedLiteral(1));
		
		var commit1 = new StatementCommitImpl(branchURI , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		augmenter.handleCommit(commit1);
		factory.getEventStore(branchURI).appendCommit(commit1);
		
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(2L));
		issue1.remove(schema.getPriorityProperty(), m.createTypedLiteral(1L));
		issue1.removeAll(schema.getStateProperty());
		issue1.addProperty(schema.getStateProperty(), m.createTypedLiteral("Resolved"));
		seq.add(1, "NewFirst");
		map.put("Second", m.createTypedLiteral(2));
		var commit2 = new StatementCommitImpl(branchURI , "TestCommit2", "", 1, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		augmenter.handleCommit(commit2);
		factory.getEventStore(branchURI).appendCommit(commit2);
		
		
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(3L));
		issue1.remove(schema.getPriorityProperty(), m.createTypedLiteral(2L));
		issue1.removeAll(schema.getStateProperty());
		issue1.addProperty(schema.getStateProperty(), m.createTypedLiteral("Closed"));
		seq.add(1, "VeryNewFirst");
		map.put("First", m.createTypedLiteral(3));
		var commit3 = new StatementCommitImpl(branchURI , "TestCommit3", "", 2, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		augmenter.handleCommit(commit3);
		factory.getEventStore(branchURI).appendCommit(commit3);
		
		Thread.sleep(1000); // we need to let the projections run first
	}
	
	void removeStream(String streamName) {
		try {						
			factory.getClient().getStreamMetadata(streamName); //throws exception if doesn't exist, then we wont need to delete
			factory.getClient().deleteStream(streamName, DeleteStreamOptions.get()).get();
		} catch (Exception e) {
			// ignore
		}		
	}	
	
	
	
}

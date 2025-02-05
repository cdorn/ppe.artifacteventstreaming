package at.jku.isse.artifacteventstreaming.replay;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.outgoing.CommitToHistoryHandler;
import at.jku.isse.artifacteventstreaming.rule.MockSchema;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResource;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.passiveprocessengine.rdfwrapper.ResourceMismatchException;

@ExtendWith(MockitoExtension.class) 
class TestContainmentReplay {

	public static URI baseURI = URI.create("http://at.jku.isse.artifacteventstreaming/test/replay#");
	String branchURI = baseURI.toString()+"testbranch";
	StatementAggregator aggr;
	OntModel m;
	MockSchema schema;
	OntObject issue1;
	ReplayEntryCollectorFromHistory collector;
	MetaModelSchemaTypes schemaUtil;
	
	@Mock Branch mockBranch;
	
	@BeforeEach
	void setupListener() throws Exception {
				
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_RDFS_INF );
		var metaModel = MetaModelOntology.buildInMemoryOntology(); 
		new RuleSchemaFactory(metaModel); // add rule schema to meta model		
		schemaUtil = new MetaModelSchemaTypes(m, metaModel);
		schema = new MockSchema(m, schemaUtil);
		aggr = new StatementAggregator();
		aggr.registerWithModel(m);
		
		InMemoryHistoryRepository historyRepo = new InMemoryHistoryRepository();
		collector = new ReplayEntryCollectorFromHistory(historyRepo, branchURI );
		
		
		when(mockBranch.getBranchName()).thenReturn("testBranch");
		when(mockBranch.getBranchResource()).thenReturn(m.createIndividual(baseURI+"mock"));
		var commit2history = new CommitToHistoryHandler(mockBranch, historyRepo, null);
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
		commit2history.handleCommit(commit1);
		
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(2L));
		issue1.remove(schema.getPriorityProperty(), m.createTypedLiteral(1L));
		issue1.removeAll(schema.getStateProperty());
		issue1.addProperty(schema.getStateProperty(), m.createTypedLiteral("Resolved"));
		seq.add(1, "NewFirst");
		map.put("Second", m.createTypedLiteral(2));
		var commit2 = new StatementCommitImpl(branchURI , "TestCommit2", "", 1, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		augmenter.handleCommit(commit2);
		commit2history.handleCommit(commit2);
		
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(3L));
		issue1.remove(schema.getPriorityProperty(), m.createTypedLiteral(2L));
		issue1.removeAll(schema.getStateProperty());
		issue1.addProperty(schema.getStateProperty(), m.createTypedLiteral("Closed"));
		seq.add(1, "VeryNewFirst");
		map.put("First", m.createTypedLiteral(3));
		var commit3 = new StatementCommitImpl(branchURI , "TestCommit3", "", 2, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		augmenter.handleCommit(commit3);
		commit2history.handleCommit(commit3);
	}
	
	

	@Test
	void testEmptyRevert() {
		var prevSize = m.size();
		var map = new HashMap<Resource,Set<Property>>();
		ReplaySession session = new ReplaySession(collector, map, m);
		session.revert();
		assertEquals(prevSize, m.size());
		var fwd = session.playForwardOneTimestamp();
		assertTrue(fwd.isEmpty());
	}
			
	@Test
	void testRevertCommits() {
		var prevSize = m.size();
		var map = new HashMap<Resource,Set<Property>>();
		map.put(issue1, Set.of(schema.getPriorityProperty()));
		ReplaySession session = new ReplaySession(collector, map, m);
		session.revert();
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		assertTrue(prevSize > m.size());
		
		var fwd = session.playForwardOneTimestamp();
		assertEquals(1, fwd.size());
		assertEquals(1, issue1.getProperty(schema.getPriorityProperty()).getLiteral().getValue());
		
		fwd = session.playForwardOneTimestamp();
		assertEquals(2, fwd.size());
		assertEquals(2, issue1.getProperty(schema.getPriorityProperty()).getLiteral().getValue());
		
		fwd = session.playForwardOneTimestamp();
		assertEquals(2, fwd.size());
		assertEquals(3, issue1.getProperty(schema.getPriorityProperty()).getLiteral().getValue());
		
		fwd = session.playForwardOneTimestamp();
		assertTrue(fwd.isEmpty());
	}
	
	@Test
	void testRevertExpandScopeCommits() {
		var prevSize = m.size();
		var map = new HashMap<Resource,Set<Property>>();
		map.computeIfAbsent(issue1, k -> new HashSet<>()).add(schema.getPriorityProperty());
		ReplaySession session = new ReplaySession(collector, map, m);
		session.revert();
		assertTrue(prevSize > m.size());
		
		var fwd = session.playForwardOneTimestamp();
		assertEquals(1, fwd.size());
		assertEquals(1, issue1.getProperty(schema.getPriorityProperty()).getLiteral().getValue());
		
		var extMap = new HashMap<Resource,Set<Property>>();
		extMap.put(issue1, Set.of(schema.getStateProperty()));
		session.addToScope(extMap);
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		assertEquals("InProgress", issue1.getProperty(schema.getStateProperty()).getLiteral().getValue());
		
		fwd = session.playForwardOneTimestamp();
		assertEquals(4, fwd.size());
		assertEquals(2, issue1.getProperty(schema.getPriorityProperty()).getLiteral().getValue());
		assertEquals("Resolved", issue1.getProperty(schema.getStateProperty()).getLiteral().getValue());
		
		fwd = session.playForwardOneTimestamp();
		assertEquals(4, fwd.size());
		assertEquals(3, issue1.getProperty(schema.getPriorityProperty()).getLiteral().getValue());
		assertEquals("Closed", issue1.getProperty(schema.getStateProperty()).getLiteral().getValue());
		
		fwd = session.playForwardOneTimestamp();
		assertTrue(fwd.isEmpty());
	}
	
	@Test 
	void testRevertListContainmentCommits() {
		var scope = new HashMap<Resource,Set<Property>>();
		scope.computeIfAbsent(issue1, k -> new HashSet<>()).add(schema.getLabelProperty().asProperty());
		ReplaySession session = new ReplaySession(collector, scope, m);
		session.revert();
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		var fwd = session.playForwardOneTimestamp();
		assertEquals(5, fwd.size()); //type info and list linking
		var seq = schemaUtil.getListType().getOrCreateSequenceFor(issue1, schema.getLabelProperty());
		assertEquals("First", seq.getString(1));
		
		fwd = session.playForwardOneTimestamp();
		assertEquals(3, fwd.size()); // insert and move once
		assertEquals("NewFirst", seq.getString(1));
		
		fwd = session.playForwardOneTimestamp();
		assertEquals(5, fwd.size()); // insert and move twice
		assertEquals("VeryNewFirst", seq.getString(1));
		
	}

	@Test
	void testRevertMapContainmentCommits() throws ResourceMismatchException {
		var scope = new HashMap<Resource,Set<Property>>();
		scope.computeIfAbsent(issue1, k -> new HashSet<>()).add(schema.getKeyValueProperty().asProperty());
		ReplaySession session = new ReplaySession(collector, scope, m);
		session.revert();
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		var fwd = session.playForwardOneTimestamp();
		assertEquals(5, fwd.size()); //type info and entry linking
		var map = MapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType());
		assertEquals(1, map.get("First").asLiteral().getInt());
		
		fwd = session.playForwardOneTimestamp();
		assertEquals(5, fwd.size()); //type info and entry linking
		map = MapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType()); // need to reload map
		assertEquals(1, map.get("First").asLiteral().getInt());
		assertEquals(2, map.get("Second").asLiteral().getInt());
		
		fwd = session.playForwardOneTimestamp();
		assertEquals(2, fwd.size()); // replacing 
		map = MapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType()); // need to reload map
		assertEquals(3, map.get("First").asLiteral().getInt());
		assertEquals(2, map.get("Second").asLiteral().getInt());
	}
	
	
}

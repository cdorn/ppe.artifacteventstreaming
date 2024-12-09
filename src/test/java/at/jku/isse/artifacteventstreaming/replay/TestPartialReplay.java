package at.jku.isse.artifacteventstreaming.replay;

import static org.junit.jupiter.api.Assertions.*;

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

import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.rule.MockSchema;

class TestPartialReplay {

	public static URI baseURI = URI.create("http://at.jku.isse.artifacteventstreaming/test/replay#");
	StatementAggregator aggr;
	OntModel m;
	MockSchema schema;
	OntObject issue1;
	ReplayEntryCollectorFromCommits collector;
	
	@BeforeEach
	void setupListener() {
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_RDFS_INF );
		schema = new MockSchema(m);
		aggr = new StatementAggregator();
		aggr.registerWithModel(m);
		collector = new ReplayEntryCollectorFromCommits();
		issue1 = schema.createIssue("Issue1");
		
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(1L));
		issue1.removeAll(schema.getStateProperty());
		issue1.addProperty(schema.getStateProperty(), m.createTypedLiteral("InProgress"));
		var commit1 = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		collector.addCommit(commit1);
		
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(2L));
		issue1.remove(schema.getPriorityProperty(), m.createTypedLiteral(1L));
		issue1.removeAll(schema.getStateProperty());
		issue1.addProperty(schema.getStateProperty(), m.createTypedLiteral("Resolved"));
		var commit2 = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit2", "", 1, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		collector.addCommit(commit2);
		
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(3L));
		issue1.remove(schema.getPriorityProperty(), m.createTypedLiteral(2L));
		issue1.removeAll(schema.getStateProperty());
		issue1.addProperty(schema.getStateProperty(), m.createTypedLiteral("Closed"));
		var commit3 = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit3", "", 2, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		collector.addCommit(commit3);
		
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

}

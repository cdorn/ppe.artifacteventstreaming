package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.jena.graph.Graph;
import org.apache.jena.ontapi.UnionGraph;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.ModelChangedListener;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;

class TestRuleTriggering extends TestRuleEvaluation {

	StatementAggregator aggr;
	RuleTriggerObserver observer;
	
	@Override
	@BeforeEach
	void setup() {
		super.setup();
		aggr = new StatementAggregator();
			 
		observer = new RuleTriggerObserver("RuleTriggerObserver1", m, factory, repo); // we reuse the main model here as there is no branch involved anyway
		register(m, aggr);
		//m.register(aggr);		// when using inference, this does not register the listener at the right graph:	https://github.com/apache/jena/issues/2868
	}
	
	private void register(OntModel model, ModelChangedListener listener) {
		var given = model.getGraph();
		if (given instanceof InfGraph infG) {
            Graph raw = infG.getRawGraph();
            if (raw instanceof UnionGraph ugraph) {
            	ugraph.getEventManager().register(((ModelCom)model).adapt(listener));
            }            
        }
        if (given instanceof UnionGraph ugraph) {
        	ugraph.getEventManager().register(((ModelCom)model).adapt(listener));
        }
	}
	
	
	@Test
	void testCreateRuleWithPreexistingInstance() throws RuleException {
		var def = getBasicArtSubTypeWithSubRefOnLabelRule(1);
		var defURI = def.getRuleDefinition().getURI();
		var commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);				
		// assert that rule evaluation is available and has been evaluated
		var affected = repo.getRulesAffectedByChange(inst1, subProp.asProperty());
		assertEquals(1, affected.size());
		var ruleEval = affected.iterator().next();
		assertEquals(Boolean.FALSE, ruleEval.getEvaluationResult());
		
		aggr.retrieveAddedStatements(); // to clear changes
		aggr.retrieveRemovedStatements();
		
		// now when we remove that rule externally, see if it is detected
		ruleEval.delete(); // we just delete one, the other local one remains and needs to be automatically deleted, checked below
		def.delete(); 		
		var commit2 = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit2);
		// the rule should now be gone
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		var deletedDef = repo.findRuleDefinitionForURI(defURI);
		assertNull(deletedDef);	
		assertTrue(getAllScopes().stream().allMatch(scope -> scope.getProperty(factory.getUsedInRuleProperty().asProperty())==null));
	}

	// test: if inst is removed whether all scopes are gone
	@Test
	void testRemoveScopeUponDeletion() throws RuleException {
		var def = getBasicArtSubTypeWithSubRefOnLabelRule(1);
		var defURI = def.getRuleDefinition().getURI();
		var commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);				
		// assert that rule evaluation is available and has been evaluated
		var affected = repo.getRulesAffectedByChange(inst1, subProp.asProperty());
		assertEquals(1, affected.size());
		var ruleEval = affected.iterator().next();
		assertEquals(Boolean.FALSE, ruleEval.getEvaluationResult());
		
		aggr.retrieveAddedStatements(); // to clear changes
		aggr.retrieveRemovedStatements();
		
		inst1.removeProperties();
		inst2.removeProperties();
		var commit2 = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit2);
		// the rule should now be gone
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;				
		assertEquals(0, getAllScopes().size());			
	}
}

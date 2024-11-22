package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.*;

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
		m.register(aggr);
		observer = new RuleTriggerObserver("RuleTriggerObserver1", m, factory, repo); // we reuse the main model here as there is no branch involved anyway
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
		
		aggr.retrieveAddedStatements(); // to reset changes
		aggr.retrieveRemovedStatements();
		
		// now when we remove that rule externally, see if it is detected
		ruleEval.delete();
		def.delete(); 		
		var commit2 = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit2);
		// the rule should now be gone
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		var deletedDef = repo.findRuleDefinitionForURI(defURI);
		assertNull(deletedDef);
		//RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		//TODO: better check if really remove type and remove eval are called, and if the order matters
	}

	// test: if inst is removed whether all scopes are gone
	
}

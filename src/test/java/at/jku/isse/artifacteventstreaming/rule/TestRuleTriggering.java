package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;

class TestRuleTriggering extends TestRuleEvaluation {

	StatementAggregator aggr;
	AbstractRuleTriggerObserver observer;
	
	@Override
	@BeforeEach
	void setup() {
		super.setup();
		aggr = new StatementAggregator();
			 
		observer = new ActiveRuleTriggerObserver("RuleTriggerObserver1", m, factory, repo); // we reuse the main model here as there is no branch involved anyway
		aggr.registerWithModel(m);
		}
	
	
	@Test
	void testCreateRuleWithPreexistingInstance() throws RuleException {
		var def = getBasicArtSubTypeWithSubRefOnLabelRule(1);
		var defURI = def.getRuleDefinition().getURI();
		var commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
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
		var commit2 = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit2);
		// the rule should now be gone
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		var deletedDef = repo.findRuleDefinitionForURI(defURI);
		assertNull(deletedDef);	
		assertTrue(inspector.getAllScopes().stream().allMatch(scope -> scope.getProperty(factory.getUsedInRuleProperty().asProperty())==null));
	}

	@Test
	void testCreateRuleWithInstanceCreatedLaterAndRemoved() throws RuleException {
		var def = getBasicArtSubTypeWithSubRefOnLabelRule(1);
		var defURI = def.getRuleDefinition().getURI();
		aggr.retrieveAddedStatements(); // to clear changes
		aggr.retrieveRemovedStatements();
		
		var inst3 = artSubType.createIndividual(baseURI+"inst3");
		
		var commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);				
		// assert that rule evaluation is available and has been evaluated
		var affected = repo.getRulesAffectedByChange(inst3, subProp.asProperty());
		assertEquals(1, affected.size());
		var ruleEval = affected.iterator().next();
		assertEquals(Boolean.FALSE, ruleEval.getEvaluationResult());
		
		// remove subart type;
		inst3.remove(RDF.type, artSubType);
		commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);				
		// assert that rule evaluation is no longer available 
		affected = repo.getRulesAffectedByChange(inst3, subProp.asProperty());
		assertEquals(0, affected.size());
	}
	
	@Test
	void testInstanceRetyping() throws RuleException {
		var def = getBasicArtTypeWithRefOnLabelRule(1);		
		var commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);
		
		var affected = repo.getRulesAffectedByChange(inst2, refProp.asProperty());
		assertEquals(1, affected.size());
		var ruleEval = affected.iterator().next();
		assertEquals(Boolean.FALSE, ruleEval.getEvaluationResult());
		aggr.retrieveAddedStatements(); // to clear changes
		aggr.retrieveRemovedStatements();
		
		// remove subart type;
		inst2.remove(RDF.type, artSubType);
		inst2.addProperty(RDF.type, artType);
		//inst2.classes(true).forEach(clazz -> System.out.println(clazz));
				
		commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit2", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);				
		// assert that rule evaluation is available and has been evaluated
		var affected2 = repo.getRulesAffectedByChange(inst2, refProp.asProperty());
		
		var scopes = inspector.getScopes(inst2);
		//scopes.stream().forEach(scope -> printScope(scope));
		assertEquals(2, scopes.size()); // one for each rule
		assertTrue(scopes.stream().allMatch(scope -> inspector.getEvalCountFromScope(scope) == 1));				
		assertEquals(1, affected2.size());
		assertEquals(affected, affected2); // same eval objects		
		
	}
	
	// test: if inst is removed whether all scopes are gone
	@Test
	void testRemoveScopeUponDeletion() throws RuleException {
		var def = getBasicArtSubTypeWithSubRefOnLabelRule(1);
		var defURI = def.getRuleDefinition().getURI();
		var commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
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
		var commit2 = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit2);
		// the rule should now be gone
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;				
		assertEquals(0, inspector.getAllScopes().size());			
	}
	
	@Test
	void testExternalRemoveEval() throws RuleException {
		var def = getBasicArtSubTypeWithSubRefOnLabelRule(1);
		var defURI = def.getRuleDefinition().getURI();
		var commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
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
		var commit2 = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit2);
		// the rule should now be gone
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		affected = repo.getRulesAffectedByChange(inst1, subProp.asProperty());
		assertEquals(0, affected.size());
	}
	
	@Test
	void testRuleContextRetyping() throws RuleException {
		var def = getBasicArtTypeWithRefOnLabelRule(1);		
		var commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);
		
		var affected = repo.getRulesAffectedByChange(inst2, refProp.asProperty());
		assertEquals(1, affected.size());
		var ruleEval = affected.iterator().next();
		assertEquals(Boolean.FALSE, ruleEval.getEvaluationResult());
		aggr.retrieveAddedStatements(); // to clear changes
		aggr.retrieveRemovedStatements();
		
		// change context type type;
		def.setRuleExpression("self->isDefined()");
							
		commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit2", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);				
		// assert that rule evaluation is available and has been evaluated
		var affected2 = repo.getRulesAffectedByChange(inst2, refProp.asProperty());
				
		assertNotEquals(affected, affected2); // not the same eval objects		
		
	}
	
	@Test @Disabled // as we dont support that usecase yet
	void testRuleUsedPropertyRemoval() throws RuleException {
		var def = getBasicArtTypeWithPriorityRule(1);		
		var commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "TestCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);
		
		var affected = repo.getRulesAffectedByChange(inst2, priorityProp.asProperty());
		assertEquals(1, affected.size());
		var ruleEval = affected.iterator().next();
		assertEquals(Boolean.FALSE, ruleEval.getEvaluationResult());
		aggr.retrieveAddedStatements(); // to clear changes
		aggr.retrieveRemovedStatements();
		
		schemaUtils.getSingleType().removeSingleProperty(artType, priorityProp);
		commit = new StatementCommitImpl(baseURI+"SomeBranchID"  , "SchemaChangeCommit", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
		observer.handleCommit(commit);				
		
		assertFalse(ruleEval.isEnabled());
	}
}

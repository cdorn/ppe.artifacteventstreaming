package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashSet;
import java.util.Set;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class TestRuleEvaluation extends TestRuleDefinitions {

	RuleRepository repo;
	RuleRepositoryInspector inspector;
	OntIndividual inst1;
	OntIndividual inst2;
	
	@Override
	@BeforeEach
	void setup() {
		super.setup();
		repo = new RuleRepository(factory);
		inspector = new RuleRepositoryInspector(factory);
		inst1 = artSubType.createIndividual(baseURI+"inst1");
		inst2 = artSubType.createIndividual(baseURI+"inst2");
	}
	
	
	@Test
	void testTypeRuleEval() throws RuleException {					
		var ruleDef = repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.asType(<"+ artSubType.getURI()+">).subref.size() > 1")
				.build();
		inst1.addProperty(subProp.asProperty(), inst2);
		
		System.out.println(ruleDef.getExpressionError());
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
								
		var rulesToEval = repo.getRulesToEvaluateUponRuleDefinitionActivation(ruleDef);				
		assertEquals(2, rulesToEval.size());
		rulesToEval.forEach(rule -> rule.evaluate());
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		assertTrue(rulesToEval.stream().allMatch(evalWrapper -> { 
			System.out.println(evalWrapper);
			return (evalWrapper.isConsistent() == false);
		}));
		
	}
	
	@Test
	void testNavigationInRule() throws RuleException {
		var ruleDef = repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.asType(<"+ artSubType.getURI()+">).subref->exists(art | art.label = 'someLabel')")
				.build();
		System.out.println(ruleDef.getExpressionError());
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		inst1.addProperty(subProp.asProperty(), inst2);
		var rulesToEval = repo.getRulesToEvaluateUponRuleDefinitionActivation(ruleDef);				
		assertEquals(2, rulesToEval.size());
		rulesToEval.forEach(rule -> rule.evaluate());
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		assertTrue(rulesToEval.stream().allMatch(evalWrapper -> { 
			System.out.println(evalWrapper);
			return (evalWrapper.isConsistent() == false);
		}));
	}
	
	/*	 
	 * rules has been evaluated, new art added, now check if that scope is listed 	
	 * */
	@Test
	void testScopeAdding() throws RuleException {
		var ruleDef = repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.asType(<"+ artSubType.getURI()+">).subref->exists(art | art.label = 'someLabel')")
				.build();
		System.out.println(ruleDef.getExpressionError());
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		System.out.println("All Scopes before addition:");
		inspector.getAllScopes().forEach(scope -> inspector.printScope(scope));					
		var evals = inspector.getEvalWrappersFromScopes(inst2, labelProp.getURI());
		assertEquals(0, evals.size());
		
		// now lets add this to property
		inst1.addProperty(subProp.asProperty(), inst2);
		rulesToEval = repo.getRulesAffectedByChange(inst1, subProp.asProperty());
		assertEquals(1, rulesToEval.size());
		evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();							
		evals = inspector.getEvalWrappersFromScopes(inst2, labelProp.getURI());
		assertEquals(1, evals.size());
		
		
		var ruleScopes = getScopesForRule(((RuleEvaluationWrapperResourceImpl) evalWrapper).getRuleEvalObj());
		System.out.println("Rule 1 Scopes after addition:");
		ruleScopes.forEach(scope -> inspector.printScope(scope));
		assertEquals(2, ruleScopes.size()); // 2 not 3 because rule does not point to Scope that is used to represent the context usage
		
		// now lets see which ones we would activate if we official add art2
		var rules = repo.getRulesAffectedByCreation(inst2);
		assertEquals(2, rules.size()); // 1 for the new instantiation, 1 for the use of inst2 in ruleEval1
	}
	
	/*	 
	 * rules has been evaluated, new art added, and removed again, is also the scope removed?
	 * */
	@Test
	void testScopeRemovingProperty() throws RuleException {
		testScopeAdding();
		inst1.remove(subProp.asProperty(), inst2);
		var rulesToEval = repo.getRulesAffectedByChange(inst1, subProp.asProperty());
		assertEquals(1, rulesToEval.size());
		
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		var result = evalWrapper.getEvaluationResult();
		assertEquals(Boolean.FALSE, result);
		assertEquals(inst1, evalWrapper.getContextInstance());
		
		var evalsOf2 = inspector.getEvalWrappersFromScopes(inst2, labelProp.getURI());
		assertEquals(0, evalsOf2.size());		
		var ruleScopes = getScopesForRule(((RuleEvaluationWrapperResourceImpl) evalWrapper).getRuleEvalObj());
		System.out.println("Rule 1 Scopes after addition:");
		ruleScopes.forEach(scope -> inspector.printScope(scope));
		assertEquals(1, ruleScopes.size()); // 1 not 2 because rule does not point to Scope that is used to represent the context usage
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}
		
	/*
	 * 2 rule instantiation on same art -> one context scope per context, that lists both rules 
	 * */
	@Test 
	void testTwoRuleScopeAdded() throws RuleException {
		getBasicArtSubTypeWithSubRefOnLabelRule(1);
		var ruleDef2 = getBasicArtSubTypeWithSubRefOnLabelRule(2);
		System.out.println(ruleDef2.getExpressionError());
		assertFalse(ruleDef2.hasExpressionError());
		assertNotNull(ruleDef2.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(2, rulesToEval.size());
		rulesToEval.forEach(rule -> rule.evaluate());		
		
		var scopes = inspector.getScopes(inst1);
		scopes.forEach(scope -> inspector.printScope(scope));
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;		
		assertEquals(2, scopes.size());
		assertTrue(scopes.stream().allMatch(scope -> inspector.getEvalCountFromScope(scope) == 2));
		
		scopes = inspector.getScopes(inst1, subProp.getURI());		
		assertEquals(1, scopes.size());		
		
		var affected = repo.getRulesAffectedByTypeRemoval(inst1, artSubType.getURI());
		assertEquals(2, affected.size());
	}
	
	/*
	 * 2 rule instantiation on same art -> one context scope per context, that lists both rules 
	 * */
	@Test 
	void testRuleDefDeactivation() throws RuleException {
		var ruleDef2 = getBasicArtSubTypeWithSubRefOnLabelRule(1);		
		System.out.println(ruleDef2.getExpressionError());
		assertFalse(ruleDef2.hasExpressionError());
		assertNotNull(ruleDef2.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		rulesToEval.forEach(rule -> rule.evaluate());		
		rulesToEval = repo.getRulesAffectedByCreation(inst2);				
		assertEquals(1, rulesToEval.size());
		rulesToEval.forEach(rule -> rule.evaluate());
		
		var deactive = repo.deactivateRulesToNoLongerUsedUponRuleDefinitionDeactivation(ruleDef2);
		assertEquals(2, deactive.size());
		
		var reeval = repo.getRulesToEvaluateUponRuleDefinitionActivation(ruleDef2);
		reeval.forEach(rule -> rule.evaluate());
		assertEquals(2, reeval.size());
		var scopes = inspector.getScopes(inst1);
		scopes.forEach(scope -> inspector.printScope(scope));
		assertTrue(scopes.stream().allMatch(scope -> inspector.getEvalCountFromScope(scope) == 1));
		//RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}
	
	/*	 
	 * art removal --> rules don't point to art anymore.  
	 * */
	@Test 
	void testRemoveArtFromTwoRules() throws RuleException {		
		var ruleDef1 = getBasicArtSubTypeWithSubRefOnLabelRule(1);
		var ruleDef2 = getBasicArtSubTypeWithSubRefOnLabelRule(2);
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(2, rulesToEval.size());
		
		inst1.removeProperties();			
		var affectedRules = repo.getRemovedRulesAffectedByInstanceRemoval(inst1);						
		assertEquals(2, affectedRules.size());
		assertTrue(affectedRules.stream().allMatch(rule -> rule.isEnabled() == false));
		
		var scopes = inspector.getScopes(inst1);
		assertEquals(0, scopes.size());
		
		var allScopes = inspector.getAllScopes();
		assertEquals(0, allScopes.size());
		
	}
	

	@Test
	void testReifyEvaluationsFromModel() throws RuleException {
		var ruleDef1 = getBasicArtSubTypeWithSubRefOnLabelRule(1);
		var ruleDef2 = getBasicArtSubTypeWithSubRefOnLabelRule(2);
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(2, rulesToEval.size());
		rulesToEval.forEach(rule -> rule.evaluate());
		
		// now lets load model into new repo
		var repo2 = new RuleRepository(factory);
		// and see whether a fictive change returns the rules
		var affected = repo2.getRulesAffectedByChange(inst1, subProp.asProperty());
		assertEquals(2, affected.size());		
	}
	
	
	
	
	protected Set<OntIndividual> getScopesForRule(OntIndividual ruleEvalObj) {
		var scopes = new HashSet<OntIndividual>();		
		var iter = ruleEvalObj.listProperties(factory.getHavingScopePartProperty().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var scope = stmt.getResource().as(OntIndividual.class);			
			scopes.add(scope);
		}
		return scopes;
	}
	
	protected RDFRuleDefinition getBasicArtSubTypeWithSubRefOnLabelRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artSubType)
				.withDescription("TestRule"+counter)
				.withRuleTitle("TestRuleTitle"+counter)
				.withRuleExpression("self.subref->exists(art | art.label = 'someLabel')")
				.build();
	}
	
	protected RDFRuleDefinition getBasicArtTypeWithRefOnLabelRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("Test2Rule"+counter)
				.withRuleTitle("Test2RuleTitle"+counter)
				.withRuleExpression("self.ref->exists(art | art.label = 'someLabel')")
				.build();
	}
	

}

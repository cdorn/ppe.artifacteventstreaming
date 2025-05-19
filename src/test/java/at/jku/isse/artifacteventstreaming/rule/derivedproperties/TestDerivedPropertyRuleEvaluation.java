package at.jku.isse.artifacteventstreaming.rule.derivedproperties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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

import at.jku.isse.artifacteventstreaming.rule.RuleException;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleRepositoryInspector;
import at.jku.isse.artifacteventstreaming.rule.definition.RDFRuleDefinition;


class TestDerivedPropertyRuleEvaluation extends TestDerivedPropertyRuleDefinitions {

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
	void testDerivedStringProperty() throws RuleException {
		
		inst1.addProperty(labelProp, "Reenable");
		var ruleDef = getBasicArtTypeWithDerivedTitleRule(1);
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		
		var derivedValue = inst1.getProperty(labelDerivedProp).getString();
		assertEquals("Re", derivedValue);
		
		inst1.removeAll(labelProp);
		inst1.addProperty(labelProp, "De-enable");
		evalWrapper.evaluate();
		derivedValue = inst1.getProperty(labelDerivedProp).getString();
		assertEquals("De", derivedValue);
		
	}

	@Test
	void testDerivedIntProperty() throws RuleException {
		inst1.addProperty(labelProp, "Reenable");
		var ruleDef = getBasicArtTypeWithDerivedPrioRule(1);
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		
		var derivedValue = inst1.getProperty(priorityDerivedProp).getInt();
		assertEquals(8, derivedValue);
		
		inst1.removeAll(labelProp);
		inst1.addProperty(labelProp, "De-enable");
		evalWrapper.evaluate();
		derivedValue = inst1.getProperty(priorityDerivedProp).getInt();
		assertEquals(9, derivedValue);	
		
		inst1.removeAll(labelProp);
		evalWrapper.evaluate();
		derivedValue = inst1.getProperty(priorityDerivedProp).getInt();
		assertEquals(0, derivedValue);	
	}
	
	@Test
	void testDerivedObjectSetProperty() throws RuleException {
		inst1.addProperty(refProp.asProperty(), inst2);
		var ruleDef = getBasicArtTypeWithDerivedObjectSetRule(1);
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		
		var derivedValue = inst1.getProperty(refPropDerived.asProperty()).getResource();
		assertEquals(inst2.getURI(), derivedValue.getURI());
		
		var inst3 = artSubType.createIndividual(baseURI+"inst3");
		inst1.addProperty(refProp.asProperty(), inst3);
		evalWrapper.evaluate();
		var iter = inst1.listProperties(refPropDerived.asProperty());
		Set<String> derivedValues = new HashSet<>();
		while(iter.hasNext()) {
			derivedValues.add(iter.next().getResource().getURI());
		}
		assertEquals(Set.of(inst2.getURI(), inst3.getURI()), derivedValues);
		
		inst1.removeAll(refProp.asProperty());
		evalWrapper.evaluate();
		var lastDerivedValue = inst1.getProperty(refPropDerived.asProperty());
		assertNull(lastDerivedValue);
	}
	
	@Test
	void testDerivedSetProperty() throws RuleException {
		inst1.addLiteral(numbersProp.asProperty(), 5);
		var ruleDef = getBasicArtTypeWithDerivedLiteraltSetRule(1);
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		
		var derivedValue = inst1.getProperty(numbersDerivedProp.asProperty()).getInt();
		assertEquals(5, derivedValue);
		
		inst1.addLiteral(numbersProp.asProperty(), 6);
		evalWrapper.evaluate();
		var iter = inst1.listProperties(numbersDerivedProp.asProperty());
		Set<Integer> derivedValues = new HashSet<>();
		while(iter.hasNext()) {
			derivedValues.add(iter.next().getInt());
		}
		assertEquals(Set.of(5,6), derivedValues);
		
		inst1.removeAll(numbersProp.asProperty());
		evalWrapper.evaluate();
		var lastDerivedValue = inst1.getProperty(numbersDerivedProp.asProperty());
		assertNull(lastDerivedValue);
	}
	
	
	@Test
	void testDerivedCountSetProperty() throws RuleException {
		inst1.addProperty(refProp.asProperty(), inst2);
		var ruleDef = getBasicArtTypeWithDerivedIndivLiteralFromSetRule(1);
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		
		var derivedValue = inst1.getProperty(priorityDerivedProp.asProperty()).getInt();
		assertEquals(1, derivedValue);
		
		var inst3 = artSubType.createIndividual(baseURI+"inst3");
		inst1.addProperty(refProp.asProperty(), inst3);
		evalWrapper.evaluate();
		derivedValue = inst1.getProperty(priorityDerivedProp.asProperty()).getInt();
		assertEquals(2, derivedValue);
		
		inst1.removeAll(refProp.asProperty());
		evalWrapper.evaluate();
		derivedValue = inst1.getProperty(priorityDerivedProp.asProperty()).getInt();
		assertEquals(0, derivedValue);
	}
//
//	
//	/*	 
//	 * rules has been evaluated, new art added, now check if that scope is listed 	
//	 * */
//	@Test
//	void testScopeAdding() throws RuleException {
//		var ruleDef = repo.getRuleBuilder()
//				.withContextType(artType)
//				.withDescription("TestRule")
//				.withRuleTitle("TestRuleTitle")
//				.withRuleExpression("self.asType(<"+ artSubType.getURI()+">).subref->exists(art | art.label = 'someLabel')")
//				.build();
//		System.out.println(ruleDef.getExpressionError());
//		assertFalse(ruleDef.hasExpressionError());
//		assertNotNull(ruleDef.getSyntaxTree());
//		
//		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
//		assertEquals(1, rulesToEval.size());
//		var evalWrapper = rulesToEval.iterator().next();
//		evalWrapper.evaluate();
//		System.out.println("All Scopes before addition:");
//		inspector.getAllScopes().forEach(scope -> inspector.printScope(scope));					
//		var evals = inspector.getEvalWrappersFromScopes(inst2, labelProp.getURI());
//		assertEquals(0, evals.size());
//		
//		// now lets add this to property
//		inst1.addProperty(subProp.asProperty(), inst2);
//		rulesToEval = repo.getRulesAffectedByChange(inst1, subProp.asProperty());
//		assertEquals(1, rulesToEval.size());
//		evalWrapper = rulesToEval.iterator().next();
//		evalWrapper.evaluate();							
//		evals = inspector.getEvalWrappersFromScopes(inst2, labelProp.getURI());
//		assertEquals(1, evals.size());
//		
//		
//		var ruleScopes = getScopesForRule(((RuleEvaluationWrapperResource) evalWrapper).getRuleEvalObj());
//		System.out.println("Rule 1 Scopes after addition:");
//		ruleScopes.forEach(scope -> inspector.printScope(scope));
//		assertEquals(2, ruleScopes.size()); // 2 not 3 because rule does not point to Scope that is used to represent the context usage
//		
//		// now lets see which ones we would activate if we official add art2
//		var rules = repo.getRulesAffectedByCreation(inst2);
//		assertEquals(2, rules.size()); // 1 for the new instantiation, 1 for the use of inst2 in ruleEval1
//	}
//	
//	/*	 
//	 * rules has been evaluated, new art added, and removed again, is also the scope removed?
//	 * */
//	@Test
//	void testScopeRemovingProperty() throws RuleException {
//		testScopeAdding();
//		inst1.remove(subProp.asProperty(), inst2);
//		var rulesToEval = repo.getRulesAffectedByChange(inst1, subProp.asProperty());
//		assertEquals(1, rulesToEval.size());
//		
//		var evalWrapper = rulesToEval.iterator().next();
//		evalWrapper.evaluate();
//		var result = evalWrapper.getEvaluationResult();
//		assertEquals(Boolean.FALSE, result);
//		assertEquals(inst1, evalWrapper.getContextInstance());
//		
//		var evalsOf2 = inspector.getEvalWrappersFromScopes(inst2, labelProp.getURI());
//		assertEquals(0, evalsOf2.size());		
//		var ruleScopes = getScopesForRule(((RuleEvaluationWrapperResource) evalWrapper).getRuleEvalObj());
//		System.out.println("Rule 1 Scopes after addition:");
//		ruleScopes.forEach(scope -> inspector.printScope(scope));
//		assertEquals(1, ruleScopes.size()); // 1 not 2 because rule does not point to Scope that is used to represent the context usage
//		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
//	}
//		
//	/*
//	 * 2 rule instantiation on same art -> one context scope per context, that lists both rules 
//	 * */
//	@Test 
//	void testTwoRuleScopeAdded() throws RuleException {
//		getBasicArtSubTypeWithSubRefOnLabelRule(1);
//		var ruleDef2 = getBasicArtSubTypeWithSubRefOnLabelRule(2);
//		System.out.println(ruleDef2.getExpressionError());
//		assertFalse(ruleDef2.hasExpressionError());
//		assertNotNull(ruleDef2.getSyntaxTree());
//		
//		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
//		assertEquals(2, rulesToEval.size());
//		rulesToEval.forEach(rule -> rule.evaluate());		
//		
//		var scopes = inspector.getScopes(inst1);
//		scopes.forEach(scope -> inspector.printScope(scope));
//		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;		
//		assertEquals(2, scopes.size());
//		assertTrue(scopes.stream().allMatch(scope -> inspector.getEvalCountFromScope(scope) == 2));
//		
//		scopes = inspector.getScopes(inst1, subProp.getURI());		
//		assertEquals(1, scopes.size());		
//		
//		var affected = repo.getRulesAffectedByTypeRemoval(inst1, artSubType.getURI());
//		assertEquals(2, affected.size());
//	}
//	
//	/*
//	 * 2 rule instantiation on same art -> one context scope per context, that lists both rules 
//	 * */
//	@Test 
//	void testRuleDefDeactivation() throws RuleException {
//		var ruleDef2 = getBasicArtSubTypeWithSubRefOnLabelRule(1);		
//		System.out.println(ruleDef2.getExpressionError());
//		assertFalse(ruleDef2.hasExpressionError());
//		assertNotNull(ruleDef2.getSyntaxTree());
//		
//		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
//		assertEquals(1, rulesToEval.size());
//		rulesToEval.forEach(rule -> rule.evaluate());		
//		rulesToEval = repo.getRulesAffectedByCreation(inst2);				
//		assertEquals(1, rulesToEval.size());
//		rulesToEval.forEach(rule -> rule.evaluate());
//		
//		var deactive = repo.deactivateRulesToNoLongerUsedUponRuleDefinitionDeactivation(ruleDef2);
//		assertEquals(2, deactive.size());
//		
//		var reeval = repo.getRulesToEvaluateUponRuleDefinitionActivation(ruleDef2);
//		reeval.forEach(rule -> rule.evaluate());
//		assertEquals(2, reeval.size());
//		var scopes = inspector.getScopes(inst1);
//		scopes.forEach(scope -> inspector.printScope(scope));
//		assertTrue(scopes.stream().allMatch(scope -> inspector.getEvalCountFromScope(scope) == 1));
//		//RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
//	}
//	
//	/*	 
//	 * art removal --> rules don't point to art anymore.  
//	 * */
//	@Test 
//	void testRemoveArtFromTwoRules() throws RuleException {		
//		var ruleDef1 = getBasicArtSubTypeWithSubRefOnLabelRule(1);
//		var ruleDef2 = getBasicArtSubTypeWithSubRefOnLabelRule(2);
//		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
//		assertEquals(2, rulesToEval.size());
//		
//		inst1.removeProperties();			
//		var affectedRules = repo.getRemovedRulesAffectedByInstanceRemoval(inst1);						
//		assertEquals(2, affectedRules.size());
//		assertTrue(affectedRules.stream().allMatch(rule -> rule.isEnabled() == false));
//		
//		var scopes = inspector.getScopes(inst1);
//		assertEquals(0, scopes.size());
//		
//		var allScopes = inspector.getAllScopes();
//		assertEquals(0, allScopes.size());
//		
//	}
//	
//
//	@Test
//	void testReifyEvaluationsFromModel() throws RuleException {
//		var ruleDef1 = getBasicArtSubTypeWithSubRefOnLabelRule(1);
//		var ruleDef2 = getBasicArtSubTypeWithSubRefOnLabelRule(2);
//		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
//		assertEquals(2, rulesToEval.size());
//		rulesToEval.forEach(rule -> rule.evaluate());
//		
//		// now lets load model into new repo
//		var repo2 = new RuleRepository(factory);
//		// and see whether a fictive change returns the rules
//		var affected = repo2.getRulesAffectedByChange(inst1, subProp.asProperty());
//		assertEquals(2, affected.size());		
//	}
//	
	
	
	
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
	
//	protected RDFRuleDefinition getBasicArtSubTypeWithSubRefOnLabelRule(int counter) throws RuleException {
//		return repo.getRuleBuilder()
//				.withContextType(artSubType)
//				.withDescription("TestRule"+counter)
//				.withRuleTitle("TestRuleTitle"+counter)
//				.withRuleExpression("self.subref->exists(art | art.title = 'someLabel')")
//				.build();
//	}
//	
//	protected RDFRuleDefinition getBasicArtTypeWithRefOnLabelRule(int counter) throws RuleException {
//		return repo.getRuleBuilder()
//				.withContextType(artType)
//				.withDescription("Test2Rule"+counter)
//				.withRuleTitle("Test2RuleTitle"+counter)
//				.withRuleExpression("self.ref->exists(art | art.title = 'someLabel')")
//				.build();
//	}
//	
//	protected RDFRuleDefinition getBasicArtTypeWithRefOnPriorityRule(int counter) throws RuleException {
//		return repo.getRuleBuilder()
//				.withContextType(artType)
//				.withDescription("Test3Rule"+counter)
//				.withRuleTitle("Test3RuleTitle"+counter)
//				.withRuleExpression("self.ref->exists(art | art.priority = 1)")
//				.build();
//	}
	
	protected RDFRuleDefinition getBasicArtTypeWithDerivedTitleRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("Test3Rule"+counter)
				.withRuleTitle("Test3RuleTitle"+counter)
				.withRuleExpression("self.title.substring(1,2)")
				.forDerivedProperty(labelDerivedProp)
				.build();
	}
	
	protected RDFRuleDefinition getBasicArtTypeWithDerivedPrioRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("Test3Rule"+counter)
				.withRuleTitle("Test3RuleTitle"+counter)
				.withRuleExpression("self.title.size()")
				.forDerivedProperty(priorityDerivedProp)
				.build();
	}
	
	protected RDFRuleDefinition getBasicArtTypeWithDerivedObjectSetRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("Test3Rule"+counter)
				.withRuleTitle("Test3RuleTitle"+counter)
				.withRuleExpression("self.ref")
				.forDerivedProperty(refPropDerived)
				.build();
	}
	
	protected RDFRuleDefinition getBasicArtTypeWithDerivedLiteraltSetRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("Test3Rule"+counter)
				.withRuleTitle("Test3RuleTitle"+counter)
				.withRuleExpression("self.numbers.asSet()")
				.forDerivedProperty(numbersDerivedProp)
				.build();
	}
	
	protected RDFRuleDefinition getBasicArtTypeWithDerivedIndivLiteralFromSetRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("Test3Rule"+counter)
				.withRuleTitle("Test3RuleTitle"+counter)
				.withRuleExpression("self.ref.size()")
				.forDerivedProperty(priorityDerivedProp)
				.build();
	}
}

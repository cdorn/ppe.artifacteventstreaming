package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestRuleRepair extends TestRuleEvaluation {
	
	RepairService repairService;
	
	@Override
	@BeforeEach
	void setup() {
		super.setup();
		repairService = new RepairService(m, repo);
	}
	
	@Test
	void testSetRepairable() {
		// when set at higher level, repairable is true for child and super type
		repairService.setPropertyRepairable(artType, labelProp, true);		
		assertTrue(repairService.isPropertyRepairable(artType, labelProp));
		assertTrue(repairService.isPropertyRepairable(artSubType, labelProp));
		
		// when set at lower level, repairable only true for child type, not super type
		repairService.setPropertyRepairable(artType, labelProp, false);
		repairService.setPropertyRepairable(artSubType, labelProp, true);
		assertFalse(repairService.isPropertyRepairable(artType, labelProp));
		assertTrue(repairService.isPropertyRepairable(artSubType, labelProp));
	}
	
	@Test
	void testRepairSetProperty() throws RuleException {
		var ruleDef = repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.asType(<"+ artSubType.getURI()+">).subref.size() > 1")
				.build();
		inst1.addProperty(subProp.asProperty(), inst2);
		System.out.println(ruleDef.getExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesToEvaluateUponRuleDefinitionActivation(ruleDef);				
		assertEquals(2, rulesToEval.size());
		rulesToEval.forEach(rule -> rule.evaluate());
		
		var repairRoot = repairService.getRepairRootNode(inst1, ruleDef);
		assertNotNull(repairRoot);
		assertEquals(2, repairRoot.getChildren().size());
	}
	
	@Test
	void testRepairSingleProperty() throws RuleException {
		var ruleDef = repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("TestRule2")
				.withRuleTitle("TestRuleTitle2")
				.withRuleExpression("self.priority > 1")
				.build();
		inst1.addLiteral(priorityProp.asProperty(), 1L);
		System.out.println(ruleDef.getExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesToEvaluateUponRuleDefinitionActivation(ruleDef);				
		assertEquals(2, rulesToEval.size());
		rulesToEval.forEach(rule -> rule.evaluate());
		
		var repairRoot = repairService.getRepairRootNode(inst1, ruleDef);
		assertNotNull(repairRoot);
		assertEquals(2, repairRoot.getChildren().size());
	}
}

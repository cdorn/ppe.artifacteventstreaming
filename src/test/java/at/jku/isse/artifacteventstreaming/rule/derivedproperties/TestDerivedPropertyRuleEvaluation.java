package at.jku.isse.artifacteventstreaming.rule.derivedproperties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.rule.RuleException;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleRepositoryInspector;
import at.jku.isse.artifacteventstreaming.rule.definition.DerivedPropertyRuleDefinition;
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
		var ruleDef = getBasicArtTypeWithDerivedLiteralSetRule(1);
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
	
	@Test
	void testDerivedListProperty() throws RuleException {
		inst1.addLiteral(numbersProp.asProperty(), 5);
		var ruleDef = getBasicArtTypeWithDerivedLiteralListRule(1);
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		
		var derivedList = inst1.getPropertyResourceValue(numbersListDerivedProp.asProperty()).as(Seq.class);
		assertEquals(1, derivedList.size());
		assertEquals(5, derivedList.getInt(1)); // RDF is 1 index based!!!
		
		inst1.addLiteral(numbersProp.asProperty(), 6);
		evalWrapper.evaluate();
		derivedList = inst1.getPropertyResourceValue(numbersListDerivedProp.asProperty()).as(Seq.class);
		assertEquals(2, derivedList.size());
		var content = Set.of(derivedList.getInt(1), derivedList.getInt(2)); 
		assertEquals(Set.of(5,6), content); 
		
		inst1.removeAll(numbersProp.asProperty());
		evalWrapper.evaluate();
		derivedList = inst1.getPropertyResourceValue(numbersListDerivedProp.asProperty()).as(Seq.class);
		assertEquals(0, derivedList.size());
	}
	
	@Test
	void testDerivedListOrderedProperty() throws RuleException {
		var sourceSeq = inst1.getModel().createSeq();
		inst1.addProperty(numbersListProp.asProperty(), sourceSeq);
		sourceSeq.add(5);
		var ruleDef = getBasicArtTypeWithDerivedList2ListRule(1);
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		
		var derivedList = inst1.getPropertyResourceValue(numbersListDerivedProp.asProperty()).as(Seq.class);
		assertEquals(1, derivedList.size());
		assertEquals(5, derivedList.getInt(1)); // RDF is 1 index based!!!
		
		
		sourceSeq.add(1,6);
		evalWrapper.evaluate();
		derivedList = inst1.getPropertyResourceValue(numbersListDerivedProp.asProperty()).as(Seq.class);
		assertEquals(2, derivedList.size());
		var content = List.of(derivedList.getInt(1), derivedList.getInt(2)); 
		assertEquals(List.of(6,5), content); 
		
		sourceSeq.remove(2);
		sourceSeq.remove(1);
		evalWrapper.evaluate();
		derivedList = inst1.getPropertyResourceValue(numbersListDerivedProp.asProperty()).as(Seq.class);
		assertEquals(0, derivedList.size());
	}

	@Test
	void testCompatibleListDeriveEval() throws RuleException {
		var sourceSeq = inst1.getModel().createSeq();
		inst1.addProperty(artList.asProperty(), sourceSeq);
		sourceSeq.add(inst2);
		
		var ruleDef = repo.getRuleBuilder()
					.withContextType(artSubType)
					.withDescription("Test3Rule")
					.withRuleTitle("Test3RuleTitle")
					.withRuleExpression("self.artList.asList()")
					.forDerivedProperty(artListDerivedProp)
					.build();
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		var rulesToEval = repo.getRulesAffectedByCreation(inst1);				
		assertEquals(1, rulesToEval.size());
		var evalWrapper = rulesToEval.iterator().next();
		evalWrapper.evaluate();
		
		var derivedList = inst1.getPropertyResourceValue(artListDerivedProp.asProperty()).as(Seq.class);
		assertEquals(1, derivedList.size());
		var entry = derivedList.getObject(1);
		assertTrue(entry.isResource());
		assertEquals(inst2, entry); // RDF is 1 index based!!!
		
		var inst3 = artSubType.createIndividual(baseURI+"inst3");
		sourceSeq.add(1, inst3);
		evalWrapper.evaluate();
		derivedList = inst1.getPropertyResourceValue(artListDerivedProp.asProperty()).as(Seq.class);
		assertEquals(2, derivedList.size());
		var content = List.of(derivedList.getObject(1), derivedList.getObject(2)); 
		assertEquals(List.of(inst3, inst2), content); 
		
		sourceSeq.remove(2);
		sourceSeq.remove(1);
		evalWrapper.evaluate();
		derivedList = inst1.getPropertyResourceValue(artListDerivedProp.asProperty()).as(Seq.class);
		assertEquals(0, derivedList.size());
	}
	
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
	
	protected RDFRuleDefinition getBasicArtTypeWithDerivedLiteralSetRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("Test3Rule"+counter)
				.withRuleTitle("Test3RuleTitle"+counter)
				.withRuleExpression("self.numbers.asSet()")
				.forDerivedProperty(numbersDerivedProp)
				.build();
	}
	
	protected RDFRuleDefinition getBasicArtTypeWithDerivedLiteralListRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("Test3Rule"+counter)
				.withRuleTitle("Test3RuleTitle"+counter)
				.withRuleExpression("self.numbers.asList()")
				.forDerivedProperty(numbersListDerivedProp)
				.build();
	}
	protected RDFRuleDefinition getBasicArtTypeWithDerivedList2ListRule(int counter) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(artType)
				.withDescription("Test3Rule"+counter)
				.withRuleTitle("Test3RuleTitle"+counter)
				.withRuleExpression("self.numbersList.asList()")
				.forDerivedProperty(numbersListDerivedProp)
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

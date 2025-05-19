package at.jku.isse.artifacteventstreaming.rule;

import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.elementHasRuleScopeURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.havingScopeURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.ruleContextElementURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.ruleContextTypeURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.ruleEvaluationErrorURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.ruleEvaluationResultBaseTypeURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.ruleEvaluationResultURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.ruleExpressionErrorURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.ruleHasConsistentResultURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.ruleIsEnabledURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.ruleScopePartURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.uri;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.usedInRuleURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.usingElementURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.usingPropertyURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairTreeNodeURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairLiteralValueURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairNodeParentURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairNodeTypeURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairObjectValueURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairOperationURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairPredicateURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairRestrictionURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairSubjectURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.hasRepairNodesURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.repairNodeChildOrderURI;
import static at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory.derivedPredicateURI;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;

import at.jku.isse.artifacteventstreaming.rule.definition.RuleDefinitionBuilder;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import lombok.Getter;

public class RuleSchemaProvider {

	private final OntModel model;
	
	@Getter private OntClass definitionType;
	@Getter private OntDataProperty expressionProperty;
	@Getter private OntObjectProperty contextTypeProperty;
	@Getter private OntDataProperty expressionErrorProperty;
	
	@Getter private OntClass resultBaseType;
	@Getter private OntDataProperty evaluationErrorProperty;
	@Getter private OntObjectProperty evaluationResultProperty;
	@Getter private OntDataProperty evaluationHasConsistentResultProperty;
	@Getter private OntObjectProperty contextElementScopeProperty;
	
	@Getter private OntDataProperty isEnabledProperty;
	
	@Getter private OntClass ruleScopeCollection;
	@Getter private OntObjectProperty usingPredicateProperty;
	@Getter private OntObjectProperty usingElementProperty;
	@Getter private OntObjectProperty usedInRuleProperty;
	
	// used by rule to point to scopeUsageEntry
	@Getter private OntObjectProperty havingScopePartProperty;
	
	// additional property on individual to point to scope
	@Getter private OntObjectProperty hasRuleScope;
	
	// flat repair tree serialization
	@Getter private OntClass repairTreeNodeType;
	
	@Getter private OntDataProperty repairNodeTypeProperty;
	@Getter private OntObjectProperty repairSubjectProperty;
	@Getter private OntObjectProperty repairPredicateProperty;
	@Getter private OntDataProperty repairLiteralValueProperty;
	@Getter private OntObjectProperty repairObjectValueProperty;
	@Getter private OntDataProperty repairRestrictionProperty;
	@Getter private OntDataProperty repairOperationProperty;
	@Getter private OntObjectProperty repairNodeParentProperty;
	@Getter private OntDataProperty repairNodeChildOrderProperty;
	// to link from eval base type to set of repair nodes
	@Getter private OntObjectProperty hasRepairNodesProperty;
	
	// derived properties
	@Getter private OntClass derivedPropertyRuleType;
	@Getter private OntObjectProperty derivedPredicateProperty;
	
	
	@Getter private final MetaModelSchemaTypes schemaFactory;
	
	private RDFModelAccess modelAccess;
	
	public RuleSchemaProvider(OntModel model, MetaModelSchemaTypes metaTypes) {
		super();
		model.setNsPrefix("rules", uri);		
		this.model = model;		
		this.schemaFactory = metaTypes;
		modelAccess = new RDFModelAccess(model, metaTypes);
		initTypes();
	}
	
	private void initTypes() {
		initOntClasses();		
		initDefinitionTypeProperties();
		initResultBaseTypeProperties();
		initScopePartTypeProperties();
		initRuleContextReferenceProperty();
		initRepairNodeTypeProperties();
		initDerivedPredicateTypeProperties();
	}
	


	private void initOntClasses() {
		definitionType = model.getOntClass(RuleSchemaFactory.ruleDefinitionURI);
		resultBaseType = model.getOntClass(ruleEvaluationResultBaseTypeURI);
		ruleScopeCollection = model.getOntClass(ruleScopePartURI);
		repairTreeNodeType = model.getOntClass(repairTreeNodeURI);
	}

	private void initDefinitionTypeProperties() {		
		expressionProperty = model.getDataProperty(RuleSchemaFactory.ruleExpressionURI);				
		expressionErrorProperty = model.getDataProperty(ruleExpressionErrorURI);				
		contextTypeProperty = model.getObjectProperty(ruleContextTypeURI);		
	}
	
	private void initResultBaseTypeProperties() {				
		evaluationResultProperty = model.getObjectProperty(ruleEvaluationResultURI);			
		evaluationHasConsistentResultProperty = model.getDataProperty(ruleHasConsistentResultURI);				
		evaluationErrorProperty = model.getDataProperty(ruleEvaluationErrorURI);		
		contextElementScopeProperty = model.getObjectProperty(ruleContextElementURI);			
		havingScopePartProperty = model.getObjectProperty(havingScopeURI);		
		isEnabledProperty = model.getDataProperty(ruleIsEnabledURI);					
	}
	
	private void initScopePartTypeProperties() {						
		usingPredicateProperty = model.getObjectProperty(usingPropertyURI);				
		usingElementProperty = model.getObjectProperty(usingElementURI);		
		usedInRuleProperty = model.getObjectProperty(usedInRuleURI);		
	}

	/**
	 *  creates the reference property from instance/individual to rule scopes
	 * */
	private void initRuleContextReferenceProperty() {
		hasRuleScope = model.getObjectProperty(elementHasRuleScopeURI);		
	}
	
	private void initRepairNodeTypeProperties() {
		repairNodeTypeProperty = model.getDataProperty(repairNodeTypeURI);
		repairSubjectProperty = model.getObjectProperty(repairSubjectURI);
		repairPredicateProperty = model.getObjectProperty(repairPredicateURI);
		repairLiteralValueProperty = model.getDataProperty(repairLiteralValueURI);
		repairObjectValueProperty = model.getObjectProperty(repairObjectValueURI);
		repairRestrictionProperty = model.getDataProperty(repairRestrictionURI);
		repairOperationProperty = model.getDataProperty(repairOperationURI);
		repairNodeParentProperty = model.getObjectProperty(repairNodeParentURI);
		repairNodeChildOrderProperty = model.getDataProperty(repairNodeChildOrderURI);
		
		hasRepairNodesProperty = model.getObjectProperty(hasRepairNodesURI);
	}
	
	private void initDerivedPredicateTypeProperties() {
		derivedPropertyRuleType = model.getOntClass(RuleSchemaFactory.derivedPropertyRuleDefinitionURI);
		derivedPredicateProperty = model.getObjectProperty(derivedPredicateURI);
	}
	
	public RuleDefinitionBuilder createRuleDefinitionBuilder() {
		return new RuleDefinitionBuilder(this);
	}

	public RDFModelAccess getModelAccess() {
		return modelAccess;
	}
	
	
	
}

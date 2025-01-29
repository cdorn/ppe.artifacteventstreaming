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

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;

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
	@Getter private final RuleSchemaFactory schemaFactory;
	
	private RDFModelAccess modelAccess;
	
	public RuleSchemaProvider(OntModel model, RuleSchemaFactory schemaFactory) {
		super();
		model.setNsPrefix("rules", uri);
		schemaFactory.addRuleSchemaToModel(model);		
		this.schemaFactory = schemaFactory;
		this.model = model;		
		modelAccess = new RDFModelAccess(model, schemaFactory.getPropertyCardinalityTypes());
		initTypes();
	}
	
	private void initTypes() {
		initOntClasses();		
		initDefinitionTypeProperties();
		initResultBaseTypeProperties();
		initScopePartTypeProperties();
		initRuleContextReferenceProperty();
	}
	
	private void initOntClasses() {
		definitionType = model.getOntClass(RuleSchemaFactory.ruleDefinitionURI);
		resultBaseType = model.getOntClass(ruleEvaluationResultBaseTypeURI);
		ruleScopeCollection = model.getOntClass(ruleScopePartURI);
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
	
	public RuleDefinitionBuilder createRuleDefinitionBuilder() {
		return new RuleDefinitionBuilder(this);
	}

	public RDFModelAccess getModelAccess() {
		return modelAccess;
	}
	
	
	
}

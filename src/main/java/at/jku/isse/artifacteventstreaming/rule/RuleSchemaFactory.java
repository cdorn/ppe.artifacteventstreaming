package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;

import at.jku.isse.artifacteventstreaming.schemasupport.SchemaFactory;

public class RuleSchemaFactory extends SchemaFactory {

	private static final String RULEONTOLOGY = "ruleontology";

	public static final String uri = "http://at.jku.isse.artifacteventstreaming.rules#";

	// definition level
	public static final String ruleDefinitionURI = uri+"RuleDefinition";
	public static final String ruleExpressionURI = uri+"ruleExpression";
	public static final String ruleExpressionErrorURI = uri+"ruleExpressionError";
	public static final String ruleContextTypeURI = uri+"ruleContextType";
	// evaluation level
	public static final String ruleEvaluationResultBaseTypeURI = uri+"RuleEvaluation";
	public static final String ruleEvaluationErrorURI = uri+"ruleEvaluationError";
	public static final String ruleEvaluationResultURI = uri+"ruleEvaluationResult";
	public static final String ruleHasConsistentResultURI = uri+"ruleHasConsistentResult";
	public static final String ruleContextElementURI = uri+"ruleContextElement";

	// usage index
	public static final String ruleScopePartURI = uri+"RuleScopePart"; // type
	public static final String usingPropertyURI = uri+"usingProperty";
	public static final String usingElementURI = uri+"usingElement";
	public static final String usedInRuleURI = uri+"usedInRule";
	public static final String havingScopeURI = uri+"havingScope";


	//both levels:
	public static final String ruleIsEnabledURI = uri+"isEnabled";

	// to reference from instance to its use as context in rule evaluations, to know upon instance removal, which evaluations to remove
	public static final String elementHasRuleScopeURI = uri+"hasRuleScope";

	private OntClass definitionType;	
	private OntClass resultBaseType;

	// subclass of Bag
	private OntClass ruleScopeCollection;

	// used by rule to point to scopeUsageEntry
	private OntObjectProperty havingScopePartProperty;

	private final OntModel model;

	public RuleSchemaFactory() {
		this.model = loadOntologyFromFilesystem(RULEONTOLOGY); 			
		initTypes();
		model.setNsPrefix("rules", uri);
		super.writeOntologyToFilesystemn(model, RULEONTOLOGY);
	}

	private void initTypes() {		
		initOntClasses();		
		initDefinitionTypeProperties();
		initResultBaseTypeProperties();
		initScopePartTypeProperties();
		initRuleContextReferenceProperty();				
	}

	private void initOntClasses() {
		definitionType = model.createOntClass(ruleDefinitionURI);
		resultBaseType = model.createOntClass(ruleEvaluationResultBaseTypeURI);
		ruleScopeCollection = model.createOntClass(ruleScopePartURI);
	}

	private void initDefinitionTypeProperties() {

		var expressionProperty = model.getDataProperty(ruleExpressionURI);
		if (expressionProperty == null) {
			expressionProperty = model.createDataProperty(ruleExpressionURI);
			expressionProperty
			.addDomain(definitionType)
			.addRange(model.getDatatype(XSD.xstring));		
			definitionType.addSuperClass(model.createDataMaxCardinality(expressionProperty, 1, null));
		}

		var expressionErrorProperty = model.getDataProperty(ruleExpressionErrorURI);
		if (expressionErrorProperty == null) {
			expressionErrorProperty = model.createDataProperty(ruleExpressionErrorURI);
			expressionErrorProperty
			.addDomain(definitionType)
			.addRange(model.getDatatype(XSD.xstring));
			definitionType.addSuperClass(model.createDataMaxCardinality(expressionErrorProperty, 1, null));
		}

		var contextTypeProperty = model.getObjectProperty(ruleContextTypeURI);
		if (contextTypeProperty == null) {	
			contextTypeProperty = model.createObjectProperty(ruleContextTypeURI);
			contextTypeProperty
			.addDomain(definitionType)
			.addRange(model.createOntClass(OWL2.Class.getURI())); // the concept of 'class' in OWL2, not the java .class 
			definitionType.addSuperClass(model.createObjectMaxCardinality(contextTypeProperty, 1, null));
		}
	}

	private void initResultBaseTypeProperties() {				
		var evaluationResultProperty = model.getObjectProperty(ruleEvaluationResultURI);
		if (evaluationResultProperty == null) {		
			evaluationResultProperty = model.createObjectProperty(ruleEvaluationResultURI);
			evaluationResultProperty.addDomain(resultBaseType);
			// 	no range as any type of object is allowed to be in the output/result of a rule
			//model.createObjectMaxCardinality(evaluationResultProperty, 1, null); has potentially multiple outcomes (when a set is returned)
		}

		var evaluationHasConsistentResultProperty = model.getDataProperty(ruleHasConsistentResultURI);
		if (evaluationHasConsistentResultProperty == null) {
			evaluationHasConsistentResultProperty = model.createDataProperty(ruleHasConsistentResultURI);
			evaluationHasConsistentResultProperty
			.addDomain(resultBaseType)
			.addRange(model.getDatatype(XSD.xboolean));
			resultBaseType.addSuperClass(model.createDataMaxCardinality(evaluationHasConsistentResultProperty, 1, null));
		}

		var evaluationErrorProperty = model.getDataProperty(ruleEvaluationErrorURI);
		if (evaluationErrorProperty == null) {
			evaluationErrorProperty = model.createDataProperty(ruleEvaluationErrorURI);
			evaluationErrorProperty
			.addDomain(resultBaseType)
			.addRange(model.getDatatype(XSD.xstring));
			resultBaseType.addSuperClass(model.createDataMaxCardinality(evaluationErrorProperty, 1, null));
		}

		var contextElementScopeProperty = model.getObjectProperty(ruleContextElementURI);
		if (contextElementScopeProperty == null) {
			contextElementScopeProperty = model.createObjectProperty(ruleContextElementURI);
			contextElementScopeProperty
			.addDomain(resultBaseType)
			.addRange(ruleScopeCollection);  
			resultBaseType.addSuperClass(model.createObjectMaxCardinality(contextElementScopeProperty, 1, null));
		}

		havingScopePartProperty = model.getObjectProperty(havingScopeURI);
		if (havingScopePartProperty == null) {
			havingScopePartProperty = model.createObjectProperty(havingScopeURI);
			havingScopePartProperty
			.addDomain(resultBaseType)
			.addRange(ruleScopeCollection);
		}

		var isEnabledProperty = model.getDataProperty(ruleIsEnabledURI);	
		if (isEnabledProperty == null) {
			isEnabledProperty = model.createDataProperty(ruleIsEnabledURI);
			isEnabledProperty
			.addDomain(definitionType)
			.addDomain(resultBaseType)
			.addRange(model.getDatatype(XSD.xboolean));
			var max1 = model.createDataMaxCardinality(isEnabledProperty, 1, null);
			definitionType.addSuperClass(max1);
			resultBaseType.addSuperClass(max1);			
		}
	}

	private void initScopePartTypeProperties() {						
		var usingPredicateProperty = model.getObjectProperty(usingPropertyURI);
		if (usingPredicateProperty == null) {
			usingPredicateProperty = model.createObjectProperty(usingPropertyURI);
			usingPredicateProperty
			.addDomain(ruleScopeCollection)
			.addRange(model.createOntClass(RDF.Property.getURI()));
			ruleScopeCollection.addSuperClass(model.createObjectMaxCardinality(usingPredicateProperty, 1, null));
		}

		var usingElementProperty = model.getObjectProperty(usingElementURI);
		if (usingElementProperty == null) {
			usingElementProperty = model.createObjectProperty(usingElementURI);
			usingElementProperty
			.addDomain(ruleScopeCollection)
			.addRange(model.createOntClass(OWL2.NamedIndividual.getURI()));
			ruleScopeCollection.addSuperClass(model.createObjectMaxCardinality(usingElementProperty, 1, null));
		}

		var usedInRuleProperty = model.getObjectProperty(usedInRuleURI);
		if (usedInRuleProperty == null) {
			usedInRuleProperty = model.createObjectProperty(usedInRuleURI);
			usedInRuleProperty
			.addDomain(ruleScopeCollection)
			.addRange(resultBaseType);		

			usedInRuleProperty.addInverseProperty(havingScopePartProperty);
		}
	}

	/**
	 *  creates the reference property from instance/individual to rule scopes
	 * */
	private void initRuleContextReferenceProperty() {
		var hasRuleScope = model.getObjectProperty(elementHasRuleScopeURI);
		if (hasRuleScope == null) {
			hasRuleScope = model.createObjectProperty(elementHasRuleScopeURI);
			hasRuleScope.addRange(ruleScopeCollection);
		}

		// usingElementProperty.addInverseProperty(hasRuleScope); //we dont want this as inverse, as if we remove instance, then this backlink would be gone as well.
	}


	public void addRuleSchemaToModel(Model modelToAddOntologyTo) {
		modelToAddOntologyTo.add(model);		
	}
}

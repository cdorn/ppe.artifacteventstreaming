package at.jku.isse.artifacteventstreaming.rule;

import java.util.List;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;

import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType;

public class RuleSchemaFactory {

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

	//both def and instance/eval levels:
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
	private final Dataset ontology;
	private final SingleResourceType singleType;	

	public RuleSchemaFactory(MetaModelSchemaTypes.MetaModelOntology metamodel) {
		this.ontology = metamodel.getMetaontology();
		ontology.begin(ReadWrite.WRITE);
		this.model = metamodel.getMetamodel();
		this.singleType = new SingleResourceType(model); // we use this type provider only on the meta model, no actual runtime model uses this instance
		initTypes();
		model.setNsPrefix("rules", uri);
		ontology.commit();
		ontology.close();
	}

	private void initTypes() {		
		initOntClasses();		
		initDefinitionTypeProperties();
		initResultBaseTypeProperties();
		initScopePartTypeProperties();
		initRuleContextReferenceProperty();				
	}

	private void initOntClasses() {
		
		definitionType = model.getOntClass(ruleDefinitionURI);
		if (definitionType == null)
			definitionType = model.createOntClass(ruleDefinitionURI);
		
		resultBaseType = model.getOntClass(ruleEvaluationResultBaseTypeURI);
		if (resultBaseType == null) {
			resultBaseType = model.createOntClass(ruleEvaluationResultBaseTypeURI);
			resultBaseType.addDisjointClass(definitionType);
		}
		
		ruleScopeCollection = model.getOntClass(ruleScopePartURI);
		if (ruleScopeCollection == null)
			ruleScopeCollection = model.createOntClass(ruleScopePartURI);
	}

	private void initDefinitionTypeProperties() {

		var expressionProperty = model.getDataProperty(ruleExpressionURI);
		if (expressionProperty == null) {
			singleType.createSingleDataPropertyType(ruleExpressionURI, definitionType, model.getDatatype(XSD.xstring)); 
		}

		var expressionErrorProperty = model.getDataProperty(ruleExpressionErrorURI);
		if (expressionErrorProperty == null) {
			singleType.createSingleDataPropertyType(ruleExpressionErrorURI, definitionType, model.getDatatype(XSD.xstring));
		}

		var contextTypeProperty = model.getObjectProperty(ruleContextTypeURI);
		if (contextTypeProperty == null) {	
			singleType.createSingleObjectPropertyType(ruleContextTypeURI, definitionType, model.createOntClass(OWL2.Class.getURI())); // the concept of 'class' in OWL2, not the java .class 			
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
			singleType.createSingleDataPropertyType(ruleHasConsistentResultURI, resultBaseType, model.getDatatype(XSD.xboolean));
		}

		var evaluationErrorProperty = model.getDataProperty(ruleEvaluationErrorURI);
		if (evaluationErrorProperty == null) {
			singleType.createSingleDataPropertyType(ruleEvaluationErrorURI, resultBaseType, model.getDatatype(XSD.xstring));
		}

		var contextElementScopeProperty = model.getObjectProperty(ruleContextElementURI);
		if (contextElementScopeProperty == null) {
			singleType.createSingleObjectPropertyType(ruleContextElementURI, resultBaseType, ruleScopeCollection);  			
		}

		havingScopePartProperty = model.getObjectProperty(havingScopeURI);
		if (havingScopePartProperty == null) {
<<<<<<< HEAD
			havingScopePartProperty = model.createObjectProperty(havingScopeURI);
			havingScopePartProperty.addDomain(resultBaseType);
			havingScopePartProperty.addRange(ruleScopeCollection);
=======
			havingScopePartProperty = singleType.createSingleObjectPropertyType(havingScopeURI, resultBaseType, ruleScopeCollection); //FIXME: this should be a SET property, not Single!!!!
>>>>>>> b91c58406582eaa7283a094d466f14e34da43ec4
		}

		var isEnabledProperty = model.getDataProperty(ruleIsEnabledURI);	
		if (isEnabledProperty == null) {
			singleType.createSingleDataPropertyType(ruleIsEnabledURI, List.of(definitionType, resultBaseType), model.getDatatype(XSD.xboolean));																	
		}
	}

	private void initScopePartTypeProperties() {						
		var usingPredicateProperty = model.getObjectProperty(usingPropertyURI);
		if (usingPredicateProperty == null) {
			singleType.createSingleObjectPropertyType(usingPropertyURI, ruleScopeCollection, model.createOntClass(RDF.Property.getURI()));
		}

		var usingElementProperty = model.getObjectProperty(usingElementURI);
		if (usingElementProperty == null) {
			singleType.createSingleObjectPropertyType(usingElementURI, ruleScopeCollection, model.createOntClass(OWL2.NamedIndividual.getURI()));
		}

		OntObjectProperty usedInRuleProperty = model.getObjectProperty(usedInRuleURI);
		if (usedInRuleProperty == null) {
			usedInRuleProperty = singleType.createBaseObjectPropertyType(usedInRuleURI, ruleScopeCollection, resultBaseType);		
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

}

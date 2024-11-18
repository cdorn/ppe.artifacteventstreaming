package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.riot.protobuf.wire.PB_RDF.RDF_Stream;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

import lombok.Getter;
import lombok.NonNull;

public class RuleFactory {

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
	public static final String elementInRuleUsageEntryURI = uri+"RuleUsage";
	public static final String usingPropertyURI = uri+"usingProperty";
	public static final String usingElementURI = uri+"usingElement";
	
	public static final String elementAsContextInRuleReferenceURI = uri+"isContextIn";
	
	
	private final OntModel model;
	
	@Getter private OntClass definitionType;
	@Getter private OntDataProperty expressionProperty;
	@Getter private OntObjectProperty contextTypeProperty;
	@Getter private OntDataProperty expressionErrorProperty;
	
	@Getter private OntClass resultBaseType;
	@Getter private OntDataProperty evaluationErrorProperty;
	@Getter private OntObjectProperty evaluationResultProperty;
	@Getter private OntDataProperty evaluationHasConsistentResultProperty;
	@Getter private OntObjectProperty contextElementProperty;
	
	// subclass of Bag
	@Getter private OntClass usageEntry;
	@Getter private OntObjectProperty usingPredicateProperty;
	@Getter private OntObjectProperty usingElementProperty;
	
	// additional property on individual for which this rule is the context for
	@Getter private OntObjectProperty elementIsContextForRule;
	
	public RuleFactory(OntModel model) {
		super();
		this.model = model;
		initTypes();
	}
	
	private void initTypes() {
		initDefinitionType();
		initResultBaseType();
		initUsageType();
		initRuleContextReferenceProperty();
	}



	private void initResultBaseType() {
		resultBaseType = model.createOntClass(ruleEvaluationResultBaseTypeURI);
		
		evaluationResultProperty = model.createObjectProperty(ruleEvaluationResultURI);
		evaluationResultProperty.addDomain(resultBaseType);
		// no range as any type of object is allowed to be in the output/result of a rule
		//model.createObjectMaxCardinality(evaluationResultProperty, 1, null); has potentially multiple outcomes (when a set is returned)
		
		evaluationHasConsistentResultProperty = model.createDataProperty(ruleHasConsistentResultURI);
		evaluationHasConsistentResultProperty
			.addDomain(resultBaseType)
			.addRange(model.getDatatype(XSD.xboolean));
		resultBaseType.addSuperClass(model.createDataMaxCardinality(evaluationHasConsistentResultProperty, 1, null));
		
		evaluationErrorProperty = model.createDataProperty(ruleEvaluationErrorURI);
		evaluationErrorProperty
			.addDomain(resultBaseType)
			.addRange(model.getDatatype(XSD.xstring));
		resultBaseType.addSuperClass(model.createDataMaxCardinality(evaluationErrorProperty, 1, null));
		
		contextElementProperty = model.createObjectProperty(ruleContextElementURI);
		contextElementProperty
			.addDomain(resultBaseType)
			.addRange(model.getOntClass(OWL2.Thing));  
		resultBaseType.addSuperClass(model.createObjectMaxCardinality(contextElementProperty, 1, null));
	}

	private void initDefinitionType() {
		definitionType = model.createOntClass(ruleDefinitionURI);
		
		expressionProperty = model.createDataProperty(ruleExpressionURI);
		expressionProperty
			.addDomain(definitionType)
			.addRange(model.getDatatype(XSD.xstring));
		definitionType.addSuperClass(model.createDataMaxCardinality(expressionProperty, 1, null));
		
		expressionErrorProperty = model.createDataProperty(ruleExpressionErrorURI);
		expressionErrorProperty
			.addDomain(definitionType)
			.addRange(model.getDatatype(XSD.xstring));
		definitionType.addSuperClass(model.createDataMaxCardinality(expressionErrorProperty, 1, null));
		
		contextTypeProperty = model.createObjectProperty(ruleContextTypeURI);
		contextTypeProperty
			.addDomain(definitionType)
			.addRange(model.createOntClass(OWL2.Class.getURI())); // the concept of 'class' in OWL2, not the java .class 
		definitionType.addSuperClass(model.createObjectMaxCardinality(contextTypeProperty, 1, null));
	}
	
	private void initUsageType() {
		var superClass = model.createOntClass(RDF.Bag.getURI());
		usageEntry = model.createOntClass(elementInRuleUsageEntryURI);
		usageEntry.addSuperClass(superClass);
		
		usingPredicateProperty = model.createObjectProperty(usingPropertyURI);
		usingPredicateProperty.addDomain(usageEntry);
		usingPredicateProperty.addRange(model.createOntClass(RDF.Property.getURI()));
		usageEntry.addSuperClass(model.createObjectMaxCardinality(usingPredicateProperty, 1, null));
		
		usingElementProperty = model.createObjectProperty(usingPropertyURI);
		usingElementProperty.addDomain(usageEntry);
		usingElementProperty.addRange(model.createOntClass(OWL2.NamedIndividual.getURI()));
		usageEntry.addSuperClass(model.createObjectMaxCardinality(usingElementProperty, 1, null));
	}

	private void initRuleContextReferenceProperty() {
		elementIsContextForRule = model.createObjectProperty(elementAsContextInRuleReferenceURI);
		elementIsContextForRule.addRange(resultBaseType);
		elementIsContextForRule.addInverseProperty(contextElementProperty);
	}
	
	public RuleDefinitionBuilder createRuleDefinitionBuilder() {
		return new RuleDefinitionBuilder(this);
	}
	
	
	
}

package at.jku.isse.artifacteventstreaming.rule.evaluation;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntIndividual.Named;

import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.definition.DerivedPropertyRuleDefinition;
import at.jku.isse.artifacteventstreaming.rule.definition.RDFRuleDefinition;
import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleEvaluationFactory {

	// or create from underlying ont object 
	/**
	 * @param ruleEvalObj pre-existing, that needs wrapping
	 * @param factory to access properties 
	 * @param ruleRepo to access existing definitions (or register new from ruleEvalObject reference
	 * @return rule evaluation wrapper for the provided ont individual
	 * @throws EvaluationException when ruleEvalObject is not a rule evaluation, or it does not point to context rule scope, or it does not point to a rule definition   
	 */
	public static RuleEvaluationWrapperResource loadFromModel(@NonNull OntIndividual ruleEvalObj, @NonNull RuleSchemaProvider factory, @NonNull RuleRepository ruleRepo) throws EvaluationException {
		// check if an rule eval instance
		var type = RuleEvaluationDTO.getRuleTypeClass(ruleEvalObj, factory); 							
		// check if has context
		var ctx = RuleEvaluationDTO.getContextInstanceFrom(ruleEvalObj, factory);		
		// check if has definition, if so fetch from factory
		var def = RuleEvaluationDTO.resolveDefinition(type, ruleRepo);
		// dont evaluate unless evaluate() is called or access to result (lazy loading/generation of eval result)
		if (def == null) {
			log.error("Cannot instantiate RuleEvaluationWrapperResource as RDFRuleDefinition is null");
			return null;
		} else 	
			if( def instanceof DerivedPropertyRuleDefinition derivedDef) {
				// dont evaluate unless evaluate() is called or access to result (lazy loading/generation of eval result)
				return new DerivedPropertyRuleEvaluationWrapperResource(derivedDef, ruleEvalObj, ctx, factory);
			} else {
				return new RuleEvaluationWrapperResource(def, ruleEvalObj, ctx, factory);
			}
	}

	// either create new	
	/**
	 * @param factory for accessing properties
	 * @param def type of rule
	 * @param contextInstance for which instance to create the evaluation object wrapped by this class
	 * @return a new evaluation object wrapper, ensuring that the evaluation and context element point to the same rule scope. 
	 */
	public static RuleEvaluationWrapperResource create(@NonNull RuleSchemaProvider factory, @NonNull RDFRuleDefinition def, @NonNull OntIndividual contextInstance) {
		var uri = createEvalURI(def, contextInstance);
		var evalObj = def.getRuleDefinition().createIndividual(uri);	
		evalObj.addLabel(def.getName());
		RuleEvaluationWrapperResource.addAddRuleEvaluationToNewOrExistingScope(contextInstance, evalObj, factory); // just to make sure that the context scope is set (no effect if already so)
		if (def instanceof DerivedPropertyRuleDefinition derivedDef) {
			return new DerivedPropertyRuleEvaluationWrapperResource(derivedDef, evalObj, contextInstance, factory);
		} else {
			return new RuleEvaluationWrapperResource(def, evalObj, contextInstance, factory);
		}
	}

	public static String createEvalURI(@NonNull RDFRuleDefinition def, @NonNull OntIndividual contextInstance) {
		return def.getRuleDefinition().getURI()+"::"+contextInstance.getLocalName()+"::"+contextInstance.getURI().hashCode(); // we assume here that context instance come from the same namespace, hence are distinguishable based on their localname, but add the hashcode of the uri to be on a safer side
	}
	

}

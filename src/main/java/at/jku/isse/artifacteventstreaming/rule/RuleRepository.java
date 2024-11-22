package at.jku.isse.artifacteventstreaming.rule;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleRepository {
	

	private final RuleFactory factory;
	
	private final Map<String, RDFRuleDefinition> definitions = new HashMap<>();
	private final Map<AnonId, RuleEvaluationWrapperResourceImpl> evaluations = new HashMap<>();
	
	public RuleRepository(RuleFactory factory) {
		super();
		this.factory = factory;
		loadFromModel();
	}
		
	/*
	 * this initialization does not cause rule reevaluation, just prepares all wrapper objects, evaluation is triggered by incoming change events
	 */
	private void loadFromModel() {		
		factory.getDefinitionType().individuals().toList().stream().forEach(this::storeRuleDefinition);
		factory.getResultBaseType().individuals().toList().stream().map(eval -> { // we need to have a list first as otherwise inference has concurrent modificatione exception
				try {
					return RuleEvaluationWrapperResourceImpl.loadFromModel(eval, factory, this);
				} catch (EvaluationException e) {
					log.warn("Error loading evaluation results from model, ignoring: "+e);
					return null;
				}
			})
			.filter(Objects::nonNull)
			.forEach(evalWrapper -> evaluations.put(evalWrapper.getRuleEvalObj().getId(), evalWrapper));
	}
	
	public RuleDefinitionRegistrar getRuleBuilder() {
		return new RuleDefinitionRegistrar(factory, this);
	}
	
	protected void registerRuleDefinition(RDFRuleDefinition def) {
		definitions.putIfAbsent(def.getRuleDefinition().getURI(), def);
	}
	
	protected RDFRuleDefinition storeRuleDefinition(OntIndividual definition) {		
		return definitions.computeIfAbsent(definition.getURI(), k -> new RuleDefinitionImpl(definition, factory));	
	}
	
	public void removeRuleDefinition(@NonNull String ruleDefinitionURI) {
		getRulesAffectedByDeletedRuleDefinition(ruleDefinitionURI);
	}
	
	public RDFRuleDefinition findRuleDefinitionForResource(@NonNull OntClass ruleDef) {
		return findRuleDefinitionForURI(ruleDef.getURI());
	}	
	
	public RDFRuleDefinition findRuleDefinitionForURI(@NonNull String definitionUri) {
		return definitions.get(definitionUri);
	}
		
	public Set<RuleEvaluationWrapperResource> getRulesToEvaluateUponRuleDefinitionActivation(RDFRuleDefinition def) {
		if (!def.hasExpressionError()) {
			// find all instances of the rule context type --> create rule evaluations for every instance
			var individuals = def.getRDFContextType().individuals().collect(Collectors.toSet());
			return individuals.stream()	
				.map(ind -> RuleEvaluationWrapperResourceImpl.create(factory, def, ind))							
				.map(eval -> { evaluations.put(eval.getRuleEvalObj().getId(), eval); return eval;} )
				.map(RuleEvaluationWrapperResource.class::cast)
				.collect(Collectors.toSet());			
		} else {
			return Collections.emptySet();
		}
	}

	
	/**
	 * @param def the RuleDefinition to no longer evaluate upon changes, 
	 * @return all evaluation instances of that definition that now become stale and wont ever be reactivated
	 */
	public Set<RuleEvaluationWrapperResource> getRulesToNoLongerUsedUponRuleDefinitionDeactivation(RDFRuleDefinition def) {
		// set all affected rules to "stale", remove them, they will be recreated
		return def.getRuleDefinition().individuals()
			.map(Resource::getId)
			.map(evaluations::get)			
			.filter(Objects::nonNull)
			.map(eval -> { eval.disable(); return eval;}) // disable the eval wrapper
			.map(eval -> { evaluations.remove(eval.getRuleEvalObj().getId()); return eval; }) // remove evalwrapper from cache
			.collect(Collectors.toSet());		
	}
	
	/**
	 * @param def the RuleDefinition to remove including all evaluation instances thereof 
	 * @return all evaluation instances of that definition that now become stale and wont ever be reactivated because we remove their statements
	 */
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByDeletedRuleDefinition(String definitionURI) {
		var old = definitions.remove(definitionURI);
		if (old != null) {
			var affected = getRulesToNoLongerUsedUponRuleDefinitionDeactivation(old);
			affected.forEach(ruleEval -> ruleEval.delete());
			old.delete();			
			return affected;
		} else {
			return Collections.emptySet();
		}				
	}

	/**	 
	 * @param definition that changed
	 * @return all the instances of the definition that now need re-evaluation
	 */
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByRuleDefinitionExpressionChanged(OntIndividual definition) { //TODO trigger from observer
		var defWrapper = definitions.get(definition.getURI());
		if (defWrapper == null) {
			storeRuleDefinition(definition);
			return getRulesToEvaluateUponRuleDefinitionActivation(defWrapper);
		} else {
			((RuleDefinitionImpl) defWrapper).reloadContextAndExpression(); // ugly cast
			if (!defWrapper.hasExpressionError()) {
				// get all individuals from that type, and if not yet wrapped, do wrap, then return for evaluation
				return defWrapper.getRuleDefinition().individuals()
					.map(evalRes -> evaluations.computeIfAbsent(evalRes.getId(), k -> RuleEvaluationWrapperResourceImpl.create(factory, defWrapper, evalRes)))
					.collect(Collectors.toSet());
			} else {
				return Collections.emptySet(); 
			}
		}		
	}
	
	/**
	 * @param definition to have an updated context type, causes deletion of old evaluations and recreation of new evaluations
	 * @return only the new rule evaluations that need to be triggered/reevaluated
	 */
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByRuleDefinitionContextTypeChanged(OntIndividual definition) {   //TODO trigger from observer
		var def = definitions.get(definition.getURI());
		if (def == null) {
			storeRuleDefinition(definition);									
		} else {
			getRulesToNoLongerUsedUponRuleDefinitionDeactivation(def);			
		}		
		return getRulesToEvaluateUponRuleDefinitionActivation(def);
	}
	
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByCreation(OntIndividual newSubject) {
		// check if it has any existing scope, return also those, as a pessimistic caution as we dont know what the type changes imply	
		var reEval = getAllRuleEvaluationsThatUse(newSubject);			
		// find definitions that have this as context		
		var types = newSubject.classes(false).collect(Collectors.toSet());
		reEval.addAll(
				definitions.values().stream().filter(def -> types.contains(def.getRDFContextType()))
			.map(def -> RuleEvaluationWrapperResourceImpl.create(factory, def, newSubject))	
			.map(eval -> { evaluations.put(eval.getRuleEvalObj().getId(), eval); return eval;} )
			.map(RuleEvaluationWrapperResource.class::cast).toList());			
		//TODO: check for duplicate eval objects, should not happen under correct event handling, but better be on the safe side
		return reEval;
	}
	
	/**
	 * @param subject for which to find any scope elements, then for any reference to rule evaluation fetch the wrapper (creating if necessary)
	 * @return all rule evaluations this subject is used in.
	 */
	private Set<RuleEvaluationWrapperResource> getAllRuleEvaluationsThatUse(OntIndividual subject) {
		Set<RuleEvaluationWrapperResource> evals = new HashSet<>();
		var iter = subject.listProperties(factory.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var scope = stmt.getResource().as(OntIndividual.class);
			var iterRule = scope.listProperties(factory.getUsedInRuleProperty().asProperty());
			while(iterRule.hasNext()) {
				var ruleRes = iterRule.next().getResource().as(OntIndividual.class);
				if (evaluations.containsKey(ruleRes.getId())) {
					evals.add(evaluations.get(ruleRes.getId()));
				} else {
					var evalObj = RuleEvaluationWrapperResourceImpl.loadFromModel(ruleRes, factory, this);
					evaluations.put(evalObj.getRuleEvalObj().getId(), evalObj);
					evals.add(evalObj);
				}
			}
		}				
		return evals;
	}
	
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByChange(OntIndividual changedSubject, Property predicate) {
		// check which rules have this subject and predicate in the scope
		Set<RuleEvaluationWrapperResource> evals = new HashSet<>();
		var iter = changedSubject.listProperties(factory.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var scope = stmt.getResource().as(OntIndividual.class);
			var propUsage = scope.getProperty(factory.getUsingPredicateProperty().asProperty());
			if (propUsage != null && propUsage.getResource().equals(predicate)) { // found a scope with this property in use
				var iterRule = scope.listProperties(factory.getUsedInRuleProperty().asProperty());
				while(iterRule.hasNext()) {
					var ruleRes = iterRule.next().getResource().as(OntIndividual.class);
					if (evaluations.containsKey(ruleRes.getId())) {
						evals.add(evaluations.get(ruleRes.getId()));
					} else {
						var evalObj = RuleEvaluationWrapperResourceImpl.loadFromModel(ruleRes, factory, this);
						evaluations.put(evalObj.getRuleEvalObj().getId(), evalObj);
						evals.add(evalObj);
					}
				}
			}
		}				
		return evals;
	}
	
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByTypeRemoval(OntIndividual subject, String removedTypeURI) {
		// retyping of an instance
		// TODO look where the type info is used as property for casting or checking!!
		// for now we reeval all
		return getAllRuleEvaluationsThatUse(subject);
	}
	
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByRemoval(Resource subject) {
		// see if that instance is used as context in a rule, with exactly that type as contextType -->
		// if so then remove rule evaluation for that instance (incl scope updates)
		Set<RuleEvaluationWrapperResource> evals = new HashSet<>();
		Set<OntIndividual> scopes = new HashSet<>();
		// if an instance is removed completely, then there is no more reference to a scope,	
		var model = factory.getDefinitionType().getModel();
		var iter = model.listResourcesWithProperty(factory.getUsingElementProperty().asProperty(), subject);
		while (iter.hasNext()) { // all the rule scopes associated with the subject
			//now find which of these scope has no usedPredicate property
			var scopeObj = iter.next().as(OntIndividual.class);
			scopes.add(scopeObj);
			var predUsage = scopeObj.getPropertyResourceValue(factory.getUsingPredicateProperty().asProperty());
			if (predUsage == null) { //  --> indicative of context usage, there should only be one
				// then delete all rulesEvals references from there
				var iter2 = scopeObj.listProperties(factory.getUsedInRuleProperty().asProperty());
				while (iter2.hasNext()) {
					var evalObj = iter2.next().getResource().as(OntIndividual.class);
					var evalObjWrapper = evaluations.remove(evalObj.getId());
					if (evalObjWrapper == null) {
						evalObjWrapper = RuleEvaluationWrapperResourceImpl.loadFromModel(evalObj, factory, this);
						// not we dont add to index here, as we remove these anyway before returning
					} 
					evals.add(evalObjWrapper); 
				}				
			}			
		}
		// clean up scopes //TODO check if some empty pointers with eval Wrappers remain!
		scopes.forEach(Resource::removeProperties);
		// delete eval wrapper
		evals.forEach(eval -> eval.delete());				
		return evals;
	}

	public Set<RuleEvaluationWrapperResource> getRulesAffectedByDeletedRuleEvaluation(AnonId ruleEvalId) {
		var eval = evaluations.remove(ruleEvalId);
		if (eval == null) {
			return Collections.emptySet(); // we no longer have this stored anyway
		} else {
			eval.delete();
			return Set.of(eval);
		}
	}


}

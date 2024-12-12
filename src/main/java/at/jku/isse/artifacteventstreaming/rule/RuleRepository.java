package at.jku.isse.artifacteventstreaming.rule;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;

import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleRepository {
	

	private final RuleSchemaProvider factory;
	
	private final Map<String, RDFRuleDefinition> definitions = new HashMap<>();
	@Getter private final EvaluationsCache evaluations = new EvaluationsCache();
	
	public RuleRepository(@NonNull RuleSchemaProvider factory) {
		super();
		this.factory = factory;
		loadFromModel();
	}
		
	/*
	 * this initialization does not cause rule reevaluation, just prepares all wrapper objects, evaluation is triggered by incoming change events
	 */
	private void loadFromModel() {		
		factory.getDefinitionType().individuals().toList().stream().forEach(this::storeRuleDefinition);
		factory.getResultBaseType().individuals().toList().stream().map(eval -> { // we need to have a list first as otherwise inference has concurrent modification exception
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
	
	/**
	 * @param def the RDFRuleDefinition to store/register, 
	 * no rule evaluations will be conducted at this time, as typically the model events/statements resulting from creating the definition will be processed via the RuleTriggerObserver
	 * if no such observer is running, then manual rule evaluation instances are created upon calling {@link #getRulesToEvaluateUponRuleDefinitionActivation(OntIndividual) }
	 */
	protected void registerRuleDefinition(@NonNull RDFRuleDefinition def) {
		var key = def.getRuleDefinition().getURI();
		definitions.putIfAbsent(key, def);
	}
	
	protected RDFRuleDefinition storeRuleDefinition(@NonNull OntObject definition) {	
		var key = definition.getURI();
		return definitions.computeIfAbsent(key, k -> new RDFRuleDefinitionImpl(definition, factory));	
	}
	
	public void removeRuleDefinition(@NonNull String ruleDefinitionURI) {
		removeRulesAffectedByDeletedRuleDefinition(ruleDefinitionURI);
	}
	
	public RDFRuleDefinition findRuleDefinitionForResource(@NonNull OntClass ruleDef) {
		return findRuleDefinitionForURI(ruleDef.getURI());
	}	
	
	public RDFRuleDefinition findRuleDefinitionForURI(@NonNull String definitionUri) {
		return definitions.get(definitionUri);
	}
				
	/**
	 * @param def the rule evaluation objects to obtain for the given rule definition
	 * @return for each instance/individual that matches the rule definition's context type, one rule evaluation object, from which the evaluation must be triggered.
	 * this method does NOT trigger rule evaluation
	 */
	public Set<RuleEvaluationWrapperResource> getRulesToEvaluateUponRuleDefinitionActivation(@NonNull RDFRuleDefinition def) {
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
	 * @return all evaluation instances of that definition that now are deleted and wont ever be reactivated
	 */
	public Set<RuleEvaluationWrapperResource> deactivateRulesToNoLongerUsedUponRuleDefinitionDeactivation(@NonNull RDFRuleDefinition def) {		
		// set all affected rules to "stale", essentially removing them, they will be recreated upon rule activation
		var toRemove = def.getRuleDefinition().individuals()
			.map(Resource::getId)
			.map(evaluations::get)			
			.filter(Objects::nonNull)						
			.collect(Collectors.toSet());
		// two step processing needed as otherwise concurrent modification in reasoner
		return toRemove.stream()			
			.map(eval -> { evaluations.remove(eval.getRuleEvalObj().getId()); return eval;}) // remove evalwrapper from cache
			.map(eval -> { eval.delete(); return eval;}) // disable the eval wrapper
			.map(RuleEvaluationWrapperResource.class::cast)
			.collect(Collectors.toSet());
		
	}
	
	/**
	 * @param def the RuleDefinition to remove including all evaluation instances thereof 
	 * @return all evaluation instances of that definition that now become stale and wont ever be reactivated because we remove their statements
	 */
	public Set<RuleEvaluationWrapperResource> removeRulesAffectedByDeletedRuleDefinition(@NonNull String definitionURI) {
		var old = definitions.remove(definitionURI);
		if (old != null) {
			// if this is called externally, then the rule definition is gone and the following method cannot access individuals thereof to delete,  			
			// var affected = deactivateRulesToNoLongerUsedUponRuleDefinitionDeactivation(old);
			// just ensure there is no local rule eval that is not externally known and remains dangling
			var affected = getRuleEvaluationIdsByDefinitionURI(definitionURI);
			var filtered = affected.stream()
				.map(evaluations::remove) 
				.filter(Objects::nonNull) // if locally not known, then ignore (we assume a consistent cache) 
				.map(ruleEval -> { ruleEval.delete(); return ruleEval; })
				.map(RuleEvaluationWrapperResource.class::cast)
				.collect(Collectors.toSet());
			old.delete();			
			return filtered;
		} else {						
			return Collections.emptySet();
		}				
	}
	
	/**
	 * used upon deletion when definition type is already gone/no longer accessible
	 * */
	private Set<AnonId> getRuleEvaluationIdsByDefinitionURI(String definitionURI) {
		Set<AnonId> evals = new HashSet<>();
		var model = factory.getDefinitionType().getModel();		
		var def = model.createResource(definitionURI);
		var iter = model.listResourcesWithProperty(RDF.type, def);
		while(iter.hasNext()) {
			var res = iter.next();			
			if (res.getId() != null)
				evals.add(res.getId());
		}
		return evals;
	}
	
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByCreation(@NonNull OntIndividual newSubject) {
		// check if it has any existing scope, return also those, as a pessimistic caution as we dont know what the type changes imply	
		var reEval = getAllRuleEvaluationsThatUse(newSubject);			
		List <RuleEvaluationWrapperResourceImpl> ctxEval = getRuleEvaluationsWhereSubjectIsContext(newSubject).stream()
				.map(eval -> getOrWrapAndRegister(eval))
				.filter(Objects::nonNull)
				.toList();
		
		// find definitions that have this as context		
		var types = newSubject.classes(false).collect(Collectors.toSet());
		reEval.addAll(
				definitions.values().stream().filter(def -> types.contains(def.getRDFContextType()))
				.filter(def -> !isSubjectContextOfRule(def, ctxEval)) // filter out if this subject is already context of that rule, which can happen upon type changes
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
	private Set<RuleEvaluationWrapperResource> getAllRuleEvaluationsThatUse(@NonNull OntIndividual subject) {
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
	
	private boolean isSubjectContextOfRule(RDFRuleDefinition def, List <RuleEvaluationWrapperResourceImpl> ctxEval) {
		return ctxEval.stream().anyMatch(eval -> eval.getDefinition().equals(def));
	}
	
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByChange(OntIndividual changedSubject, Property predicate) {
		// if this is a mapentry change -> do not need to support that, as rules dont support maps
		// if this is a list entry change --> find owner of list, and property between owner and this entry
		if (factory.getSchemaFactory().getPropertyCardinalityTypes().getListType().isListContainer(changedSubject)) {
			var entry = findListOwner(changedSubject);
			if (entry != null) {
				changedSubject = entry.getKey();
				predicate = entry.getValue();
			}
		}
		
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
	
	private Entry<OntIndividual, Property> findListOwner(OntIndividual list) {
		var id = list.isAnon() ? list.getId() : list.getURI();
		// first obtain the owner of the list
		var optOwner = factory.getSchemaFactory().getPropertyCardinalityTypes().getCurrentListOwner(list);
		// should only exist one such resource as we dont share lists across individuals
		if (optOwner.isEmpty()) {			
			return null;
		}
		var owner = optOwner.get();
		var commonProps = factory.getSchemaFactory().getPropertyCardinalityTypes().getListType().findListReferencePropertiesBetween(owner, list); // list is never removed, just stays empty
		if (commonProps.size() != 1) {
			return null;
		}
		var listProp = commonProps.get(0);
		return new AbstractMap.SimpleEntry<OntIndividual, Property>(owner.as(OntIndividual.class), listProp);
	}

	public Set<RuleEvaluationWrapperResource> getRulesAffectedByTypeRemoval(@NonNull OntIndividual subject, @NonNull String removedTypeURI) {
		// check if type is completely removed except for own/rdf types
		if (!hasSomeSpecificType(subject)) { 
			// we treat this as an instance removal as if there is no more type available, 
			// then we can't make any guarantees or inferences about available properties, adding a type later will recreate the evaluation object
			return getRemovedRulesAffectedByInstanceRemoval(subject);
		}
		// else check for retyping of an instance, then we need to remove the evaluation as well
		// for each use as context, check if the rule context type still matches the remaining type information		
		var removed = getRuleEvaluationsWhereSubjectIsContext(subject).stream()
			.map(eval -> getOrWrapAndRegister(eval))
			.filter(Objects::nonNull)
			.filter(wrapper -> !isSubjectTypeMatchingRuleContext(wrapper, subject))
			.map(wrapper -> { evaluations.remove(wrapper.getRuleEvalObj().getId()); wrapper.delete(); return wrapper; })
			.collect(Collectors.toSet());				
		
		// TODO also look where the type info is used as property for casting or checking!!				
		// for now we reeval all
		return getAllRuleEvaluationsThatUse(subject);
	}
	
	private boolean isSubjectTypeMatchingRuleContext(RuleEvaluationWrapperResourceImpl wrapper,
			@NonNull OntIndividual subject) {
		var ctxType = wrapper.getDefinition().getRDFContextType();
		return subject.hasOntClass(ctxType, false);		
	}

	private boolean hasSomeSpecificType(OntIndividual subject) {		
		return subject.classes(true)
				.filter(clazz -> !clazz.getURI().equals(OWL2.NamedIndividual.getURI()))
				.count() > 0;		
	}
	
	private Set<OntIndividual> getRuleEvaluationsWhereSubjectIsContext(OntIndividual subject) {
		var evals = new HashSet<OntIndividual>();
		var iter = subject.listProperties(factory.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var scope = stmt.getResource().as(OntIndividual.class);			
			var property = scope.getPropertyResourceValue(factory.getUsingPredicateProperty().asProperty());
			if (property == null ) {								
				var iterRule = scope.listProperties(factory.getUsedInRuleProperty().asProperty());
				while(iterRule.hasNext()) { //property
					var ruleRes = iterRule.next().getResource().as(OntIndividual.class);
					evals.add(ruleRes);
				}
			}
		}
		return evals;
	}
	
	private RuleEvaluationWrapperResourceImpl getOrWrapAndRegister(OntIndividual eval) {		 
			var evalWrapper = evaluations.get(eval.getId());
			if (evalWrapper == null) {
				try {				
					evalWrapper = RuleEvaluationWrapperResourceImpl.loadFromModel(eval, factory, this);
					evaluations.put(eval.getId(), evalWrapper);
					return evalWrapper;
				} catch (EvaluationException e) {
					log.warn("Error loading evaluation results from model, ignoring: "+e);
					return null;
				}
			} else {
				return evalWrapper;
			}
	}
	
	public Set<RuleEvaluationWrapperResource> getRemovedRulesAffectedByInstanceRemoval(Resource subject) {
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
					evals.add(evalObjWrapper); //we delete evals further down 
				}				
			}			
		}
		// clean up scopes //TODO check if some empty pointers with eval Wrappers remain!
		scopes.forEach(Resource::removeProperties);
		// delete eval wrapper
		evals.forEach(eval -> eval.delete());				
		return evals;
	}


	public Set<RuleEvaluationWrapperResource> removeRulesAffectedByDeletedRuleEvaluation(@NonNull AnonId ruleEvalId) {
		log.debug("Handling removal of rule evaluation object: "+ruleEvalId.toString());
		var eval = evaluations.remove(ruleEvalId);
		if (eval == null) {
			return Collections.emptySet(); // we no longer have this stored anyway
		} else {
			eval.delete();
			return Set.of(eval);
		}
	}

	public static class EvaluationsCache{
		
		private final Map<AnonId, RuleEvaluationWrapperResourceImpl> evaluations = new HashMap<>();
		private final Map<String, RuleEvaluationWrapperResourceImpl> indexByCtxAndDef = new HashMap<>();

		private RuleEvaluationWrapperResourceImpl remove(@NonNull AnonId ruleEvalId) {
			var eval = evaluations.remove(ruleEvalId);
			if (eval != null) { // also remove from secondary index
				var key = makeKeyFrom(eval.getContextInstance(), eval.getDefinition());
				indexByCtxAndDef.remove(key);
			}
			return eval;
		}

		private boolean containsKey(AnonId id) {
			return evaluations.containsKey(id);
		}

		public RuleEvaluationWrapperResourceImpl get(AnonId id) {
			return evaluations.get(id);
		}

		private void put(AnonId id, RuleEvaluationWrapperResourceImpl evalWrapper) {
			evaluations.put(id, evalWrapper);
			// also add to secondary index
			var key = makeKeyFrom(evalWrapper.getContextInstance(), evalWrapper.getDefinition());
			indexByCtxAndDef.put(key, evalWrapper);
		}
		
		public Optional<RuleEvaluationWrapperResource> findEvaluation(@NonNull OntObject contextInstance, @NonNull RDFRuleDefinition def) {
			return Optional.ofNullable(indexByCtxAndDef.get(makeKeyFrom(contextInstance, def)));
		}
		
		private String makeKeyFrom(OntObject contextInstance, RDFRuleDefinition def) {
			var ctxId = contextInstance.isAnon() ? contextInstance.getId().toString() : contextInstance.getURI();
			return ctxId+def.getRuleDefinition().getURI();
		}
	}


}

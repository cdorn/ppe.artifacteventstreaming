package at.jku.isse.artifacteventstreaming.rule;

import java.util.AbstractMap;
import java.util.Collection;
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

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;

import at.jku.isse.artifacteventstreaming.rule.definition.RDFRuleDefinition;
import at.jku.isse.artifacteventstreaming.rule.definition.RuleDefinitionFactory;
import at.jku.isse.artifacteventstreaming.rule.definition.RuleDefinitionRegistrar;
import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleEvaluationFactory;
import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleEvaluationWrapperResource;
import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleRepository {
	

	@Getter private final RuleSchemaProvider factory;
	@Getter private final RuleRepositoryInspector inspector;
	
	private final Map<String, RDFRuleDefinition> definitions = new HashMap<>();
	@Getter private final EvaluationsCache evaluations = new EvaluationsCache();
	
	public RuleRepository(@NonNull RuleSchemaProvider factory) {
		super();
		this.factory = factory;
		this.inspector = new RuleRepositoryInspector(factory);
		loadFromModel();
	}
		
	/*
	 * this initialization does not cause rule reevaluation, just prepares all wrapper objects, evaluation is triggered by incoming change events
	 */
	private void loadFromModel() {		
		factory.getDefinitionType().individuals(false).toList().stream().forEach(this::storeRuleDefinition); // FIXME: for some reason ruleEvalResources are also included here, need filtering out to avoid log warn messages
		factory.getResultBaseType().individuals().toList().stream().forEach(eval ->  // we need to have a list first as otherwise inference has concurrent modification exception
				loadAndStoreEvaluationWrapperFromModel(eval)
			);
	}
	
	public RuleEvaluationWrapperResource loadAndStoreEvaluationWrapperFromModel(OntIndividual eval) {
		try {
			var evalWrapper = RuleEvaluationFactory.loadFromModel(eval, factory, this);
			evaluations.put(evalWrapper.getRuleEvalObj().getURI(), evalWrapper);
			return evalWrapper;
		} catch (EvaluationException e) {
			log.warn("Error loading evaluation results from model, ignoring: "+e);
			return null;
		}
	}
	
	public Collection<RDFRuleDefinition> getRuleDefinitions() {
		return definitions.values();
	}
	
	public RuleDefinitionRegistrar getRuleBuilder() {
		return new RuleDefinitionRegistrar(factory, this);
	}
	
	/**
	 * @param def the RDFRuleDefinition to store/register, 
	 * no rule evaluations will be conducted at this time, as typically the model events/statements resulting from creating the definition will be processed via the RuleTriggerObserver
	 * if no such observer is running, then manual rule evaluation instances are created upon calling {@link #getRulesToEvaluateUponRuleDefinitionActivation(OntIndividual) }
	 */
	public void registerRuleDefinition(@NonNull RDFRuleDefinition def) {
		var key = def.getRuleDefinition().getURI();
		definitions.putIfAbsent(key, def);
	}
	
	public RDFRuleDefinition storeRuleDefinition(@NonNull OntObject definition) {	
		var key = definition.getURI();		
		return definitions.computeIfAbsent(key, k -> RuleDefinitionFactory.rebuildRDFRuleDefinitionImpl(definition, factory));	
	}
	
	public void removeRuleDefinition(@NonNull String ruleDefinitionURI) {
		removeRulesAffectedByDeletedRuleDefinition(ruleDefinitionURI, true);
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
				.filter(indiv -> evaluations.findEvaluation(indiv, def).isEmpty()) // ensure there is not already one eval wrapper 
				.map(ind -> RuleEvaluationFactory.create(factory, def, ind))							
				.map(eval -> { evaluations.put(eval.getRuleEvalObj().getURI(), eval); return eval;} )
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
			.map(Resource::getURI)
			.map(evaluations::get)			
			.filter(Objects::nonNull)						
			.collect(Collectors.toSet());
		// two step processing needed as otherwise concurrent modification in reasoner
		return toRemove.stream()			
			.map(eval -> { evaluations.remove(eval.getRuleEvalObj().getURI()); return eval;}) // remove evalwrapper from cache
			.map(eval -> { eval.delete(); return eval;}) // disable the eval wrapper
			.map(RuleEvaluationWrapperResource.class::cast)
			.collect(Collectors.toSet());
		
	}
	
	/**
	 * @param removeRuleDefinition whether to remove the rule definition or just the evaluations thereof
	 * @param def the RuleDefinition to remove including all evaluation instances thereof 
	 * @return all evaluation instances of that definition that now become stale and wont ever be reactivated because we remove their statements
	 */
	public Set<RuleEvaluationWrapperResource> removeRulesAffectedByDeletedRuleDefinition(@NonNull String definitionURI, boolean removeRuleDefinition) {
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
			if (removeRuleDefinition) {
				old.delete();
			}
			return filtered;
		} else {						
			return Collections.emptySet();
		}				
	}
	
	/**
	 * @param definitionURI to remove the corresponding rule definition, without considering rule evaluations or repairs, would need to be done separately
	 * used to clean up upon external deletion of underlying rdf statements.
	 */
	public void removeRuleDefinitionWrapper(@NonNull String definitionURI) {
		definitions.remove(definitionURI);
		//FIXME: how to remove rdf wrappers upon definition removal?! we dont know about these at this level
	}
	
	/**
	 * @param predicate the property definition that was removed, hence any rules (and their evaluations) that use this property are to be removed
	 */
	public void removeRuleDefinitionsAffectedByPredicateRemoval(Resource predicate) {
		// TODO we need to establish which rules make use of a property, this is not tracked at the moment
		
		
	}
	
	
	/**
	 * used upon deletion when definition type is already gone/no longer accessible
	 * */
	private Set<String> getRuleEvaluationIdsByDefinitionURI(String definitionURI) {
		Set<String> evals = new HashSet<>();
		var model = factory.getDefinitionType().getModel();		
		var def = model.createResource(definitionURI);
		var iter = model.listResourcesWithProperty(RDF.type, def);
		while(iter.hasNext()) {
			var res = iter.next();			
			if (res.getURI() != null)
				evals.add(res.getURI());
		}
		return evals;
	}
	
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByCreation(@NonNull OntIndividual newSubject) {
		// check if it has any existing scope, return also those, as a pessimistic caution as we dont know what the type changes imply	
		var reEval = getAllRuleEvaluationsThatUse(newSubject);			
		List <RuleEvaluationWrapperResource> ctxEval = getRuleEvaluationsWhereSubjectIsContext(newSubject).stream()
				.map(this::getOrWrapAndRegister)
				.filter(Objects::nonNull)
				.toList();
		
		// find definitions that have this as context		
		var types = newSubject.classes(false).collect(Collectors.toSet());
		reEval.addAll(
				definitions.values().stream().filter(def -> types.contains(def.getRDFContextType()))
				.filter(def -> !isSubjectContextOfRule(def, ctxEval)) // filter out if this subject is already context of that rule, which can happen upon type changes
				.map(def -> RuleEvaluationFactory.create(factory, def, newSubject))	
				.map(eval -> { evaluations.put(eval.getRuleEvalObj().getURI(), eval); return eval;} )
				.map(RuleEvaluationWrapperResource.class::cast).toList());			
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
				var evalResource = iterRule.next().getResource();				
				collectExistingOrNewEvaluationWrapper(evals, evalResource);
			}
		}				
		return evals;
	}

	private void collectExistingOrNewEvaluationWrapper(Set<RuleEvaluationWrapperResource> evals, Resource evalResource) {
		if (evaluations.containsKey(evalResource.getURI())) {
			evals.add(evaluations.get(evalResource.getURI()));
		} else {
			if (!evalResource.canAs(OntIndividual.class)) {
				log.warn("Found dangling reference to evaluation resource with URI: "+evalResource.getURI());
				// there seem to be some leftover evaluation references that no longer can be identified as an evaluation object 
				// and hence fail to be used as OntIndividual
			} else {
			try {
				var ruleRes = evalResource.as(OntIndividual.class);
				var evalObj = RuleEvaluationFactory.loadFromModel(ruleRes, factory, this);
				evaluations.put(evalObj.getRuleEvalObj().getURI(), evalObj);
				evals.add(evalObj);
			} catch (EvaluationException e) {
				log.warn("Error wrapping evaluation resource: "+e.getMessage());
				// ignored, check via logs how to improve scope handling issues
			}			
			}
		}
	}
	
	private boolean isSubjectContextOfRule(RDFRuleDefinition def, List <RuleEvaluationWrapperResource> ctxEval) {
		return ctxEval.stream().anyMatch(eval -> eval.getDefinition().equals(def));
	}
	
	public Set<RuleEvaluationWrapperResource> getRulesAffectedByChange(OntIndividual changedSubject, Property predicate) {
		// if this is a mapentry change -> do not need to support that, as rules dont support maps
		// if this is a list entry change --> find owner of list, and property between owner and this entry
		if (factory.getSchemaFactory().getListType().isListCollection(changedSubject)) {
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
					collectExistingOrNewEvaluationWrapper(evals, ruleRes);
				}
			}
		}				
		return evals;
	}
	
	private Entry<OntIndividual, Property> findListOwner(OntIndividual list) {		
		// first obtain the owner of the list
		var optOwner = factory.getSchemaFactory().getCurrentListOwner(list);
		// should only exist one such resource as we dont share lists across individuals
		if (optOwner.isEmpty()) {			
			return null;
		}
		var owner = optOwner.get();
		var commonProps = factory.getSchemaFactory().getListType().findListReferencePropertiesBetween(owner, list); // list is never removed, just stays empty
		if (commonProps.size() != 1) {
			return null;
		}
		var listProp = commonProps.get(0);
		return new AbstractMap.SimpleEntry<>(owner.as(OntIndividual.class), listProp);
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
		getRuleEvaluationsWhereSubjectIsContext(subject).stream()
			.map(this::getOrWrapAndRegister)
			.filter(Objects::nonNull)
			.filter(wrapper -> !isSubjectTypeMatchingRuleContext(wrapper, subject))
			.forEach(wrapper -> { evaluations.remove(wrapper.getRuleEvalObj().getURI()); wrapper.delete(); });				
		
		// TODO also look where the type info is used as property for casting or checking!!				
		// for now we reeval all
		return getAllRuleEvaluationsThatUse(subject);
	}
	
	private boolean isSubjectTypeMatchingRuleContext(RuleEvaluationWrapperResource wrapper,
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
	
	private RuleEvaluationWrapperResource getOrWrapAndRegister(OntIndividual eval) {		 
			var evalWrapper = evaluations.get(eval.getURI());
			if (evalWrapper == null) {
				try {				
					evalWrapper = RuleEvaluationFactory.loadFromModel(eval, factory, this);
					evaluations.put(eval.getURI(), evalWrapper);
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
					var evalObjWrapper = evaluations.remove(evalObj.getURI());
					if (evalObjWrapper == null) {						
						try { 
							evalObjWrapper = RuleEvaluationFactory.loadFromModel(evalObj, factory, this);
							// now we dont add to index here, as we remove these anyway before returning
						} catch (EvaluationException e) {
							// ignored, check via logs how to improve scope handling issues
						}													
					} 					
					evals.add(evalObjWrapper); //we delete evals further down 
				}				
			}			
		}
		// clean up scopes
		scopes.forEach(Resource::removeProperties);
		// delete eval wrapper
		evals.stream().filter(Objects::nonNull)
			.forEach(eval -> eval.delete());				
		return evals;
	}


	public Set<RuleEvaluationWrapperResource> removeRulesAffectedByDeletedRuleEvaluation(@NonNull String ruleEvalURI) {
		log.debug("Handling removal of rule evaluation object: "+ruleEvalURI);
		var eval = evaluations.remove(ruleEvalURI);
		if (eval == null) {
			return Collections.emptySet(); // we no longer have this stored anyway
		} else {
			eval.delete();
			return Set.of(eval);
		}
	}

	public static class EvaluationsCache{
		
		private final Map<String, RuleEvaluationWrapperResource> evaluationsByURI = new HashMap<>();
		private final Map<String, RuleEvaluationWrapperResource> indexByCtxAndDef = new HashMap<>();

		private RuleEvaluationWrapperResource remove(@NonNull String uri) {
			var eval = evaluationsByURI.remove(uri);
			if (eval != null) { // also remove from secondary index
				var key = makeKeyFrom(eval.getContextInstance(), eval.getDefinition());
				indexByCtxAndDef.remove(key);
			}
			return eval;
		}

		private boolean containsKey(String uri) {
			return evaluationsByURI.containsKey(uri);
		}

		public RuleEvaluationWrapperResource get(String uri) {
			return evaluationsByURI.get(uri);
		}

		private void put(String uri, RuleEvaluationWrapperResource evalWrapper) {
			evaluationsByURI.put(uri, evalWrapper);
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
		
		public Set<RuleEvaluationWrapperResource> getEvaluations() {
			return new HashSet<>(evaluationsByURI.values());
		}
	}




}

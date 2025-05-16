package at.jku.isse.artifacteventstreaming.rule;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluation;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Christoph Mayr-Dorn
 * the purpose of this class is to only update the rule repository and its evaluation wrappers due to changes to the underlying model
 * this class will not trigger the (re)evaluation of rules, but rather just notify which evaluation results have changed
 * it will therefore not detect rule evaluations that did not result in a changed rule and 
 * as such will not yield the information needed for evaluation analysis, for this: add a listener to an ActiveRuleTriggerObserver
 */
@Slf4j
public class PassiveRuleTriggerObserver extends AbstractRuleTriggerObserver {
	
	public PassiveRuleTriggerObserver(String serviceName, OntModel repoModel, RuleSchemaProvider ruleFactory, RuleRepository ruleRepo) {
		super(serviceName, repoModel, ruleFactory, ruleRepo);
	}
	
	protected void handleChanges(List<? extends Statement> removalScope, List<? extends Statement> additionScope) {
		// house keeping first, any rule removed, added, changed, or instance created/deleted that cause rule evaluations to be created or removed		
		// first we look at removals															
		Set<Resource> removedResourceURIs = determineRemovedResources(removalScope);// will contain any removed individuals that we dont care about their property changes 
		// then at additions
		Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> reevalutedRules = new HashMap<>();
		Set<Resource> createdResourceURIs = determineAddedResources(additionScope, reevalutedRules); // will contain any completely new individual that will trigger as context, but we can ignore their individual properties	
		
		// now we have the initial set of rule that are due to new instance as context, and we have a list of resources to ignore
		// now lets just merge the statements, filter out ignored ones, and then obtain rules to evaluate.		
		collectTriggeredRules(additionScope, Stream.concat(removedResourceURIs.stream(), createdResourceURIs.stream()).collect(Collectors.toSet()), reevalutedRules);
		// now lets update REIM metadata
		Set<RuleEvaluationIterationMetadata> reim = reevalutedRules.values().stream()
				.map(ruleMeta -> { 
					ruleMeta.getRule().reloadRepairNodes(); // outcome has changed, hence repair tree needs updating
					ruleMeta.update(true);
					return ruleMeta;
				})
				.collect(Collectors.toSet());
		log.debug(String.format("%s synced %s remotely changed rule evaluations within scope of '%s'", serviceName, reim.size(), super.commit.getCommitMessage()));
		listeners.stream().forEach(listener -> listener.signalRuleEvaluationFinished(reim));
	}
	
	/**
	 * @param statements
	 * @return set of URIs identifying rule definitions or evaluations that have been removed from the rule repository
	 */
	private  Set<Resource> determineRemovedResources(List<? extends Statement> statements) {		
		Set<Resource> removedResourceURIs = new HashSet<>();
		Map<Property, List<Statement>> aggrPerProp = statements.stream().collect(Collectors.groupingBy(Statement::getPredicate));
		aggrPerProp.getOrDefault(RDF.type, Collections.emptyList()).stream()
						.forEach(stmt -> removeDefinitionsOrEvaluations(stmt, removedResourceURIs)); // typically signals either rule removal, or instance removal, or instance retyping
		aggrPerProp.getOrDefault(RDFS.subClassOf, Collections.emptyList()).stream()
						.forEach(stmt -> removeDefinitionsOrEvaluations(stmt, removedResourceURIs)); 
		return removedResourceURIs;
	}
	
	private void removeDefinitionsOrEvaluations(Statement stmt, Set<Resource> removedResourceURIs) {
		// if removed type is a rule definition type --> removal of rule		
		var subject = stmt.getSubject();
		if (isAboutRuleDefinitionType(stmt)) {
			repo.removeRuleDefinitionWrapper(subject.getURI()); // any evaluation wrappers will be deleted separately
			removedResourceURIs.add(subject);
		} else if (isAboutRuleEvaluationType(stmt)) { // 
			repo.removeRulesAffectedByDeletedRuleEvaluation(subject.getURI()); // this is a side effect of a remote rule evaluation removal or rule type removal
			removedResourceURIs.add(subject);			
			// we can ignore repair nodes as they are accessed via the evaluation DTO
		} 
		//we dont care about any other type removal changes, if they affected rule definitons or evaluations, would have been handled above								
	}	
		
	/**
	 * @param statements
	 * @param reevalutedRules
	 * @return set of URIs identifying rule definitions or evaluation that have now been added to the rule repository
	 */
	private Set<Resource> determineAddedResources(List<? extends Statement> statements, Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> reevalutedRules) {
		Set<Resource> createdResourceURIs = new HashSet<>();
		Map<Property, List<Statement>> aggrPerProp = statements.stream().collect(Collectors.groupingBy(Statement::getPredicate));
		
		aggrPerProp.getOrDefault(RDF.type, Collections.emptyList()).stream()
			.forEach(stmt -> processTypeAddition(stmt, reevalutedRules, createdResourceURIs)); // typically signals either rule definition, or instance creation, or instance retyping
		aggrPerProp.getOrDefault(RDFS.subClassOf, Collections.emptyList()).stream()
			.forEach(stmt -> processTypeAddition(stmt, reevalutedRules, createdResourceURIs));
		// any changes to rule are available via ruledefinition wrapper and rule evaluation wrapper, no need to handle explicitly
		// if this change cause a reevaluation with repair change, then evaluation will have changed
		return createdResourceURIs;		
	}

	private void processTypeAddition(Statement stmt, Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> reevaluatedRules, Set<Resource> createdResourceURIs) {
		var res = stmt.getResource();
		if (res.getURI() != null && res.getURI().startsWith(OWL2.NS))
			return;

		var subject = stmt.getSubject();
		if (subject.canAs(OntClass.class)) {
			if (isAboutRuleDefinitionType(stmt)) { // a new rule was created
				repo.storeRuleDefinition(subject.as(OntObject.class));				
				//any evaluations are externally created hence no need to ask for them explicitly, see below
				createdResourceURIs.add(subject);				
			} 
		} else if (isAboutRuleEvaluationType(stmt)) {// if a rule evaluation object created, ignore that 
			// add this evaluation object to the repo
			var evalWrapper = repo.loadAndStoreEvaluationWrapperFromModel(subject.as(OntIndividual.class));
			reevaluatedRules.computeIfAbsent(evalWrapper, k -> new RuleEvaluationIterationMetadata(evalWrapper)).addOperation(stmt);
			createdResourceURIs.add(subject);		
			//we can ignore repair nodes as they are accessed via the evaluation DTO
		} else { 
			// any other individual created which we ignore
		}
	}
	
	private void collectTriggeredRules(List<? extends Statement> additionScope, Set<Resource> subjectsToIgnore, 
			Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> reevalutedRules) {		
		// here we are only interested in which rule evaluation outcome changes, ignoring newly added evaluations (as these are handled above)
		additionScope.stream()
			.filter(stmt -> !subjectsToIgnore.contains(stmt.getSubject()))
			.filter(stmt -> stmt.getPredicate().getURI().equals(RuleSchemaFactory.ruleHasConsistentResultURI)
					|| stmt.getPredicate().getURI().equals(RuleSchemaFactory.hasRepairNodesURI)) // only interested in changes to rule result or repair tree (to ensure we update the repair DTO accordingly
			.map(stmt -> repo.getEvaluations().get(stmt.getSubject().getURI()))
			.filter(Objects::nonNull)		
			.forEach(evalWrapper ->  reevalutedRules.computeIfAbsent(evalWrapper, k -> new RuleEvaluationIterationMetadata(evalWrapper)) );
	}
}

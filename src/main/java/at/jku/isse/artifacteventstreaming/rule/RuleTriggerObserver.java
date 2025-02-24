package at.jku.isse.artifacteventstreaming.rule;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluation;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleTriggerObserver extends AbstractHandlerBase implements IncrementalCommitHandler {
	
	@Getter private final RuleSchemaProvider factory;
	@Getter private final RuleRepository repo;
	private final LinkedList<RuleEvaluationListener> listeners = new LinkedList<>();
	
	public RuleTriggerObserver(String serviceName, OntModel repoModel, RuleSchemaProvider ruleFactory, RuleRepository ruleRepo) {
		super(serviceName, repoModel);
		this.factory = ruleFactory;
		this.repo = ruleRepo;
	}
	
	/**
	 * @param listener that wants to be informed what rules have been reevaluated due to what changes, even when the rule outcome has not changed
	 */
	public void registerListener(RuleEvaluationListener listener) { 
		if (!listeners.contains(listener))
			listeners.add(listener);
	}

	public static String getWellknownServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+RuleTriggerObserver.class.getSimpleName();
	}
	
	@Override
	protected String getServiceTypeURI() {
		return getWellknownServiceTypeURI();
	}
	
	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);		
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		super.logIncomingCommit(commit, indexOfNewAddition, indexOfNewRemoval);
		Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> rulesToReevaluate = new HashMap<>();		
		List<? extends Statement> removalScope = null;
		List<? extends Statement> additionScope = null;
		
		// house keeping first, any rule removed, added, changed, or instance created/deleted that cause rule evaluations to be created or removed		
		// first we look at removals									
		Set<Resource> removedResourceURIs = new HashSet<>(); // will contain any removed individuals that we dont care about their property changes 							
		var removals = commit.getRemovedStatements();
		if (indexOfNewRemoval < removals.size()) {
			removalScope = removals.subList(indexOfNewRemoval, removals.size());
			processRemovals(removalScope, removedResourceURIs);
		} else {
			removalScope = Collections.emptyList();
		}
		
		// then at additions
		Set<Resource> createdResourceURIs = new HashSet<>(); 		// will contain any completely new individual that will trigger as context, but we can ignore their individual properties	
		var additions = commit.getAddedStatements();
		if (indexOfNewAddition < additions.size()) {
			additionScope = additions.subList(indexOfNewAddition, additions.size());
			processAdditions(additionScope, rulesToReevaluate, createdResourceURIs);
		} else {
			additionScope = Collections.emptyList();
		}

		// now we have the initial set of rule that are due to new instance as context, and we have a list of resources to ignore
		// now lets just merge the statements, filter out ignored ones, and then obtain rules to evaluate.		
		collectRulesToTrigger(additionScope, removalScope, Stream.concat(removedResourceURIs.stream(), createdResourceURIs.stream()).collect(Collectors.toSet()), rulesToReevaluate);
		// now lets trigger reevaluation
		Set<RuleEvaluationIterationMetadata> reim = rulesToReevaluate.values().stream()
				.map(ruleMeta -> { 
					 Entry<RuleEvaluation, Boolean> update = ruleMeta.getRule().evaluate(); 
					 if (update != null) {
	                        ruleMeta.update(update); //update the metadata with whether the rule outcome changed and a reference to the underlying evaluation object
	                        return ruleMeta;
					 } else {
						 return null;
					 }						
				})
				.filter(Objects::nonNull)
                .collect(Collectors.toSet());
		log.debug(String.format("%s evaluated %s rules (+%s rule evaluation failures) ", serviceName, reim.size(), rulesToReevaluate.size() - reim.size()));
		listeners.stream().forEach(listener -> listener.signalRuleEvaluationFinished(reim));
	}
	
	private void processRemovals(List<? extends Statement> statements, Set<Resource> removedResourceURIs) {		
		Map<Property, List<Statement>> aggrPerProp = statements.stream().collect(Collectors.groupingBy(Statement::getPredicate));
		aggrPerProp.getOrDefault(RDF.type, Collections.emptyList()).stream()
						.forEach(stmt -> processTypeRemoval(stmt, removedResourceURIs)); // typically signals either rule removal, or instance removal, or instance retyping
		aggrPerProp.getOrDefault(RDFS.subClassOf, Collections.emptyList()).stream()
						.forEach(stmt -> processTypeRemoval(stmt, removedResourceURIs)); 
		
		aggrPerProp.getOrDefault(factory.getContextTypeProperty().asProperty(), Collections.emptyList()).stream()
						.forEach(stmt -> processRuleContextTypeOrExpressionRemoval(stmt, removedResourceURIs)); // rule has changed as hence we invalidate all old evaluations 
		aggrPerProp.getOrDefault(factory.getExpressionProperty().asProperty(), Collections.emptyList()).stream()
						.forEach(stmt -> processRuleContextTypeOrExpressionRemoval(stmt, removedResourceURIs));										
	}
	
	private void processTypeRemoval(Statement stmt, Set<Resource> removedResourceURIs) {
		// if removed type is a rule definition type --> removal of rule		
		var typeUri = stmt.getResource().getURI();
		var subject = stmt.getSubject();
		if (isAboutRuleDefinitionType(stmt)) {
			repo.removeRulesAffectedByDeletedRuleDefinition(subject.getURI(), true); // nothing to do with the returned rules eval that are no longer accessible because all related statements are removed
			removedResourceURIs.add(subject);
		} else if (isAboutRuleEvaluationType(stmt)) { // 
			repo.removeRulesAffectedByDeletedRuleEvaluation(subject.getURI()); // this is a side effect of a remote rule evaluation removal or rule type removal
			removedResourceURIs.add(subject);			
		} else if (isAboutOWLPropertySchemaChange(stmt)) { // a property has been removed, 
			repo.removeRuleDefinitionsAffectedByPredicateRemoval(stmt.getSubject());
		} else { //we assume just any other resource			
			// to distinguish between complete removal and only retyping, see if we can get an Ont individual from the uri
			if (subject.getURI() != null && typeUri != null) {// not an anonymous resource such as an ruleeval, neither a restriction as resource
				var indiv = factory.getDefinitionType().getModel().getIndividual(subject.getURI());
				if (indiv == null || isDefactoEmptyIndividual(indiv)) {// completely removed or not an ontindividual
					repo.getRemovedRulesAffectedByInstanceRemoval(stmt.getSubject());
					removedResourceURIs.add(subject);
				} else {
					repo.getRulesAffectedByTypeRemoval(indiv, typeUri);
				}			
			}
		}									
	}
	
	private boolean isAboutRuleDefinitionType(Statement stmt) {
		var typeUri = stmt.getResource().getURI();
		if (typeUri == null) return false;
		
		return ((typeUri.equals(factory.getDefinitionType().getURI()) 
			 && stmt.getPredicate().equals(RDF.type))
				|| 
			(typeUri.equals(factory.getResultBaseType().getURI())
			&& 	stmt.getPredicate().equals(RDFS.subClassOf) ));
	}
	
	private boolean isAboutOWLPropertySchemaChange(Statement stmt) { // when there is a change to properties of a class/type
		var uri = stmt.getResource().getURI();
		if (uri == null) return false;
		return uri.equals(OWL2.DatatypeProperty.getURI()) 
				|| uri.equals(OWL2.ObjectProperty.getURI());
	}
	
	private boolean isDefactoEmptyIndividual(OntIndividual indiv) {		
		var iter = indiv.listProperties();
		while(iter.hasNext()) {
			if (!iter.next().getPredicate().equals(RDF.type)) {				
				return false;
			}
		}
		return true;
	}
	
	private void processRuleContextTypeOrExpressionRemoval(Statement stmt, Set<Resource> removedResourceURIs) {
		var subject = stmt.getSubject();
		if (!removedResourceURIs.contains(subject)) {		
			repo.removeRulesAffectedByDeletedRuleDefinition(subject.getURI(), false);
			//removedResourceURIs.add(stmt.getSubject()) we are not removing the rule definition itself, just the evaluations!
		}
	}		
	
	private boolean isAboutRuleEvaluationType(Statement stmt) {
		var res = stmt.getResource();
		if (res.canAs(OntClass.class)) {
			return factory.getResultBaseType().hasSubClass(res.as(OntClass.class), true);
		}
		return false;
	}
	
	private void processAdditions(List<? extends Statement> statements, Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> rulesToReevaluate, Set<Resource> createdResourceURIs) {
		
		Map<Property, List<Statement>> aggrPerProp = statements.stream().collect(Collectors.groupingBy(Statement::getPredicate));
		
		aggrPerProp.getOrDefault(RDF.type, Collections.emptyList()).stream()
			.forEach(stmt -> processTypeAddition(stmt, rulesToReevaluate, createdResourceURIs)); // typically signals either rule definition, or instance creation, or instance retyping
		aggrPerProp.getOrDefault(RDFS.subClassOf, Collections.emptyList()).stream()
			.forEach(stmt -> processTypeAddition(stmt, rulesToReevaluate, createdResourceURIs));
		
		aggrPerProp.getOrDefault(factory.getContextTypeProperty().asProperty(), Collections.emptyList()).stream()
						.forEach(stmt -> processRuleContextTypeOrExpressionAddition(stmt, rulesToReevaluate, createdResourceURIs)); // rule has changed as hence we invalidate all old evaluations 
		aggrPerProp.getOrDefault(factory.getExpressionProperty().asProperty(), Collections.emptyList()).stream()
						.forEach(stmt -> processRuleContextTypeOrExpressionAddition(stmt, rulesToReevaluate, createdResourceURIs));
				
	}

	private void processTypeAddition(Statement stmt, Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> rulesToReevaluate, Set<Resource> createdResourceURIs) {
		var res = stmt.getResource();
		if (res.getURI() != null && res.getURI().startsWith(OWL2.NS))
			return;

		var subject = stmt.getSubject();
		if (subject.canAs(OntClass.class)) {
			if (isAboutRuleDefinitionType(stmt)) { // a new rule was created
				var def = repo.storeRuleDefinition(subject.as(OntObject.class));				
				addRulesToMetadata(rulesToReevaluate, repo.getRulesToEvaluateUponRuleDefinitionActivation(def), stmt); 
				createdResourceURIs.add(subject);				
			} 
		} else if (isAboutRuleEvaluationType(stmt)) {// if a rule evaluation object created, ignore that 
			createdResourceURIs.add(subject);		
		} else { 
			// regular individual  created
			if (subject.canAs(OntIndividual.class)) {
				addRulesToMetadata(rulesToReevaluate, repo.getRulesAffectedByCreation(subject.as(OntIndividual.class)), stmt);
			} // in any case, ignore further on
		}
	}
	
	private void processRuleContextTypeOrExpressionAddition(Statement stmt, Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> rulesToReevaluate, Set<Resource> createdResourceURIs) {
		var subject = stmt.getSubject();
		if (!createdResourceURIs.contains(subject) && subject instanceof OntObject ontObj) {					
			var def = repo.storeRuleDefinition(ontObj);			
			addRulesToMetadata(rulesToReevaluate, repo.getRulesToEvaluateUponRuleDefinitionActivation(def), stmt); 
			createdResourceURIs.add(subject);
		}
	}
	
	private void addRulesToMetadata(Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> rulesMetadata, Set<RuleEvaluationWrapperResource> wrappers, Statement cause) {
		wrappers.stream().forEach(rule -> rulesMetadata.computeIfAbsent(rule, k -> new RuleEvaluationIterationMetadata(rule)).addOperation(cause));
	}
	
	private void collectRulesToTrigger(List<? extends Statement> additionScope, List<? extends Statement> removalScope, Set<Resource> subjectsToIgnore, Map<RuleEvaluationWrapperResource, RuleEvaluationIterationMetadata> rulesToReevaluate) {		
		
		additionScope.stream()
			.filter(stmt -> !subjectsToIgnore.contains(stmt.getSubject()))
			.filter(stmt -> !stmt.getPredicate().getURI().startsWith(RuleSchemaFactory.uri)) // not interested in changes to the rule housekeeping itself 
			.filter(stmt -> stmt.getSubject().canAs(OntIndividual.class))
			.forEach(stmt ->  addRulesToMetadata(rulesToReevaluate, repo.getRulesAffectedByChange(stmt.getSubject().as(OntIndividual.class), stmt.getPredicate()), stmt));
		removalScope.stream()
			.filter(stmt -> !subjectsToIgnore.contains(stmt.getSubject()))
			.filter(stmt -> !stmt.getPredicate().getURI().startsWith(RuleSchemaFactory.uri)) // not interested in changes to the rule housekeeping itself
			.filter(stmt -> stmt.getSubject().canAs(OntIndividual.class))
			.forEach(stmt ->  addRulesToMetadata(rulesToReevaluate, repo.getRulesAffectedByChange(stmt.getSubject().as(OntIndividual.class), stmt.getPredicate()), stmt));	
	}
}

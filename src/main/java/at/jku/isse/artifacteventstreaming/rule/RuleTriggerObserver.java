package at.jku.isse.artifacteventstreaming.rule;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleTriggerObserver extends AbstractHandlerBase implements IncrementalCommitHandler {

	private final RuleFactory factory;
	private final RuleRepository repo;
	
	public RuleTriggerObserver(String serviceName, OntModel repoModel, RuleFactory ruleFactory, RuleRepository ruleRepo) {
		super(serviceName, repoModel);
		this.factory = ruleFactory;
		this.repo = ruleRepo;
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
		Set<RuleEvaluationWrapperResource> rulesToReevaluate = new HashSet<>();
		
		// will contain any removed individuals that we dont care about their property changes
		// or any completely new individual that will trigger as context, but we can ignore their individual properties			
		Stream<Resource> removedResourceURIs = Stream.empty(); 
		Stream<Resource> createdResourceURIs = Stream.empty();		
		List<Statement> removalScope = null;
		List<Statement> additionScope = null;
		
		// first we look at removals		
		var removals = commit.getRemovedStatements();
		if (indexOfNewRemoval < removals.size()) {
			removalScope = removals.subList(indexOfNewRemoval, removals.size());
			removedResourceURIs = processRemovals(removalScope);
		} else {
			removalScope = Collections.emptyList();
		}
		
		// then at additions
		var additions = commit.getAddedStatements();
		if (indexOfNewAddition < additions.size()) {
			additionScope = additions.subList(indexOfNewAddition, additions.size());
			createdResourceURIs = processAdditions(additionScope, rulesToReevaluate);
		} else {
			additionScope = Collections.emptyList();
		}
		// now we have the initial set of rule that are due to new instance as context, and we have a list of resources to ignore
		// now lets just merge the statements, filter out ignored ones, and then obtain rules to evaluate.		
		collectRulesToTrigger(additionScope, removalScope, Stream.concat(removedResourceURIs, createdResourceURIs).collect(Collectors.toSet()), rulesToReevaluate);
		// now lets trigger reevaluation
		rulesToReevaluate.forEach(rule -> rule.evaluate());
		//TODO: reintroduce rule result change tracking via RuleEvaluationIterationMetadata
	}

	private Stream<Resource> processAdditions(List<Statement> statements, Set<RuleEvaluationWrapperResource> rulesToReevaluate) {
		return statements.stream()
		.filter(stmt -> stmt.getPredicate().equals(RDF.type) || stmt.getPredicate().equals(RDFS.subClassOf) ) // typically signals either rule definition, or instance creation, or instance retyping
		.map(stmt -> processTypeAddition(stmt, rulesToReevaluate))
		.filter(Objects::nonNull);
	}

	private Resource processTypeAddition(Statement stmt, Set<RuleEvaluationWrapperResource> rulesToReevaluate) {
		var res = stmt.getResource();
		if (res.getURI().startsWith(OWL2.NS))
			return null;
		
		var subject = stmt.getSubject();
		if (res.canAs(OntClass.class)) {
			var ontClass = res.as(OntClass.class);			
			if (ontClass.equals(factory.getDefinitionType()) || factory.getResultBaseType().hasSubClass(ontClass, true)) { // a new rule was created
				var def = repo.storeRuleDefinition(subject.as(OntIndividual.class));
				rulesToReevaluate.addAll(repo.getRulesToEvaluateUponRuleDefinitionActivation(def));
				return subject;
			} // else ignore any other type creation
		} 
		// regular individual  created
		if (subject.canAs(OntIndividual.class)) {
			// if a rule evaluation object created, ignore that 
			var indiv = subject.as(OntIndividual.class);
			if (indiv.hasOntClass(factory.getResultBaseType(), true)) {
				return subject;
			} else {	
				rulesToReevaluate.addAll(repo.getRulesAffectedByCreation(subject.as(OntIndividual.class)));
			}
		} // in any case, ignore further on				
		return null;
	}
	
	private Stream<Resource> processRemovals(List<Statement> statements) {		
		return statements.stream()
		.filter(stmt -> stmt.getPredicate().equals(RDF.type) || stmt.getPredicate().equals(RDFS.subClassOf) ) // typically signals either rule removal, or instance removal, or instance retyping
		.map(this::processTypeRemoval)
		.filter(Objects::nonNull);							
	}
	
	private Resource processTypeRemoval(Statement stmt) {
		// if removed type is a rule definition type --> removal of rule		
		var typeUri = stmt.getResource().getURI();
		var subject = stmt.getSubject();
		if (isAboutRuleDefinitionType(stmt)) {
			repo.removeRulesAffectedByDeletedRuleDefinition(subject.getURI()); // nothing to do with the returned rules eval that are no longer accessible because all related statements are removed
			return subject;
		} else if (isAboutRuleEvaluationType(stmt)) { // 
			repo.removeRulesAffectedByDeletedRuleEvaluation(subject.getId()); // this is a side effect of a remote rule evaluation removal or rule type removal
			return subject;
		} else { //we assume just any other resource			
			// to distinguish between complete removal and only retying, see if we can get an Ont individual from the uri
			if (subject.getURI() != null) {// not an anonymous resource such as an ruleeval
				var indiv = factory.getDefinitionType().getModel().getIndividual(subject.getURI());
				if (indiv == null || isDefactoEmptyIndividual(indiv)) {// completely removed or not an ontindividual
					repo.getRemovedRulesAffectedByInstanceRemoval(stmt.getSubject());
					return subject;
				} else {
					repo.getRulesAffectedByTypeRemoval(indiv, typeUri);
				}			
			}
		}
		return null;										
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
	
	private boolean isAboutRuleDefinitionType(Statement stmt) {
		var typeUri = stmt.getResource().getURI();
		return ((typeUri.equals(factory.getDefinitionType().getURI()) 
			 && stmt.getPredicate().equals(RDF.type))
				|| 
			(typeUri.equals(factory.getResultBaseType().getURI())
			&& 	stmt.getPredicate().equals(RDFS.subClassOf) ));
	}
	
	private boolean isAboutRuleEvaluationType(Statement stmt) {
		var res = stmt.getResource();
		if (res.canAs(OntClass.class)) {
			return factory.getResultBaseType().hasSubClass(res.as(OntClass.class), true);
		}
		return false;
	}
	
	private void collectRulesToTrigger(List<Statement> additionScope, List<Statement> removalScope, Set<Resource> subjectsToIgnore, Set<RuleEvaluationWrapperResource> rulesToReevaluate) {		
		additionScope.stream()
			.filter(stmt -> !subjectsToIgnore.contains(stmt.getSubject()))
			.filter(stmt -> !stmt.getPredicate().getURI().startsWith(RuleFactory.uri)) // not interested in changes to the rule housekeeping itself 
			.filter(stmt -> stmt.getSubject().canAs(OntIndividual.class))
			.forEach(stmt -> rulesToReevaluate.addAll(repo.getRulesAffectedByChange(stmt.getSubject().as(OntIndividual.class), stmt.getPredicate())));
		removalScope.stream()
			.filter(stmt -> !subjectsToIgnore.contains(stmt.getSubject()))
			.filter(stmt -> !stmt.getPredicate().getURI().startsWith(RuleFactory.uri)) // not interested in changes to the rule housekeeping itself
			.filter(stmt -> stmt.getSubject().canAs(OntIndividual.class))
			.forEach(stmt -> rulesToReevaluate.addAll(repo.getRulesAffectedByChange(stmt.getSubject().as(OntIndividual.class), stmt.getPredicate())));	
	}
}

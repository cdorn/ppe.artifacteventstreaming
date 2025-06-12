package at.jku.isse.artifacteventstreaming.rule;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleEvaluationDTO;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RuleRepositoryInspector {

	private final RuleSchemaProvider schemaProvider;
	
	public Set<OntIndividual> getAllScopes() {		
		return schemaProvider.getRuleScopeCollection().individuals().collect(Collectors.toSet());
	}
	
	public Resource getElementFromScope(Resource scope) {
		return scope.getPropertyResourceValue(schemaProvider.getUsingElementProperty().asProperty());
	}
	
	protected Set<OntIndividual> getScopes(OntIndividual subject, String... limitedToProps){
		var scopes = new HashSet<OntIndividual>();
		List<String> props = Arrays.asList(limitedToProps);
		var iter = subject.listProperties(schemaProvider.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var scope = stmt.getResource().as(OntIndividual.class);
			if (!props.isEmpty()) { // only specific props are considered				
				var property = scope.getPropertyResourceValue(schemaProvider.getUsingPredicateProperty().asProperty());
				if (property == null || !props.contains(property.getURI()))
					continue;	// early iteration abort and check next scope object
			}
			scopes.add(scope);
		}
		return scopes;
	}
	
	public  int getEvalCountFromScope(OntIndividual scope) {
		var evals = new HashSet<OntIndividual>();
		var iterRule = scope.listProperties(schemaProvider.getUsedInRuleProperty().asProperty());
		while(iterRule.hasNext()) { //property
			var ruleRes = iterRule.next().getResource().as(OntIndividual.class);
			evals.add(ruleRes);
		}
		return evals.size();
	}
	
	public Set<OntIndividual> getEvalWrappersFromScopes(OntIndividual subject, String... limitedToProps){
		var evals = new HashSet<OntIndividual>();
		List<String> props = Arrays.asList(limitedToProps);
		var iter = subject.listProperties(schemaProvider.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var scope = stmt.getResource().as(OntIndividual.class);
			if (!props.isEmpty()) { // only specific props are considered				
				var property = scope.getPropertyResourceValue(schemaProvider.getUsingPredicateProperty().asProperty());
				if (property == null || !props.contains(property.getURI()))
					continue;	// early iteration abort and check next scope object
			}
			var iterRule = scope.listProperties(schemaProvider.getUsedInRuleProperty().asProperty());
			while(iterRule.hasNext()) { //property
				var ruleRes = iterRule.next().getResource().as(OntIndividual.class);
				evals.add(ruleRes);
			}
		}
		return evals;
	}
	
	public void printScope(OntIndividual scope) {
		var inst = scope.getPropertyResourceValue(schemaProvider.getUsingElementProperty().asProperty());
		var instName = inst != null ? inst.getLocalName() : "NULL_INSTANCE";
		var property = scope.getPropertyResourceValue(schemaProvider.getUsingPredicateProperty().asProperty());
		var propName = property != null ? property.getLocalName() : "NULL_PROPERTY";
		var iterRule = scope.listProperties(schemaProvider.getUsedInRuleProperty().asProperty());
		Set<String> evals = new HashSet<>();
		while(iterRule.hasNext()) { //property
			//var ruleRes = iterRule.next().getResource().as(OntIndividual.class);
			evals.add(iterRule.next().getResource().getURI());
		}
		System.out.println(String.format("Scope: %s with property %s in %s rule evals: %s", instName, propName, evals.size(), evals.toString() ));
	}
	
	public void printIndividualScopeProperties() {
		var iter = schemaProvider.getRuleScopeCollection().getModel().listResourcesWithProperty(schemaProvider.getUsingPredicateProperty().asProperty());
		while (iter.hasNext()) {
			var res = iter.next();
			System.out.println(String.format("Scope %s has usingPredicate property", res.getId()));
		}
		
		iter = schemaProvider.getRuleScopeCollection().getModel().listResourcesWithProperty(schemaProvider.getUsingElementProperty().asProperty());
		while (iter.hasNext()) {
			var res = iter.next();
			System.out.println(String.format("Scope %s has usingElement property", res.getId()));
		}
		
		iter = schemaProvider.getRuleScopeCollection().getModel().listResourcesWithProperty(schemaProvider.getUsedInRuleProperty().asProperty());
		while (iter.hasNext()) {
			var res = iter.next();
			System.out.println(String.format("Scope %s has usedInRule property", res.getId()));
		}
	}
	
	public void printAllRuleEvalElements() {
		schemaProvider.getResultBaseType().individuals().forEach(indiv -> {
			var name = indiv.getLabel() != null ? indiv.getLabel() : "NULL LABEL";
			var resultStmt = indiv.getProperty(schemaProvider.getEvaluationHasConsistentResultProperty());
			var result = resultStmt != null ? resultStmt.getObject().toString() : "NO RESULT";
			var errorStmt = indiv.getProperty(schemaProvider.getEvaluationErrorProperty());
			var error = errorStmt != null ? errorStmt.getObject().toString() : "NONE";
			var iterscope = indiv.listProperties(schemaProvider.getHavingScopePartProperty().asProperty());
			Set<String> scopes = new HashSet<>();
			while(iterscope.hasNext()) { //property				
				var scope = iterscope.next().getResource();
				var inst = scope.getPropertyResourceValue(schemaProvider.getUsingElementProperty().asProperty());
				var instName = inst != null ? inst.getLocalName() : "NULL_INSTANCE";
				scopes.add(scope.getId().toString()+"->"+instName);			
			}
			System.out.println(String.format("RuleEval: %s consistent %s error %s with %s scopes %s", name, result, error, scopes.size(), scopes.toString()));
		});
	}
	
	public Set<RepairNodeDTO> getFlatRepairTree(RuleEvaluationDTO ruleEvalDTO) {
		var iter = ruleEvalDTO.getRuleEvalObj().listProperties(schemaProvider.getHasRepairNodesProperty().asProperty());
		var nodes = new HashSet<RepairNodeDTO>();
		while(iter.hasNext()) {
			nodes.add(new RepairNodeDTO(iter.next().getResource().as(OntIndividual.class), schemaProvider));
		}
		return nodes;
	}
}

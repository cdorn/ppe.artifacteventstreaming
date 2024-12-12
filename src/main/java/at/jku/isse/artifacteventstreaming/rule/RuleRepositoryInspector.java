package at.jku.isse.artifacteventstreaming.rule;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RuleRepositoryInspector {

	private final RuleSchemaProvider factory;
	
	public Set<OntIndividual> getAllScopes() {		
		return factory.getRuleScopeCollection().individuals().collect(Collectors.toSet());
	}
	
	protected Set<OntIndividual> getScopes(OntIndividual subject, String... limitedToProps){
		var scopes = new HashSet<OntIndividual>();
		List<String> props = Arrays.asList(limitedToProps);
		var iter = subject.listProperties(factory.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var scope = stmt.getResource().as(OntIndividual.class);
			if (!props.isEmpty()) { // only specific props are considered				
				var property = scope.getPropertyResourceValue(factory.getUsingPredicateProperty().asProperty());
				if (property == null || !props.contains(property.getURI()))
					continue;	// early iteration abort and check next scope object
			}
			scopes.add(scope);
		}
		return scopes;
	}
	
	public  int getEvalCountFromScope(OntIndividual scope) {
		var evals = new HashSet<OntIndividual>();
		var iterRule = scope.listProperties(factory.getUsedInRuleProperty().asProperty());
		while(iterRule.hasNext()) { //property
			var ruleRes = iterRule.next().getResource().as(OntIndividual.class);
			evals.add(ruleRes);
		}
		return evals.size();
	}
	
	public Set<OntIndividual> getEvalWrappersFromScopes(OntIndividual subject, String... limitedToProps){
		var evals = new HashSet<OntIndividual>();
		List<String> props = Arrays.asList(limitedToProps);
		var iter = subject.listProperties(factory.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var scope = stmt.getResource().as(OntIndividual.class);
			if (!props.isEmpty()) { // only specific props are considered				
				var property = scope.getPropertyResourceValue(factory.getUsingPredicateProperty().asProperty());
				if (property == null || !props.contains(property.getURI()))
					continue;	// early iteration abort and check next scope object
			}
			var iterRule = scope.listProperties(factory.getUsedInRuleProperty().asProperty());
			while(iterRule.hasNext()) { //property
				var ruleRes = iterRule.next().getResource().as(OntIndividual.class);
				evals.add(ruleRes);
			}
		}
		return evals;
	}
	
	public void printScope(OntIndividual scope) {
		var inst = scope.getPropertyResourceValue(factory.getUsingElementProperty().asProperty());
		var instName = inst != null ? inst.getLocalName() : "NULL_INSTANCE";
		var property = scope.getPropertyResourceValue(factory.getUsingPredicateProperty().asProperty());
		var propName = property != null ? property.getLocalName() : "NULL_PROPERTY";
		var iterRule = scope.listProperties(factory.getUsedInRuleProperty().asProperty());
		Set<String> evals = new HashSet<>();
		while(iterRule.hasNext()) { //property
			//var ruleRes = iterRule.next().getResource().as(OntIndividual.class);
			evals.add(iterRule.next().getResource().getId().toString());
		}
		System.out.println(String.format("Scope: %s with property %s in rule evals: %s", instName, propName, evals.toString() ));
	}
	
}

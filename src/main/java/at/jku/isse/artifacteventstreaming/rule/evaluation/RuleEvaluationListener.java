package at.jku.isse.artifacteventstreaming.rule.evaluation;

import java.util.Set;

public interface RuleEvaluationListener {

	
	public void signalRuleEvaluationFinished(Set<RuleEvaluationIterationMetadata> iterationMetadata);
	
	
}

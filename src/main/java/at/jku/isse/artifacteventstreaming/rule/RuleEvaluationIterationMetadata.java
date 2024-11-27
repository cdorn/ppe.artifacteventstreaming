package at.jku.isse.artifacteventstreaming.rule;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.rdf.model.Statement;

import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluation;
import at.jku.isse.designspace.rule.model.Rule;
import lombok.Data;

/**
 * @author Christoph Mayr-Dorn
 * 
 * Collects all the operations that triggered a rule evaluation, 
 * and whether that re-evaluation caused a different rule fulfillment outcome
 */
@Data
public class RuleEvaluationIterationMetadata {

	final RuleEvaluationWrapperResource rule;
	@SuppressWarnings("rawtypes")
	RuleEvaluation eval;
	Boolean hasEvaluationOutcomeChanged;
	Set<Statement> evaluationTriggers = new HashSet<>();
	
	public void addOperation(Statement op) {
		evaluationTriggers.add(op);
	}
	
	public void addOperations(Collection<Statement> ops ) {
		evaluationTriggers.addAll(ops);
	}
	
	public void update(Entry<RuleEvaluation, Boolean> evalOutcome) {
		if (evalOutcome != null) {
			this.eval = evalOutcome.getKey();
			this.hasEvaluationOutcomeChanged = evalOutcome.getValue();
		}
	}
}

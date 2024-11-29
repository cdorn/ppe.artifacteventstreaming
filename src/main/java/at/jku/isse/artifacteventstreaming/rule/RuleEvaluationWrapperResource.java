package at.jku.isse.artifacteventstreaming.rule;

import java.util.Map.Entry;

import org.apache.jena.ontapi.model.OntIndividual;

import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluation;
import at.jku.isse.designspace.rule.arl.repair.RepairNode;

public interface RuleEvaluationWrapperResource {

	public Entry<RuleEvaluation, Boolean> evaluate();
	public boolean isConsistent();	
	public String getEvaluationError();
	public Object getEvaluationResult();
	public OntIndividual getContextInstance();
	public void enable();
	public void disable();
	boolean isEnabled();
	public void delete();
	public RepairNode getRepairTree();
}

package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntIndividual;

public interface RuleEvaluationWrapperResource {

	public Object evaluate();
	public boolean isConsistent();	
	public String getEvaluationError();
	public Object getEvaluationResult();
	public OntIndividual getContextInstance();
	public void enable();
	public void disable();
	boolean isEnabled();
	public void delete();
}

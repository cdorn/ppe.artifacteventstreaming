package at.jku.isse.passiveprocessengine.rdfwrapper.rule;

import java.util.Map;
import java.util.Set;

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import lombok.Data;

public interface RuleEvaluationService extends RuleDefinitionService {

	/*
	 * side-effect free, will not generate a persisted rule definition, 
	 * nor any rule evaluations that show up in the artifacts
	 * */
	public Set<ResultEntry> evaluateTransientRule(RDFInstanceType type, String constraint) throws Exception;

	public boolean isRuleCorrect(RDFInstanceType type, String constraint) throws Exception;
	
	public Set<ResultEntry> getEvaluationResults(RDFRuleDefinitionWrapper rule) throws Exception;

	public Map<RDFRuleDefinitionWrapper, Set<ResultEntry>> getEvaluationResultsWithInstanceInScope(RDFInstance instance) throws Exception;
	
	@Data
	public class ResultEntry {
		final RDFInstance contextInstance;
		final Boolean result;
		final String error;	   
		final Object evalTreeRootNode;
		final Object repairTreeRootNode;	    	
	}
}

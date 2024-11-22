package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntClass;

import at.jku.isse.designspace.rule.arl.evaluator.RuleDefinition;
import at.jku.isse.designspace.rule.arl.expressions.Expression;

public interface RDFRuleDefinition extends RuleDefinition<OntClass>{
	
	public String getRuleExpression();
	public void setRuleExpression(String expression);
	public String getExpressionError();
	public boolean hasExpressionError();
	public void setDescription(String description);
	public String getDescription();
	public OntClass getRDFContextType();
	Expression<Object> getSyntaxTree();
	void setName(String title);
	String getName();
	OntClass getRuleDefinition();
}


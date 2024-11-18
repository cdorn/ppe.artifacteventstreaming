package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntClass;

import at.jku.isse.designspace.rule.arl.expressions.Expression;

public interface RuleDefinition {
	
	public String getRuleExpression();
	public void setRuleExpression(String expression);
	public String getExpressionError();
	public boolean hasExpressionError();
	public void setDescription(String description);
	public String getDescription();
	public OntClass getContextType();
	Expression<Object> getSyntaxTree();
	void setTitle(String description);
	String getTitle();
}


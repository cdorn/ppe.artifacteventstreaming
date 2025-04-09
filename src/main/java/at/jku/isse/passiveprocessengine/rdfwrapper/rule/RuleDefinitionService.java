package at.jku.isse.passiveprocessengine.rdfwrapper.rule;

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;

public interface RuleDefinitionService {

	public RDFRuleDefinitionWrapper createInstance(RDFInstanceType type, String ruleName, String ruleExpression);
	
	public void setPropertyRepairable(RDFInstanceType type, String property, boolean isRepairable);
	
	public boolean isPropertyRepairable(RDFInstanceType type, String property);

	public RDFRuleDefinitionWrapper findByInternalId(String id);
}

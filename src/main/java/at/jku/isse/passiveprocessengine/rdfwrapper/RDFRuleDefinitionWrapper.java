package at.jku.isse.passiveprocessengine.rdfwrapper;

import at.jku.isse.artifacteventstreaming.rule.RDFRuleDefinition;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.RuleDefinition;
import lombok.Getter;

public class RDFRuleDefinitionWrapper extends RDFInstanceType {

	@Getter
	private final RDFRuleDefinition ruleDef;	
	
	public RDFRuleDefinitionWrapper(RDFRuleDefinition element, RuleEnabledResolver resolver) {
		super(element.getRuleDefinition(), resolver);
		this.ruleDef = element;
		super.cacheSuperProperties();
	}

	@Override
	public void delete() {
		var ruleResolver = (RuleEnabledResolver)resolver;
		ruleResolver.removeRuleDefinition(this);
	}
	
}

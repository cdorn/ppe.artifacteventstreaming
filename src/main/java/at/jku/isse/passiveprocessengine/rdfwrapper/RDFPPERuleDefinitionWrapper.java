package at.jku.isse.passiveprocessengine.rdfwrapper;

import at.jku.isse.artifacteventstreaming.rule.RDFRuleDefinition;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.RuleDefinition;
import lombok.Getter;

public class RDFPPERuleDefinitionWrapper extends RDFInstanceType implements RuleDefinition {

	@Getter
	private final RDFRuleDefinition ruleDef;	
	
	public RDFPPERuleDefinitionWrapper(RDFRuleDefinition element, RuleEnabledResolver resolver) {
		super(element.getRuleDefinition(), resolver);
		this.ruleDef = element;
		super.cacheSuperProperties();
	}

	@Override
	public boolean hasRuleError() {
		return ruleDef.hasExpressionError();
	}

	@Override
	public String getRuleError() {
		return ruleDef.getExpressionError();
	}

	@Override
	public String getRuleExpression() {
		return ruleDef.getRuleExpression();
	}

	@Override
	public PPEInstanceType getContext() {
		return resolver.resolveToType(ruleDef.getRDFContextType());
	}

	@Override
	public void markAsDeleted() {
		var ruleResolver = (RuleEnabledResolver)resolver;
		ruleResolver.removeRuleDefinition(this);
	}
	
}

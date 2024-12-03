package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.Map;
import java.util.Set;

import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.RuleDefinition;
import at.jku.isse.passiveprocessengine.core.RuleEvaluationService;

public class RuleEnabledResolver extends NodeToDomainResolver implements RuleEvaluationService {

	final RepairService repairService;
	
	public RuleEnabledResolver(OntModel model, RepairService repairService) {
		super(model);
		this.repairService = repairService;
	}

	@Override
	public RuleDefinition createInstance(PPEInstanceType type, String ruleName, String ruleExpression) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setPropertyRepairable(PPEInstanceType type, String property, boolean isRepairable) {
		if (type instanceof RDFInstanceType rdfType) {
			rdfType.resolveToPropertyType(property).ifPresent(prop ->
				repairService.setPropertyRepairable(rdfType.getType(), prop.getProperty().asProperty(), isRepairable));			
		} else {
			throw new RuntimeException("Expected RDFInstanceType but received "+type.getClass());
		}
	}

	@Override
	public RuleDefinition findByInternalId(String id) {
		var indiv = model.getIndividual(id);
		if (indiv != null)
			return super.instanceIndex.get(indiv);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<ResultEntry> evaluateTransientRule(PPEInstanceType type, String constraint) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isRuleCorrect(PPEInstanceType type, String constraint) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Set<ResultEntry> getEvaluationResults(RuleDefinition rule) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<RuleDefinition, Set<ResultEntry>> getEvaluationResultsWithInstanceInScope(PPEInstance instance)
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	
	
}

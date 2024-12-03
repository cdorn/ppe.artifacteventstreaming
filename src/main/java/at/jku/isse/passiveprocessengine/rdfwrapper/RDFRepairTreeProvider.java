package at.jku.isse.passiveprocessengine.rdfwrapper;

import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleEvaluationWrapperResourceImpl;
import at.jku.isse.artifacteventstreaming.rule.RuleTriggerObserver;
import at.jku.isse.passiveprocessengine.core.PPEExecutedRepairListener;
import at.jku.isse.passiveprocessengine.core.RepairTreeProvider;
import at.jku.isse.passiveprocessengine.core.RuleResult;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RDFRepairTreeProvider implements RepairTreeProvider {

	final RepairService repairService;
	final AbstractionMapper mapper;
	final RuleTriggerObserver observer;
	
	@Override
	public Object getRepairTree(RuleResult ruleResult) {
		if (ruleResult instanceof RuleEvaluationWrapperResourceImpl rdfResult) {			
			return repairService.getRepairRootNode( rdfResult.getContextInstance(), rdfResult.getDefinition());
		} else {
			throw new RuntimeException("Expected RuleEvaluationWrapperResourceImpl but received: "+ruleResult.getClass());
		}
	}

	@Override
	public Object getEvaluationNode(RuleResult ruleResult) {
		if (ruleResult instanceof RuleEvaluationWrapperResourceImpl rdfResult) {			
			return rdfResult.getDelegate().getEvaluationTree();
		} else {
			throw new RuntimeException("Expected RuleEvaluationWrapperResourceImpl but received: "+ruleResult.getClass());
		}
	}

	@Override
	public void register(PPEExecutedRepairListener listener) {
		throw new RuntimeException("NotSupported");
	}

}

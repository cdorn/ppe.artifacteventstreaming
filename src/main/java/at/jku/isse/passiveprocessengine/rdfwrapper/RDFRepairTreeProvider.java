package at.jku.isse.passiveprocessengine.rdfwrapper;

import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleEvaluationWrapperResourceImpl;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleTriggerObserver;
import at.jku.isse.passiveprocessengine.core.PPEExecutedRepairListener;
import at.jku.isse.passiveprocessengine.core.RepairTreeProvider;
import at.jku.isse.passiveprocessengine.core.RuleAnalysisService;
import at.jku.isse.passiveprocessengine.core.RuleResult;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RDFRepairTreeProvider implements RepairTreeProvider, RuleAnalysisService {

	final RepairService repairService;
	final RuleRepository ruleRepo;
	final AbstractionMapper mapper;
	final RuleTriggerObserver observer;
	
	@Override
	public Object getRepairTree(RuleResult ruleResult) {
		if (ruleResult instanceof RDFRuleResultWrapper rdfResult) {		
			var ontClass = ((RDFInstanceType)rdfResult.getInstanceType()).type;
			var def = ruleRepo.findRuleDefinitionForResource(ontClass);
			return repairService.getRepairRootNode( ((RDFInstance)rdfResult.getContextInstance()).getElement(), def);
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

	@Override
	public OverrideAnalysisSession createSession() {
		throw new RuntimeException("Not Supported");
	}

}

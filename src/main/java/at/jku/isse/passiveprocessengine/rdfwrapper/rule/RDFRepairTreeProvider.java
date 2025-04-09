package at.jku.isse.passiveprocessengine.rdfwrapper.rule;

import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleEvaluationWrapperResourceImpl;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleTriggerObserver;
import at.jku.isse.passiveprocessengine.core.PPEExecutedRepairListener;
import at.jku.isse.passiveprocessengine.core.RepairTreeProvider;
import at.jku.isse.passiveprocessengine.core.RuleAnalysisService;
import at.jku.isse.passiveprocessengine.core.RuleResult;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RDFRepairTreeProvider implements RepairTreeProvider, RuleAnalysisService {

	final RepairService repairService;
	final RuleRepository ruleRepo;	
	
	@Override
	public Object getRepairTree(RuleResult ruleResult) {
		if (ruleResult instanceof RDFRuleResultWrapper rdfResult) {		
			var ontClass = rdfResult.getInstanceType().getType();
			var def = ruleRepo.findRuleDefinitionForResource(ontClass);
			return repairService.getRepairRootNode( rdfResult.getContextInstance().getElement(), def);
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

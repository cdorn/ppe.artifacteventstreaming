package at.jku.isse.passiveprocessengine.rdfwrapper.rule;

import at.jku.isse.artifacteventstreaming.rule.RepairNodeDTO;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.designspace.rule.arl.evaluator.EvaluationNode;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RDFRepairTreeProvider  {

	final RepairService repairService;
	final RuleRepository ruleRepo;	

	public RepairNodeDTO getRepairTree(RDFRuleResultWrapper ruleResult) {
		var ontClass = ruleResult.getInstanceType().getType();
		var def = ruleRepo.findRuleDefinitionForResource(ontClass);
		return repairService.getRepairRootNode( ruleResult.getContextInstance().getElement(), def);
	}


	public EvaluationNode getEvaluationNode(RDFRuleResultWrapper ruleResult) {	
			return ruleResult.getEvalWrapper().getDelegate().getEvaluationTree();
	}

}

package at.jku.isse.artifacteventstreaming.rule.evaluation;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RuleTriggerObserverFactory implements ServiceFactory {

	private final MetaModelSchemaTypes schemaProvider;
	
	@Override
	public CommitHandler getCommitHandlerInstanceFor(Branch branch, OntIndividual serviceConfigEntryPoint)
			throws Exception {
		String labelProp = serviceConfigEntryPoint.getLabel();
		if (labelProp == null) {
			labelProp = "Anonymous"+ActiveRuleTriggerObserver.class.getSimpleName()+branch.getBranchName();
		}
		return buildActiveInstance(labelProp, branch.getModel(), serviceConfigEntryPoint.getModel());
	}
	
	public ActiveRuleTriggerObserver buildActiveInstance(String serviceName, OntModel branchModel, OntModel repoModel) {		
		var factory = new RuleSchemaProvider(branchModel, schemaProvider);
		var ruleRepo = new RuleRepository(factory);
		return new ActiveRuleTriggerObserver(serviceName, repoModel, factory, ruleRepo);
	}

	public PassiveRuleTriggerObserver buildPassiveInstance(String serviceName, OntModel branchModel, OntModel repoModel) {		
		var factory = new RuleSchemaProvider(branchModel, schemaProvider);
		var ruleRepo = new RuleRepository(factory);
		return new PassiveRuleTriggerObserver(serviceName, repoModel, factory, ruleRepo);
	}
}

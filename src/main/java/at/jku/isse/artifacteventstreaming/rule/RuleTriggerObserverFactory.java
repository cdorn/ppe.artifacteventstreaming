package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RuleTriggerObserverFactory implements ServiceFactory {

	private final RuleSchemaFactory ruleSchemaFactory;
	
	@Override
	public CommitHandler getCommitHandlerInstanceFor(Branch branch, OntIndividual serviceConfigEntryPoint)
			throws Exception {
		String labelProp = serviceConfigEntryPoint.getLabel();
		if (labelProp == null) {
			labelProp = "Anonymous"+RuleTriggerObserver.class.getSimpleName()+branch.getBranchName();
		}
		return buildInstance(labelProp, branch.getModel(), serviceConfigEntryPoint.getModel());
	}
	
	public RuleTriggerObserver buildInstance(String serviceName, OntModel branchModel, OntModel repoModel) {		
		var factoryDest = new RuleSchemaProvider(branchModel, ruleSchemaFactory);
		var ruleRepoDest = new RuleRepository(factoryDest);
		return new RuleTriggerObserver(serviceName, repoModel, factoryDest, ruleRepoDest);
	}

}

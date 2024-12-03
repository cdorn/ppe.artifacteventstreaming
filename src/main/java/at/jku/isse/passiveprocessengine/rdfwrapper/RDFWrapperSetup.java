package at.jku.isse.passiveprocessengine.rdfwrapper;

import at.jku.isse.designspace.artifactconnector.core.repository.CoreTypeFactory;
import at.jku.isse.passiveprocessengine.core.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.core.DesignspaceTestSetup;
import at.jku.isse.passiveprocessengine.core.InstanceRepository;
import at.jku.isse.passiveprocessengine.core.RepairTreeProvider;
import at.jku.isse.passiveprocessengine.core.RuleDefinitionService;
import at.jku.isse.passiveprocessengine.core.RuleEvaluationService;
import at.jku.isse.passiveprocessengine.core.SchemaRegistry;
import lombok.Getter;

@Getter
public class RDFWrapperSetup implements DesignspaceTestSetup {

	private InstanceRepository instanceRepository;
	private SchemaRegistry schemaRegistry;
	private RepairTreeProvider repairTreeProvider;
	protected RuleEvaluationService ruleEvaluationService;
	private ChangeEventTransformer changeEventTransformer;
	private CoreTypeFactory coreTypeFactory;
	
	@Override
	public void setup() {
		NodeToDomainResolver resolver = new NodeToDomainResolver(null);
		schemaRegistry = resolver;
		instanceRepository = resolver;
		
		repairTreeProvider = new RDFRepairTreeProvider(null, resolver, null); 
		RuleDefinitionService ruleDef;
		coreTypeFactory = new CoreTypeFactory(resolver, ruleDef);
	}

	@Override
	public void tearDown() {
		// TODO Auto-generated method stub

	}



}

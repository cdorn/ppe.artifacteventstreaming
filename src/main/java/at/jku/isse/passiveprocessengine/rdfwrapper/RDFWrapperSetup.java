package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.net.URI;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.replay.InMemoryHistoryRepository;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.RuleTriggerObserverFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import at.jku.isse.designspace.artifactconnector.core.repository.CoreTypeFactory;
import at.jku.isse.passiveprocessengine.core.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.core.DesignspaceTestSetup;
import at.jku.isse.passiveprocessengine.core.InstanceRepository;
import at.jku.isse.passiveprocessengine.core.RepairTreeProvider;
import at.jku.isse.passiveprocessengine.core.RuleEvaluationService;
import at.jku.isse.passiveprocessengine.core.SchemaRegistry;
import lombok.Getter;

@Getter
public class RDFWrapperSetup implements DesignspaceTestSetup {

	public static final URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/rdfwrapper");
	
	private InstanceRepository instanceRepository;
	private SchemaRegistry schemaRegistry;
	private RepairTreeProvider repairTreeProvider;
	protected RuleEvaluationService ruleEvaluationService;
	private ChangeEventTransformer changeEventTransformer;
	private CoreTypeFactory coreTypeFactory;
	private RuleSchemaProvider ruleSchemaProvider;
	
	private RuleTriggerObserverFactory observerFactory;;
	
	@Override
	public void setup() {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		try {
			var branch = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
					.setModelReasoner(OntSpecification.OWL2_DL_MEM_RDFS_INF)		
					.setBranchLocalName("main")
					.build();		
			var model1 = branch.getModel();
			var cardUtil = new PropertyCardinalityTypes(model1);
			observerFactory = new RuleTriggerObserverFactory(new RuleSchemaFactory(cardUtil));
			var observer = observerFactory.buildInstance("RuleTriggeringObserver", model1, repoModel);
			var repairService = new RepairService(model1, observer.getRepo());
			RuleEnabledResolver resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);
			var changeTransformer = new CommitChangeEventTransformer("CommitToWrapperEventsTransformer", repoModel, resolver, new InMemoryHistoryRepository());
			branch.appendBranchInternalCommitService(observer);
			branch.appendBranchInternalCommitService(changeTransformer);
			branch.startCommitHandlers(null);
			branch.getDataset().begin();
			
			instanceRepository = resolver;
			schemaRegistry = resolver;
			repairTreeProvider = new RDFRepairTreeProvider(repairService, observer.getRepo(), resolver, observer); 			
			ruleEvaluationService = resolver;
			changeEventTransformer = changeTransformer;
			coreTypeFactory = new CoreTypeFactory(resolver, resolver);
			ruleSchemaProvider = observer.getFactory();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void tearDown() {
		// nothing to tear down, we just recreate everything upon a new setup call.
	}



}

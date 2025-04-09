package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.net.URI;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.rocksdb.RocksDBException;

import com.eventstore.dbclient.DeleteStreamOptions;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory.EventStoreImpl;
import at.jku.isse.artifacteventstreaming.branch.persistence.FilebasedDatasetLoader;
import at.jku.isse.artifacteventstreaming.branch.persistence.RocksDBFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.RuleTriggerObserverFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.designspace.artifactconnector.core.repository.CoreTypeFactory;
import at.jku.isse.passiveprocessengine.core.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.core.DesignspaceTestSetup;
import at.jku.isse.passiveprocessengine.core.InstanceRepository;
import at.jku.isse.passiveprocessengine.core.RepairTreeProvider;
import at.jku.isse.passiveprocessengine.core.RuleEvaluationService;
import at.jku.isse.passiveprocessengine.core.SchemaRegistry;
import lombok.Getter;

@Getter
public class RDFWrapperTestSetup implements DesignspaceTestSetup {

	private static final String RDF_WRAPPER_TEST_CACHE_DIR = "./RDFWrapperTestCache/";
	public static final URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/rdfwrapper");
	public static final URI branchURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/rdfwrapper/testbranch");
	
	private InstanceRepository instanceRepository;
	private SchemaRegistry schemaRegistry;
	private RepairTreeProvider repairTreeProvider;
	protected RuleEvaluationService ruleEvaluationService;
	private ChangeEventTransformer changeEventTransformer;
	private CoreTypeFactory coreTypeFactory;
	private RuleSchemaProvider ruleSchemaProvider;
	private Branch branch;
	private OntModel loadedModel = OntModelFactory.createModel();
	
	private RuleTriggerObserverFactory observerFactory;
	private static RocksDBFactory cacheFactory = new RocksDBFactory(RDF_WRAPPER_TEST_CACHE_DIR);
	
	@Override
	public void setup() {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		try {
			branch = new BranchBuilder(repoURI, repoDataset, repoModel)
					.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)		
					.setBranchLocalName("main")
					.build();		
			var model1 = branch.getModel();
			var metaModel = MetaModelOntology.buildInMemoryOntology(); 
			new RuleSchemaFactory(metaModel); // add rule schema to meta model
			var cardUtil = new MetaModelSchemaTypes(model1, metaModel);				
			var observerFactory = new RuleTriggerObserverFactory(cardUtil);
			var observer = observerFactory.buildInstance("RuleTriggeringObserver", model1, repoModel);
			var repairService = new RepairService(model1, observer.getRepo());
			RuleEnabledResolver resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);
			var changeTransformer = new CommitChangeEventTransformer("CommitToWrapperEventsTransformer", repoModel, resolver, observer.getFactory());
			branch.appendBranchInternalCommitService(observer);
			branch.appendBranchInternalCommitService(changeTransformer);
			branch.appendBranchInternalCommitService(new LazyLoadingLoopControllerService("LazyLoadingLoopController", repoModel, model1));
			// add a loop lazy loading controller
			
			branch.startCommitHandlers(null);
			branch.getDataset().begin();
			
			instanceRepository = resolver;
			schemaRegistry = resolver;
			repairTreeProvider = new RDFRepairTreeProvider(repairService, observer.getRepo()); 			
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

	public void setupPersistedBranch() throws Exception {
		
		var eventstoreFactory = new EventStoreFactory();
		
		var datasetLoader = new FilebasedDatasetLoader();
		var modelDataset = datasetLoader.loadDataset(branchURI);
		if (modelDataset.isEmpty()) throw new RuntimeException(branchURI+" could not be loaded");
		
		modelDataset.get().begin(ReadWrite.WRITE);
		var stateKeeper = new StateKeeperImpl(branchURI, cacheFactory.getCache(), eventstoreFactory.getEventStore(branchURI.toString()));
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		try {
			branch = new BranchBuilder(repoURI, repoDataset, repoModel)
					.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)		
					.setDataset(modelDataset.get())
					.setStateKeeper(stateKeeper)
					.setBranchURI(branchURI)
					.build();		
			var model1 = branch.getModel();	
			loadedModel.add(model1);
			System.out.println("loaded model of size: "+model1.size());
			var metaModel = MetaModelOntology.buildDBbackedOntology(); 
			new RuleSchemaFactory(metaModel); // add rule schema to meta model
			var cardUtil = new MetaModelSchemaTypes(model1, metaModel);				
			var observerFactory = new RuleTriggerObserverFactory(cardUtil);												
			System.out.println("Size after observer factory build: "+model1.size());
			var observer = observerFactory.buildInstance("RuleTriggeringObserver", model1, repoModel);
			System.out.println("Size after observer build: "+model1.size());
			var repairService = new RepairService(model1, observer.getRepo());
			RuleEnabledResolver resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);
			var changeTransformer = new CommitChangeEventTransformer("CommitToWrapperEventsTransformer", repoModel, resolver, observer.getFactory());
			branch.appendBranchInternalCommitService(observer);
			branch.appendBranchInternalCommitService(changeTransformer);
			branch.appendBranchInternalCommitService(new LazyLoadingLoopControllerService("LazyLoadingLoopController", repoModel, model1));
			// add a loop lazy loading controller
			
			System.out.println("Size after prepare listeners: "+model1.size());
			//var unfinishedCommit = stateKeeper.loadState();
			// branch.startCommitHandlers(unfinishedCommit); // first complete other stuff on top
			
			
			instanceRepository = resolver;
			schemaRegistry = resolver;
			repairTreeProvider = new RDFRepairTreeProvider(repairService, observer.getRepo());
			System.out.println("Size after repair tree build: "+model1.size());
			ruleEvaluationService = resolver;
			changeEventTransformer = changeTransformer;
			coreTypeFactory = new CoreTypeFactory(resolver, resolver);
			ruleSchemaProvider = observer.getFactory();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static void resetPersistence() {		
		try {
			new FilebasedDatasetLoader().removeDataset(branchURI);
			cacheFactory.clearAndCloseCache();
		} catch (Exception e) {
			e.printStackTrace();
		}
		EventStoreFactory factory = new EventStoreFactory();
		try {
			factory.getClient().getStreamMetadata(branchURI.toString()); //throws exception if doesn't exist, then we wont need to delete
			factory.getClient().deleteStream(branchURI.toString(), DeleteStreamOptions.get()).get();
		}catch (Exception e) {
			// ignore
		}
		try {		
			factory.getClient().getStreamMetadata(branchURI.toString()+EventStoreImpl.INCOMING_COMMITS_STREAM_POSTFIX); //throws exception if doesn't exist, then we wont need to delete
			factory.getClient().deleteStream(branchURI.toString()+EventStoreImpl.INCOMING_COMMITS_STREAM_POSTFIX, DeleteStreamOptions.get()).get();
		}catch (Exception e) {
			// ignore
		}
	}
	
	public static void prepareForPersistedReloadWithoutDataRemoval() throws RocksDBException {
		cacheFactory.closeCache();
	}
}

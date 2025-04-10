package at.jku.isse.passiveprocessengine.rdfwrapper.config;

import java.net.URI;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;

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
import at.jku.isse.passiveprocessengine.rdfwrapper.CoreTypeFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.LazyLoadingLoopControllerService;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.CommitChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RDFRepairTreeProvider;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;
import lombok.Getter;

@Getter
public class RDFWrapperTestSetup {

	private static final String RDF_WRAPPER_TEST_CACHE_DIR = "./RDFWrapperTestCache/";
	public static final URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/rdfwrapper");
	public static final URI branchURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/rdfwrapper/testbranch");
	
	private RuleEnabledResolver resolver;
	private RDFRepairTreeProvider repairTreeProvider;
	private ChangeEventTransformer changeEventTransformer;
	private CoreTypeFactory coreTypeFactory;
	private RuleSchemaProvider ruleSchemaProvider;
	private Branch branch;
	private OntModel loadedModel = OntModelFactory.createModel();
	
	private RuleTriggerObserverFactory observerFactory;
	private static RocksDBFactory cacheFactory = new RocksDBFactory(RDF_WRAPPER_TEST_CACHE_DIR);
	

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
			observerFactory = new RuleTriggerObserverFactory(cardUtil);
			var observer = observerFactory.buildInstance("RuleTriggeringObserver", model1, repoModel);
			var repairService = new RepairService(model1, observer.getRepo());
			resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);
			var changeTransformer = new CommitChangeEventTransformer("CommitToWrapperEventsTransformer", repoModel, resolver, observer.getFactory());
			branch.appendBranchInternalCommitService(observer);
			branch.appendBranchInternalCommitService(changeTransformer);
			branch.appendBranchInternalCommitService(new LazyLoadingLoopControllerService("LazyLoadingLoopController", repoModel, model1));
			
			branch.startCommitHandlers(null);
			branch.getDataset().begin();
						
			repairTreeProvider = new RDFRepairTreeProvider(repairService, observer.getRepo()); 			
			changeEventTransformer = changeTransformer;
			coreTypeFactory = new CoreTypeFactory(resolver);
			ruleSchemaProvider = observer.getFactory();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

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
			observerFactory = new RuleTriggerObserverFactory(cardUtil);												
			System.out.println("Size after observer factory build: "+model1.size());
			var observer = observerFactory.buildInstance("RuleTriggeringObserver", model1, repoModel);
			System.out.println("Size after observer build: "+model1.size());
			var repairService = new RepairService(model1, observer.getRepo());
			resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);
			var changeTransformer = new CommitChangeEventTransformer("CommitToWrapperEventsTransformer", repoModel, resolver, observer.getFactory());
			branch.appendBranchInternalCommitService(observer);
			branch.appendBranchInternalCommitService(changeTransformer);
			branch.appendBranchInternalCommitService(new LazyLoadingLoopControllerService("LazyLoadingLoopController", repoModel, model1));
			// add a loop lazy loading controller
			
			System.out.println("Size after prepare listeners: "+model1.size());
			//var unfinishedCommit = stateKeeper.loadState();
			// branch.startCommitHandlers(unfinishedCommit); // first complete other stuff on top
			
			repairTreeProvider = new RDFRepairTreeProvider(repairService, observer.getRepo());
			System.out.println("Size after repair tree build: "+model1.size());
	
			changeEventTransformer = changeTransformer;
			coreTypeFactory = new CoreTypeFactory(resolver);
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
	
	public static void prepareForPersistedReloadWithoutDataRemoval() {
		cacheFactory.closeCache();
	}
}

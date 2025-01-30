package at.jku.isse.passiveprocessengine.rdfwrapper.config;

import java.net.URI;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchStateKeeper;
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.FilebasedDatasetLoader;
import at.jku.isse.artifacteventstreaming.branch.persistence.RocksDBFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.RuleTriggerObserverFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import at.jku.isse.designspace.artifactconnector.core.repository.CoreTypeFactory;
import at.jku.isse.passiveprocessengine.core.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.core.InstanceRepository;
import at.jku.isse.passiveprocessengine.core.RepairTreeProvider;
import at.jku.isse.passiveprocessengine.core.RuleEvaluationService;
import at.jku.isse.passiveprocessengine.core.SchemaRegistry;
import at.jku.isse.passiveprocessengine.rdfwrapper.CommitChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.LazyLoadingLoopControllerService;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFRepairTreeProvider;
import at.jku.isse.passiveprocessengine.rdfwrapper.RuleEnabledResolver;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Getter
@RequiredArgsConstructor
public class EventStreamingWrapperFactory {

	private final InstanceRepository instanceRepository;
	private final SchemaRegistry schemaRegistry;
	private final RepairTreeProvider repairTreeProvider;
	private final RuleEvaluationService ruleEvaluationService;
	private final ChangeEventTransformer changeEventTransformer;
	private final CoreTypeFactory coreTypeFactory;
	private final RuleSchemaProvider ruleSchemaProvider;
	private final Branch branch;	
	@Getter(value = AccessLevel.NONE) private final BranchStateUpdater stateKeeper;
	
	public void signalExternalSetupComplete() throws PersistenceException, BranchConfigurationException {
		var unfinishedCommit = stateKeeper.loadState();
		 branch.startCommitHandlers(unfinishedCommit); // first complete other stuff on top
	}
	
	
	@Slf4j
	public static class FactoryBuilder {
	
		public static final String defaultRepoURI = "http://at.jku.isse.artifacteventstreaming/repository";
		public static final String defaultBranchBaseURI = "http://at.jku.isse.artifacteventstreaming/repository/branch";
		public static final String defaultBranchName = "main";
		public static final String defaultCacheDirectoryPath = "./artifacteventstreamingcache/";
		
		private URI repoURI;

		private String branchLocalName = defaultBranchName;	
		private String relativeCachePath = defaultCacheDirectoryPath;
		
		public FactoryBuilder withRepoURI(@NonNull URI repoURI) {
			this.repoURI = repoURI;
			return this;
		}
		
		public FactoryBuilder withRepoURI(@NonNull String branchName) {
			this.branchLocalName = branchName;
			return this;
		}
		
		public FactoryBuilder withRelativeCachePath(@NonNull String cachePath) {
			this.relativeCachePath = cachePath;
			return this;
		}
		
		public EventStreamingWrapperFactory build() {
			try {
				// setting up persistence
				var cacheFactory = new RocksDBFactory(relativeCachePath);
				var eventstoreFactory = new EventStoreFactory();											
				var datasetLoader = new FilebasedDatasetLoader();
				URI datasetURI = new URI(defaultBranchBaseURI+"/"+branchLocalName);
				var modelDataset = datasetLoader.loadDataset(datasetURI);			
				if (modelDataset.isEmpty()) 
					throw new RuntimeException(datasetURI+" could not be loaded");			
				modelDataset.get().begin(ReadWrite.WRITE);
				
				Dataset repoDataset = DatasetFactory.createTxnMem(); // we are not persisting the repository itself 
				OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
				
				// add logic here, when history access is needed
				
				// setting up branch
				var branchURI = BranchBuilder.generateBranchURI(new URI(defaultBranchBaseURI), branchLocalName);
				var stateKeeper = new StateKeeperImpl(branchURI, cacheFactory.getCache(), eventstoreFactory.getEventStore(branchURI.toString()));
										
				var branch = new BranchBuilder(repoURI, repoDataset, repoModel)
						.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)		
						.setDataset(modelDataset.get())
						.setStateKeeper(stateKeeper)
						.setBranchURI(branchURI)
						.build();		
				var model1 = branch.getModel();	
				
				// setting up branch commit handlers
				var cardUtil = new PropertyCardinalityTypes(model1);				
				var observerFactory = new RuleTriggerObserverFactory(new RuleSchemaFactory(cardUtil));
				var observer = observerFactory.buildInstance("RuleTriggeringObserver", model1, repoModel);				
				var repairService = new RepairService(model1, observer.getRepo());
				RuleEnabledResolver resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);
				var changeTransformer = new CommitChangeEventTransformer("CommitToWrapperEventsTransformer", repoModel, resolver, observer.getFactory());
				branch.appendBranchInternalCommitService(observer);
				branch.appendBranchInternalCommitService(changeTransformer);
				branch.appendBranchInternalCommitService(new LazyLoadingLoopControllerService("LazyLoadingLoopController", repoModel, model1));								
				
				// set up additional wrapper components
				var repairTreeProvider = new RDFRepairTreeProvider(repairService, observer.getRepo(), resolver, observer);
				var coreTypeFactory = new CoreTypeFactory(resolver, resolver);
				return new EventStreamingWrapperFactory(resolver, resolver, repairTreeProvider, resolver, changeTransformer
						, coreTypeFactory, observer.getFactory(), branch, stateKeeper);																			
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
}

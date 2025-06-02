package at.jku.isse.passiveprocessengine.rdfwrapper.config;

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
import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.PerBranchEventStore;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.FilebasedDatasetLoader;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryEventStore;
import at.jku.isse.artifacteventstreaming.branch.persistence.RocksDBFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleTriggerObserverFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.passiveprocessengine.rdfwrapper.CoreTypeFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.LazyLoadingLoopControllerService;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.CommitChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.metaschema.WrapperMetaModelSchemaTypes;
import at.jku.isse.passiveprocessengine.rdfwrapper.metaschema.WrapperMetaModelSchemaTypes.WrapperMetaModelOntology;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


public class PersistedEventStreamingSetupFactory extends AbstractEventStreamingSetup {
	
	private FactoryBuilder builder;
	
	public PersistedEventStreamingSetupFactory(RuleEnabledResolver resolver, ChangeEventTransformer changeEventTransformer,
			CoreTypeFactory coreTypeFactory, RuleSchemaProvider ruleSchemaProvider, Branch branch,
			BranchStateUpdater stateKeeper) {
		super(resolver, changeEventTransformer, coreTypeFactory, ruleSchemaProvider, branch, stateKeeper);
	}


	@Override
	public void signalExternalSetupComplete() throws PersistenceException, BranchConfigurationException {
		branch.getDataset().commit();
		branch.getDataset().end();	
		var unfinishedCommit = stateKeeper.loadState();
		branch.startCommitHandlers(unfinishedCommit); 		 
	}
	
	@Override
	public void resetPersistedData() {
		branch.getDataset().close();
		builder.resetData();
	}
	
	
	@Slf4j
	public static class FactoryBuilder {
	
		private String branchLocalName = AbstractEventStreamingSetup.defaultBranchName;	
		private String relativeCachePath = AbstractEventStreamingSetup.defaultCacheDirectoryPath;
		protected final FilebasedDatasetLoader datasetLoader = new FilebasedDatasetLoader();
		private EventStoreFactory eventstoreFactory;
		private RocksDBFactory cacheFactory;
		private URI repoURI;
		protected URI datasetURI;
		protected URI branchURI;
		
		public FactoryBuilder withRepoURI(@NonNull URI repoURI) {
			this.repoURI = repoURI;
			return this;
		}

		public FactoryBuilder withBranchName(@NonNull String branchName) {
			this.branchLocalName = branchName;
			return this;
		}
		
		public FactoryBuilder withRelativeCachePath(@NonNull String cachePath) {
			this.relativeCachePath = cachePath;
			return this;
		}
		
		protected PerBranchEventStore getEventStore(String branchURI) {
			eventstoreFactory = new EventStoreFactory();
			return eventstoreFactory.getEventStore(branchURI);
		}
		
		protected BranchStateCache getBranchStateCache() throws Exception {
			cacheFactory = new RocksDBFactory(relativeCachePath);	
			return cacheFactory.getCache();
		}
		
		public PersistedEventStreamingSetupFactory build() {
			try {
				// setting up persistence
				datasetURI = new URI(AbstractEventStreamingSetup.defaultBranchBaseURI+"/"+branchLocalName);
				var modelDataset = datasetLoader.loadDataset(datasetURI);			
				if (modelDataset.isEmpty()) 
					throw new RuntimeException(datasetURI+" could not be loaded");			
				modelDataset.get().begin(ReadWrite.WRITE);
				
				Dataset repoDataset = DatasetFactory.createTxnMem(); // we are not persisting the repository itself 
				OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
				
				// add logic here, when history access is needed
				
				// setting up branch
				if (repoURI == null) repoURI = new URI(AbstractEventStreamingSetup.defaultRepoURI);
				branchURI = BranchBuilder.generateBranchURI(new URI(AbstractEventStreamingSetup.defaultBranchBaseURI), branchLocalName);
				var stateKeeper = new StateKeeperImpl(branchURI, getBranchStateCache(), getEventStore(branchURI.toString()));				
				var branch = new BranchBuilder(repoURI, repoDataset, repoModel)
						.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)		
						.setDataset(modelDataset.get())
						.setStateKeeper(stateKeeper)
						.setBranchURI(branchURI)
						.build();		
				var model1 = branch.getModel();	
				
				// setting up branch commit handlers
				var metaModel = WrapperMetaModelOntology.buildDBbackedOntology(); 
				new RuleSchemaFactory(metaModel); // add rule schema to meta model
				var cardUtil = new WrapperMetaModelSchemaTypes(model1, metaModel);				
				var observerFactory = new RuleTriggerObserverFactory(cardUtil);
				var observer = observerFactory.buildActiveInstance("ActiveRuleTriggeringObserver", model1, repoModel);				
				var repairService = new RepairService(model1, observer.getRepo());
				RuleEnabledResolver resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);
				var changeTransformer = new CommitChangeEventTransformer("CommitToWrapperEventsTransformer", repoModel, resolver, observer.getFactory());
				branch.appendBranchInternalCommitService(observer);
				branch.appendBranchInternalCommitService(changeTransformer);
				branch.appendBranchInternalCommitService(new LazyLoadingLoopControllerService("LazyLoadingLoopController", repoModel, model1));								
				
				// set up additional wrapper components
				var coreTypeFactory = new CoreTypeFactory(resolver);
				var factory = new PersistedEventStreamingSetupFactory(resolver, changeTransformer
						, coreTypeFactory, observer.getFactory(), branch, stateKeeper);			
				factory.builder = this;
				return factory;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		protected void resetData() {
			// remove triple store data
			this.datasetLoader.removeDataset(datasetURI);
			// remove event data
			this.eventstoreFactory.removeBranchEventData(branchURI.toString());	
			// remove all cache data
			try {
				cacheFactory.clearAndCloseCache();
			} catch (RocksDBException e) {
				log.error(e.getMessage());
			}
		}
	}
	
	@Slf4j
	public static class TripleStoreOnlyFactoryBuilder extends FactoryBuilder{
		
		@Override
		protected PerBranchEventStore getEventStore(String branchURI) {
			return new InMemoryEventStore();
		}
		
		@Override
		protected BranchStateCache getBranchStateCache() throws Exception {
			return new InMemoryBranchStateCache();
		}
		
		@Override
		protected void resetData() {
			// just triple store to reset
			this.datasetLoader.removeDataset(datasetURI);
		}
	}


	
}

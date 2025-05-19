package at.jku.isse.passiveprocessengine.rdfwrapper.config;

import java.net.URI;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.outgoing.DefaultDirectBranchCommitStreamer;
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


public class InMemoryEventStreamingSetupFactory extends AbstractEventStreamingSetup {

	
	public InMemoryEventStreamingSetupFactory(RuleEnabledResolver resolver, ChangeEventTransformer changeEventTransformer,
			CoreTypeFactory coreTypeFactory, RuleSchemaProvider ruleSchemaProvider, Branch branch,
			BranchStateUpdater stateKeeper) {
		super(resolver, changeEventTransformer, coreTypeFactory, ruleSchemaProvider, branch, stateKeeper);
	}

	@Override
	public void signalExternalSetupComplete() throws PersistenceException, BranchConfigurationException {
		//no state to load as nonpersisted
		branch.startCommitHandlers(null); 		 
	}
	
	@Slf4j
	public static class FactoryBuilder {
	
		private URI repoURI;

		private String branchLocalName = AbstractEventStreamingSetup.defaultBranchName;	
		
		public FactoryBuilder withRepoURI(@NonNull URI repoURI) {
			this.repoURI = repoURI;
			return this;
		}
		
		public FactoryBuilder withBranchName(@NonNull String branchName) {
			this.branchLocalName = branchName;
			return this;
		}
		
		public InMemoryEventStreamingSetupFactory build() {
			try {
				//create inmemory model for branch and repo						
				Dataset repoDataset = DatasetFactory.createTxnMem(); // we are not persisting the repository itself (not the same inmemory repo as backend wrapper!)
				OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);				
						
				// no persistence, all inmemory				
				if (repoURI == null) repoURI = new URI(AbstractEventStreamingSetup.defaultRepoURI);
				var branchURI = BranchBuilder.generateBranchURI(repoURI, branchLocalName);
				var stateKeeper = new StateKeeperImpl(branchURI,  new InMemoryBranchStateCache(), new InMemoryEventStore());				
				var branch = new BranchBuilder(repoURI, repoDataset, repoModel)
						.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)		
						.setStateKeeper(stateKeeper)
						.setBranchURI(branchURI)
						.build();		
				var backendModel = branch.getModel();	
					
				var metaModel = WrapperMetaModelOntology.buildInMemoryOntology(); 
				new RuleSchemaFactory(metaModel); // add rule schema to meta model
				var cardUtil = new WrapperMetaModelSchemaTypes(backendModel, metaModel);
				// setting up branch commit handlers				 														
				var observerFactory = new RuleTriggerObserverFactory(cardUtil);
				var observer = observerFactory.buildActiveInstance("ActiveRuleTriggeringObserver", backendModel, repoModel);					
				var repairService = new RepairService(backendModel, observer.getRepo()); 
				var resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);								
				var changeTransformer = new CommitChangeEventTransformer("BackendCommitToWrapperEventsTransformer", repoModel, resolver, observer.getFactory());				
				branch.appendBranchInternalCommitService(observer);
				branch.appendBranchInternalCommitService(changeTransformer);
				branch.appendBranchInternalCommitService(new LazyLoadingLoopControllerService("BackendLazyLoadingLoopController", repoModel, backendModel));								
				
				// set up additional wrapper components
				var coreTypeFactory = new CoreTypeFactory(resolver);
				return new InMemoryEventStreamingSetupFactory(resolver, changeTransformer
						, coreTypeFactory, observer.getFactory(), branch, stateKeeper);																			
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
}

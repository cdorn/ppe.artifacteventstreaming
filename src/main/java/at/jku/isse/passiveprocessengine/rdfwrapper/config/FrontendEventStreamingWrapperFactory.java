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
import at.jku.isse.artifacteventstreaming.branch.incoming.CompleteCommitMerger;
import at.jku.isse.artifacteventstreaming.branch.outgoing.DefaultDirectBranchCommitStreamer;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryEventStore;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleTriggerObserverFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.CoreTypeFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.CommitChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.metaschema.WrapperMetaModelSchemaTypes;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Getter
public class FrontendEventStreamingWrapperFactory extends AbstractEventStreamingSetup{

	public FrontendEventStreamingWrapperFactory(RuleEnabledResolver resolver,
			ChangeEventTransformer changeEventTransformer, CoreTypeFactory coreTypeFactory,
			RuleSchemaProvider ruleSchemaProvider, Branch branch, BranchStateUpdater stateKeeper) {
		super(resolver, changeEventTransformer, coreTypeFactory, ruleSchemaProvider, branch, stateKeeper);
	}

	public void signalExternalSetupComplete() throws PersistenceException, BranchConfigurationException {
		// nothing to load statewise as inmemory and clean copy from backend		
		branch.startCommitHandlers(null); 		 
	}
	
	
	@Slf4j
	@RequiredArgsConstructor
	public static class FactoryBuilder {
						
		public static final String defaultFrontendBranchName = "frontend";
				
		private final AbstractEventStreamingSetup backendFactory;
		private String branchLocalName = defaultFrontendBranchName;	
						
		public FactoryBuilder withFrontendBranchName(@NonNull String branchName) {
			this.branchLocalName = branchName;
			return this;
		}
						
		public FrontendEventStreamingWrapperFactory build() {
			try {											
				//create inmemory model for branch and repo						
				Dataset repoDataset = DatasetFactory.createTxnMem(); // we are not persisting the repository itself (not the same inmemory repo as backend wrapper!)
				OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);				
				Dataset modelDataset = DatasetFactory.create();
						
				// no persistence, all inmemory				
				var branchURI = BranchBuilder.generateBranchURI(new URI(AbstractEventStreamingSetup.defaultBranchBaseURI), branchLocalName);
				var stateKeeper = new StateKeeperImpl(branchURI,  new InMemoryBranchStateCache(), new InMemoryEventStore());				
				var branch = new BranchBuilder(new URI(backendFactory.getBranch().getRepositoryURI()), repoDataset, repoModel)
						.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)		
						.setDataset(modelDataset)
						.setStateKeeper(stateKeeper)
						.setBranchURI(branchURI)
						.build();		
				var frontendModel = branch.getModel();	
				var merger = new CompleteCommitMerger(branch);
				branch.appendIncomingCommitMerger(merger);
				
				//copy state/model from backend branch				
				var sourceBranch = backendFactory.getBranch();
				var backendDataset = sourceBranch.getDataset();
				var backendModel = sourceBranch.getModel();
				backendDataset.begin(ReadWrite.READ);
				frontendModel.add(backendModel);				
				backendDataset.end();
				sourceBranch.appendOutgoingCommitDistributer(new DefaultDirectBranchCommitStreamer(sourceBranch, branch, new InMemoryBranchStateCache()));				
								
				var cardUtil = new WrapperMetaModelSchemaTypes(frontendModel);									
				// setting up branch commit handlers				 														
				// we need to update rule repo without executing rules
				var observerFactory = new RuleTriggerObserverFactory(cardUtil);
				var observer = observerFactory.buildPassiveInstance("PassiveRuleTriggeringObserver", frontendModel, repoModel);					
				//repo has exactly same content as backend branch but does not reevaluate rules
				branch.appendBranchInternalCommitService(observer);
				
				var repairService = new RepairService(frontendModel, observer.getRepo()); 
				RuleEnabledResolver resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);								
				var changeTransformer = new CommitChangeEventTransformer("CommitToWrapperEventsTransformer", repoModel, resolver, observer.getFactory());				
				//	this enables refreshing of abstraction layer cache upon any changes
				branch.appendBranchInternalCommitService(changeTransformer);
				//AND no need for loop controller as there is no lazy loading here, that happened already in backend,
				var coreTypeFactory = new CoreTypeFactory(resolver);			
				
				return new FrontendEventStreamingWrapperFactory(resolver, changeTransformer
						, coreTypeFactory, observer.getFactory(), branch, stateKeeper);																			
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}


	@Override
	public void resetPersistedData() {
		// no op, as all data stored in memory
	}
	
	
}

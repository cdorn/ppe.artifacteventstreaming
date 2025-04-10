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
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryEventStore;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.RuleTriggerObserverFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.CoreTypeFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.CommitChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.metaschema.WrapperMetaModelSchemaTypes;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RDFRepairTreeProvider;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Getter
@RequiredArgsConstructor
public class FrontendEventStreamingWrapperFactory {

	private final RuleEnabledResolver resolver;
	private final RDFRepairTreeProvider repairTreeProvider;
	private final ChangeEventTransformer changeEventTransformer;
	private final CoreTypeFactory coreTypeFactory;
	private final RuleSchemaProvider ruleSchemaProvider;
	private final Branch branch;	
	@Getter(value = AccessLevel.NONE) private final BranchStateUpdater stateKeeper;
	
	public void signalExternalSetupComplete() throws PersistenceException, BranchConfigurationException {
		// nothing to load statewise as inmemory and clean copy from backend		
		branch.startCommitHandlers(null); 		 
	}
	
	
	@Slf4j
	@RequiredArgsConstructor
	public static class FactoryBuilder {
						
		public static final String defaultFrontendBranchName = "frontend";
				
		private final EventStreamingWrapperFactory backendFactory;
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
				var branchURI = BranchBuilder.generateBranchURI(new URI(EventStreamingWrapperFactory.FactoryBuilder.defaultBranchBaseURI), branchLocalName);
				var stateKeeper = new StateKeeperImpl(branchURI,  new InMemoryBranchStateCache(), new InMemoryEventStore());				
				var branch = new BranchBuilder(new URI(backendFactory.getBranch().getRepositoryURI()), repoDataset, repoModel)
						.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)		
						.setDataset(modelDataset)
						.setStateKeeper(stateKeeper)
						.setBranchURI(branchURI)
						.build();		
				var model1 = branch.getModel();	
				
				//copy state/model from backend branch				
				var sourceBranch = backendFactory.getBranch();
				var backendDataset = sourceBranch.getDataset();
				var backendModel = sourceBranch.getModel();
				backendDataset.begin(ReadWrite.READ);
				model1.add(backendModel);				
				backendDataset.end();
				sourceBranch.appendOutgoingCommitDistributer(new DefaultDirectBranchCommitStreamer(sourceBranch, branch, new InMemoryBranchStateCache()));				
								
				var cardUtil = new WrapperMetaModelSchemaTypes(model1);									
				// setting up branch commit handlers				 														
				// we need to update rule repo without executing rules
				var observerFactory = new RuleTriggerObserverFactory(cardUtil);
				var observer = observerFactory.buildInstance("RuleTriggeringObserver", model1, repoModel);					
				//TODO we need to ensure that the rule repo has exactly same content as backend branch but does not reevaluate rules
				// for now, rules get re-evaluated
				branch.appendBranchInternalCommitService(observer);
				
				var repairService = new RepairService(model1, observer.getRepo()); 
				RuleEnabledResolver resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);								
				var changeTransformer = new CommitChangeEventTransformer("CommitToWrapperEventsTransformer", repoModel, resolver, observer.getFactory());				
				//	this enables refreshing of abstraction layer cache upon any changes
				branch.appendBranchInternalCommitService(changeTransformer);
				//AND no need for loop controller as there is no lazy loading here, that happened already in backend,
				
				var coreTypeFactory = new CoreTypeFactory(resolver);			
				
				// set up additional wrapper components
				//TODO make this a cached repair tree provider, we want to reuse persisted repair trees
				var repairTreeProvider = new RDFRepairTreeProvider(repairService, observer.getRepo());
				return new FrontendEventStreamingWrapperFactory(resolver, repairTreeProvider, changeTransformer
						, coreTypeFactory, observer.getFactory(), branch, stateKeeper);																			
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
}

package at.jku.isse.artifacteventstreaming.branch;

import at.jku.isse.artifacteventstreaming.api.*;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.NotFoundException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import io.micrometer.observation.ObservationRegistry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;

import java.net.URI;
import java.util.*;

@Slf4j
public class BranchRepository {

	private final URI repositoryURI;
	private final Resource repoRes; 
	private final Dataset repoDataset;
	private final OntModel repoModel;
	private final DatasetRepository datasetLoader;
	private final StateKeeperFactory stateKeeperFactory;
	private final ServiceFactoryRegistry factoryRegistry;
	private final Map<String, Branch> branches = new HashMap<>();
	private final MetaModelOntologyProvider metaOntologyProvider;
    private final ObservationRegistry observationRegistry;

	public BranchRepository(@NonNull URI repositoryURI
			, @NonNull DatasetRepository datasetLoader
			, @NonNull StateKeeperFactory stateKeeperFactory
			, @NonNull ServiceFactoryRegistry factoryRegistry
			, @NonNull MetaModelOntologyProvider metaOntologyProvider
            , @NonNull ObservationRegistry observationRegistry) throws NotFoundException {
		this.repositoryURI = repositoryURI;
		this.metaOntologyProvider = metaOntologyProvider;
        this.observationRegistry = observationRegistry;
		Optional<Dataset> datasetOpt = datasetLoader.loadDataset(repositoryURI);
		if (datasetOpt.isEmpty()) {
			throw new NotFoundException("Could not find repository for: "+repositoryURI);
		}
		this.repoRes = ResourceFactory.createResource(repositoryURI.toString());
		this.repoDataset = datasetOpt.get();
		repoDataset.begin(ReadWrite.WRITE);
		this.repoModel = OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		this.stateKeeperFactory = stateKeeperFactory;
		this.factoryRegistry = factoryRegistry;
		this.datasetLoader = datasetLoader;
		repoDataset.commit();
		repoDataset.end();
	}

	public Dataset getRepositoryDataset() {
		return repoDataset;
	}
	
	public OntModel getRepositoryModel() {
		return repoModel;
	}
	
	public RegisteringBranchBuilder getInitializedBranchBuilder(String branchName) throws BranchConfigurationException {
		
		var uri = BranchBuilder.generateBranchURI(repoRes, branchName);
		if (BranchBuilder.doesDatasetContainBranch(repoDataset, repoRes, uri)) {
			var msg = String.format("Branch %s already exists in repo %s", branchName, repoRes.getURI());
			log.warn(msg);
			throw new BranchConfigurationException(msg);
		}
		var builder = new RegisteringBranchBuilder(repositoryURI, repoDataset, repoModel, observationRegistry);
		builder.setBranchLocalName(branchName)
				.setStateKeeper(stateKeeperFactory.createStateKeeperFor(uri))
				.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)
				.setMetaModelOntologyProvider(metaOntologyProvider)
				; // we set the default inference model here statically, can be overridden if necessary
				
		var optDataset = datasetLoader.loadDataset(uri);
        optDataset.ifPresent(builder::setDataset);
		return builder;
	}
	
	public Map<String, Resource> getAllBranches() {
		var branchSet = new HashMap<String, Resource>();
		repoDataset.begin(ReadWrite.READ);
		var iter = repoModel.listResourcesWithProperty(AES.partOfRepository, repoRes);
		while (iter.hasNext()) {
			var branchRes = iter.next();
			branchSet.put(branchRes.getURI(), branchRes);
		}
		repoDataset.end();
		return branchSet;
	}
	
	public Set<Resource> getBranchesOwnedByUser(String userId) {
		var branchSet = new HashSet<Resource>();
		repoDataset.begin(ReadWrite.READ);
		var iter = repoModel.listResourcesWithProperty(AES.ownedBy, userId);
		while (iter.hasNext()) {
			var branchRes = iter.next();
			branchSet.add(branchRes);
		}
		repoDataset.end();
		return branchSet;
	}
		
	public Branch getOrLoadBranch(URI branchURI) throws PersistenceException, BranchConfigurationException{
		Branch branch = branches.get(branchURI.toString());
		if (branch != null) {
			return branch;
		} else {
			Optional<Dataset> datasetOpt = datasetLoader.loadDataset(branchURI);
			if (datasetOpt.isEmpty()) {
				log.info("Could not find dataset for: "+branchURI.toString());
				return null;
			} else {
				BranchStateUpdater stateKeeper = stateKeeperFactory.createStateKeeperFor(branchURI);
				branch = new BranchBuilder(repositoryURI, repoDataset, repoModel, observationRegistry)
						.setDataset(datasetOpt.get())
						.setBranchLocalName(BranchBuilder.getBranchNameFromURI(branchURI))
						.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF) // DEFER: technical debt - we set the inference model here statically, not as part of the configuration. For now we assume all use cases will require this anyway.
						.setMetaModelOntologyProvider(metaOntologyProvider)
						.setStateKeeper(stateKeeper)
						.build();
				Commit prelimUnfinishedCommit = stateKeeper.loadState();
				registerBranch(branch); // now branch can be found and referenced by other branches
				
				boolean doTX = !repoDataset.isInTransaction();
				if (doTX) { 
					repoDataset.begin(ReadWrite.WRITE);		
				}
				initializeBranch(branch); // reload incoming, local, outgoing commit handlers
				if (doTX) {
					repoDataset.commit();
					repoDataset.end();
				}
				branch.startCommitHandlers(prelimUnfinishedCommit);
				return branch;
			}
		}
	}

	private void initializeBranch(Branch branch) throws BranchConfigurationException {
		// we inspect the branch resource for any configuration data
		for(var config : branch.getLocalCommitServiceConfig()) {
			CommitHandler handler = resolveHandler(branch, config);
			if (handler != null) {
				branch.appendBranchInternalCommitService((IncrementalCommitHandler) handler);
			}	
		}

		for (var config : branch.getOutgoingCommitDistributerConfig()){
			CommitHandler handler = resolveHandler(branch, config);
			if (handler != null) {
				branch.appendOutgoingCommitDistributer(handler);
			}						
			// we cant start the distributer as enqueuing relies on the destination branch's stateKeeper (which might not be ready yet) 
		}

		for (var config : branch.getIncomingCommitHandlerConfig()) {
			CommitHandler handler = resolveHandler(branch, config);
			if (handler != null) {
				branch.appendIncomingCommitMerger(handler);
			}
		}
	}
	
	private CommitHandler resolveHandler(Branch branch, OntIndividual config) throws BranchConfigurationException {
		Statement typeStmt = config.getProperty(AES.isConfigForHandlerType);
		if (typeStmt != null) {
			Optional<ServiceFactory> factory = factoryRegistry.getFactory(typeStmt.getResource().getURI());
			if (factory.isPresent()) {
				try {
					return factory.get().getCommitHandlerInstanceFor(branch, config);
				} catch (BranchConfigurationException e) {
					String msg = String.format("Error creating CommitHandler %s while initializing branch %s: %s", typeStmt.getResource().getURI(), branch.getBranchId(), e.getMessage());
					log.warn(msg);
					throw e;
				}
			} else {
				String msg = String.format("Could not resolve Factory for %s while initializing branch %s", typeStmt.getResource().getURI(), branch.getBranchId());
				log.warn(msg);
				throw new BranchConfigurationException(msg);
			}
		}
		return null;
	}

	public void registerBranch(Branch branch) {
		branches.put(branch.getBranchResource().getURI(), branch);
	}

	public void remove(Branch branch) {
		branch.deactivate();
		
		repoDataset.begin(ReadWrite.WRITE);
		repoModel.remove(branch.getBranchResource(), AES.partOfRepository, repoRes);
		branch.getBranchResource().removeProperties();
		repoDataset.commit();
		repoDataset.end();
		
		branches.remove(branch.getBranchId());
		branch.getDataset().begin(ReadWrite.WRITE);
		branch.getModel().removeAll();
		branch.getDataset().commit();
		branch.getDataset().end();
		
		//TODO: remove branch config info from repoModel/Dataset, currently only direct branch properties removed
		branch.getDataset().begin(ReadWrite.WRITE);
		//TODO: what to do if there is an ongoing write session with Lock?!
		branch.getModel().removeAll();
		branch.getDataset().commit();
		branch.getDataset().close();
		
	}
	
	public class RegisteringBranchBuilder extends BranchBuilder{

 		public RegisteringBranchBuilder(@NonNull URI repositoryURI, @NonNull Dataset repoDataset, @NonNull OntModel repoModel, @NonNull ObservationRegistry observationRegistry) {
			super(repositoryURI, repoDataset, repoModel, observationRegistry);
		}
		
		@Override
		public Branch build() {
			var branch = super.build();
			registerBranch(branch);
			return branch;
		}
	}
}

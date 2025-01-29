package at.jku.isse.artifacteventstreaming.branch;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.rdf.model.Statement;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.DatasetRepository;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import at.jku.isse.artifacteventstreaming.api.ServiceFactoryRegistry;
import at.jku.isse.artifacteventstreaming.api.StateKeeperFactory;
import at.jku.isse.artifacteventstreaming.api.exceptions.NotFoundException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BranchRepository {

	private final URI repositoryURI;
	private final Dataset repoDataset;
	private final OntModel repoModel;
	private final DatasetRepository datasetLoader;
	private final StateKeeperFactory stateKeeperFactory;
	private final ServiceFactoryRegistry factoryRegistry;
	private final Map<String, Branch> branches = new HashMap<>();

	public BranchRepository(@NonNull URI repositoryURI
			, @NonNull DatasetRepository datasetLoader
			, @NonNull StateKeeperFactory stateKeeperFactory
			, @NonNull ServiceFactoryRegistry factoryRegistry) throws NotFoundException {
		this.repositoryURI = repositoryURI;
		Optional<Dataset> datasetOpt = datasetLoader.loadDataset(repositoryURI);
		if (datasetOpt.isEmpty()) {
			throw new NotFoundException("Could not find repository for: "+repositoryURI);
		}
		this.repoDataset = datasetOpt.get();
		this.repoModel = OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		this.stateKeeperFactory = stateKeeperFactory;
		this.factoryRegistry = factoryRegistry;
		this.datasetLoader = datasetLoader;
	}

	public Dataset getRepositoryDataset() {
		return repoDataset;
	}
	
	public OntModel getRepositoryModel() {
		return repoModel;
	}
	
	public BranchBuilder getInitializedBranchBuilder() {
		return new BranchBuilder(repositoryURI, repoDataset, repoModel);
	}
	
	public Branch getOrLoadBranch(URI branchURI) throws Exception {
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
				branch = new BranchBuilder(repositoryURI, repoDataset)
						.setDataset(datasetOpt.get())
						.setBranchLocalName(BranchBuilder.getBranchNameFromURI(branchURI))
						.setStateKeeper(stateKeeper)
						.build();
				Commit prelimUnfinishedCommit = stateKeeper.loadState();
				registerBranch(branch); // now branch can be found and referenced by other branches
				initializeBranch(branch); // reload incoming, local, outgoing commit handlers
				branch.startCommitHandlers(prelimUnfinishedCommit);
				return branch;
			}
		}
	}

	private void initializeBranch(Branch branch) throws Exception {
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
	
	private CommitHandler resolveHandler(Branch branch, OntIndividual config) throws Exception{
		Statement typeStmt = config.getProperty(AES.isConfigForHandlerType);
		if (typeStmt != null) {
			Optional<ServiceFactory> factory = factoryRegistry.getFactory(typeStmt.getResource().getURI());
			if (factory.isPresent()) {
				try {
					return factory.get().getCommitHandlerInstanceFor(branch, config);
				} catch (Exception e) {
					String msg = String.format("Error creating CommitHandler %s while initializing branch %s: %s", typeStmt.getResource().getURI(), branch.getBranchId(), e.getMessage());
					log.warn(msg);
					throw new Exception(msg);
				}
			} else {
				String msg = String.format("Could not resolve Factory for %s while initializing branch %s", typeStmt.getResource().getURI(), branch.getBranchId());
				log.warn(msg);
				throw new Exception(msg);
			}
		}
		return null;
	}

	public void registerBranch(Branch branch) {
		branches.put(branch.getBranchResource().getURI(), branch);
	}
}

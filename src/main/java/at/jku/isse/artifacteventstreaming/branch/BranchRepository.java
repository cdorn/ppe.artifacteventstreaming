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
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.ResourceFactory;

import java.net.URI;
import java.util.*;

@Slf4j
public class BranchRepository extends CoreBranchRepository {

	private final Map<String, Branch> branches = new HashMap<>();
	private final StateKeeperFactory stateKeeperFactory;

	public BranchRepository(@NonNull URI repositoryURI
			, @NonNull DatasetRepository datasetLoader
			, @NonNull StateKeeperFactory stateKeeperFactory
			, @NonNull ServiceFactoryRegistry factoryRegistry
			, @NonNull MetaModelOntologyProvider metaOntologyProvider
            , @NonNull ObservationRegistry observationRegistry) throws NotFoundException {
		super(repositoryURI, datasetLoader, factoryRegistry, metaOntologyProvider, observationRegistry);
		this.stateKeeperFactory = stateKeeperFactory;
	}

	public class RegisteringBranchBuilder extends BranchBuilder{

 		public RegisteringBranchBuilder(@NonNull URI repositoryURI, @NonNull Dataset repoDataset, @NonNull ObservationRegistry observationRegistry) {
			super(repositoryURI, repoDataset, observationRegistry);
		}
		
		@Override
		public Branch build() {
			var branch = super.build();
			registerBranch(branch);
			return branch;
		}
	}

	public BranchRepository.RegisteringBranchBuilder getInitializedBranchBuilder(String branchName) throws BranchConfigurationException {
		var uri = CoreBranchBuilder.generateBranchURI(repoRes, branchName);
		var datasets = loadBranchDatasets(branchName);
		var builder = new BranchRepository.RegisteringBranchBuilder(repositoryURI, datasets.getValue(), observationRegistry);
		builder.setBranchLocalName(branchName)
				.setDataset(datasets.getKey())
				.setStateKeeper(stateKeeperFactory.createStateKeeperFor(uri))
				.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)
				.setMetaModelOntologyProvider(metaOntologyProvider)
		; // we set the default inference model here statically, can be overridden if necessary

//		var optDataset = datasetLoader.loadDataset(uri);
//		optDataset.ifPresent(builder::setDataset);
		return builder;
	}

	public void registerBranch(Branch branch) {
		branches.put(branch.getBranchResource().getURI(), branch);
	}

	@Override
	public Branch getOrLoadBranch(URI branchURI) throws PersistenceException, BranchConfigurationException {
		Branch branch = branches.get(branchURI.toString());
		if (branch != null) {
			log.debug("Branch {} found in repository cache", branchURI);
			return branch;
		} else {
			Optional<Dataset> datasetOpt = datasetLoader.loadDataset(branchURI);
			Optional<Dataset> metadataOpt = datasetLoader.loadDataset(getMetadataURIfrom(branchURI));
			if (datasetOpt.isEmpty() || metadataOpt.isEmpty()) {
				log.info("Could not find dataset or metadata dataset for: {}", branchURI);
				return null;
			} else {
				var metadata = metadataOpt.get();
				BranchStateUpdater stateKeeper = stateKeeperFactory.createStateKeeperFor(branchURI);
				branch = new BranchBuilder(repositoryURI, metadata, observationRegistry)
						.setDataset(datasetOpt.get())
						.setBranchLocalName(CoreBranchBuilder.getBranchNameFromURI(branchURI))
						.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF) // DEFER: technical debt - we set the inference model here statically, not as part of the configuration. For now we assume all use cases will require this anyway.
						.setMetaModelOntologyProvider(metaOntologyProvider)
						.setStateKeeper(stateKeeper)
						.build();
				registerBranch(branch); // now branch can be found and referenced by other branches

				metadata.begin(ReadWrite.WRITE);
				initializeBranchHandlers(branch); // reload local, in and out commit handlers
				metadata.commit();
				metadata.end();
				branch.startCommitHandlers();
				return branch;
			}
		}
	}

	private void initializeBranchHandlers(Branch branch) throws BranchConfigurationException {
		super.initializeBranchHandlers(branch);

		for (var config : branch.getOutgoingCommitDistributerConfig()) {
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
}

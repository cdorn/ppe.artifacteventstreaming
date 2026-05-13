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
import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.util.*;

@Slf4j
public class CoreBranchRepository {
    protected final URI repositoryURI;
    protected final Resource repoRes;
    private final Dataset repoDataset;
    private final OntModel repoModel;
    protected final DatasetRepository datasetLoader;
    protected final ServiceFactoryRegistry factoryRegistry;
    protected final MetaModelOntologyProvider metaOntologyProvider;
    protected final ObservationRegistry observationRegistry;
    private final Map<String, CoreBranch> branches = new HashMap<>();

    public Dataset getRepositoryDataset() {
        return repoDataset;
    }

    public CoreBranchRepository(@NonNull URI repositoryURI
            , @NonNull DatasetRepository datasetLoader
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
        this.factoryRegistry = factoryRegistry;
        this.datasetLoader = datasetLoader;
        repoDataset.commit();
        repoDataset.end();
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

    public CoreBranch getOrLoadBranch(URI branchURI) throws PersistenceException, BranchConfigurationException {
        CoreBranch branch = branches.get(branchURI.toString());
        if (branch != null) {
            return branch;
        } else {
            Optional<Dataset> datasetOpt = datasetLoader.loadDataset(branchURI);
            Optional<Dataset> metadataOpt = datasetLoader.loadDataset(getMetadataURIfrom(branchURI));
            if (datasetOpt.isEmpty() || metadataOpt.isEmpty()) {
                log.info("Could not find dataset or metadata dataset for: {}", branchURI);
                throw new BranchConfigurationException("Could not find dataset or metadata dataset for: "+ branchURI);
            } else {
                var metadata = metadataOpt.get();
                branch = new CoreBranchBuilder(repositoryURI, metadata, observationRegistry)
                        .setDataset(datasetOpt.get())
                        .setBranchLocalName(CoreBranchBuilder.getBranchNameFromURI(branchURI))
                        .setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF) // DEFER: technical debt - we set the inference model here statically, not as part of the configuration. For now we assume all use cases will require this anyway.
                        .setMetaModelOntologyProvider(metaOntologyProvider)
                        .build();
                registerBranch(branch, null); // now branch can be found and referenced by other branches

                metadata.begin(ReadWrite.WRITE);
                initializeBranchHandlers(branch); // reload local commit handlers
                metadata.commit();
                metadata.end();
                return branch;
            }
        }
    }

    protected URI getMetadataURIfrom(URI branchURI) throws BranchConfigurationException{
        var posHash = branchURI.toString().indexOf('#');
        if (posHash > 0) {
            var baseURI = branchURI.toString().substring(0, posHash);
            return URI.create(baseURI + "/metadata#metadata");
        } else {
            var msg = String.format("Branch URI %s does not contain a fragment identifier (#), cannot determine metadata URI", branchURI);
            log.warn(msg);
            throw new BranchConfigurationException(msg);
        }
    }

    protected void initializeBranchHandlers(CoreBranch branch) throws BranchConfigurationException {
        // we inspect the branch resource for any configuration data
        for (var config : branch.getLocalCommitServiceConfig()) {
            CommitHandler handler = resolveHandler(branch, config);
            if (handler != null) {
                if (handler instanceof IncrementalCommitHandler incrementalCommitHandler) {
                    branch.appendBranchInternalCommitService(incrementalCommitHandler);
                } else {
                    String msg = String.format("Configured CommitHandler %s is not an IncrementalCommitHandler for branch %s", handler.getClass().getName(), branch.getBranchId());
                    log.warn(msg);
                    throw new BranchConfigurationException(msg);
                }
            }
        }
    }

    protected CommitHandler resolveHandler(CoreBranch branch, OntIndividual config) throws BranchConfigurationException {
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

    protected void registerBranch(@NonNull CoreBranch branch, @Nullable String owner) {
        repoDataset.begin(ReadWrite.WRITE);
        var branchResource = repoDataset.getDefaultModel().createResource(branch.getBranchId());
        repoModel.add(branchResource, AES.partOfRepository, repoRes);
        if (owner != null && !owner.isEmpty()) {
            branchResource.addLiteral(AES.ownedBy, owner);
        }
        repoDataset.commit();
        repoDataset.end();

        branches.put(branch.getBranchResource().getURI(), branch);
    }

    public void remove(CoreBranch branch) {
        branch.deactivate();

        // repo overview, remove branch resource and owner
        repoDataset.begin(ReadWrite.WRITE);
        var branchRes = repoDataset.getDefaultModel().getResource(branch.getBranchId());
        branchRes.removeProperties();
        repoDataset.commit();
        repoDataset.end();
        // local cache
        branches.remove(branch.getBranchId());

        //
        branch.getBranchMetadataDataset().begin(ReadWrite.WRITE);
        branch.getBranchMetadataModel().removeAll();
        branch.getBranchMetadataDataset().commit();
        branch.getBranchMetadataDataset().end();

        // branch data
        branch.getDataset().begin(ReadWrite.WRITE);
        //TODO: what to do if there is an ongoing write session with Lock?!
        branch.getModel().removeAll();
        branch.getDataset().commit();
        branch.getDataset().close();

    }

    public class RegisteringBranchBuilder extends CoreBranchBuilder{


        public RegisteringBranchBuilder(@NonNull URI repositoryURI, @Nullable Dataset metadataDataset, @NonNull ObservationRegistry observationRegistry) {
            super(repositoryURI, metadataDataset, observationRegistry);
        }

        @Override
        public CoreBranch build() {
            var branch = super.build();
            registerBranch(branch, owner);

            return branch;
        }
    }

    public CoreBranchRepository.RegisteringBranchBuilder getInitializedCoreBranchBuilder(String branchName) throws BranchConfigurationException {

        var datasets = loadBranchDatasets(branchName);
        var builder = new CoreBranchRepository.RegisteringBranchBuilder(repositoryURI, datasets.getValue(), observationRegistry);
        builder.setBranchLocalName(branchName)
                .setDataset(datasets.getKey())
                .setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)
                .setMetaModelOntologyProvider(metaOntologyProvider)
        ;
        return builder;
    }

    protected Map.Entry<Dataset, Dataset> loadBranchDatasets(String branchName) throws BranchConfigurationException {
        var uri = CoreBranchBuilder.generateBranchURI(repoRes, branchName);
        var metadataURI = getMetadataURIfrom(uri);
        var datasetOpt = datasetLoader.loadDataset(uri);
        var metadataOpt = datasetLoader.loadDataset(metadataURI);
        if (datasetOpt.isEmpty() || metadataOpt.isEmpty()) {
            var msg = String.format("Could not find dataset or metadata dataset for branch %s in repo %s", branchName, repoRes.getURI());
            log.warn(msg);
            throw new BranchConfigurationException(msg);
        }
        return new AbstractMap.SimpleEntry<>(datasetOpt.get(), metadataOpt.get());
    }

    protected URI generateOrThrowIfExists(String branchName) throws BranchConfigurationException {
        var uri = CoreBranchBuilder.generateBranchURI(repoRes, branchName);
        if (doesBranchExist(repoRes, uri)) {
            var msg = String.format("Branch %s already exists in repo %s", branchName, repoRes.getURI());
            log.warn(msg);
            throw new BranchConfigurationException(msg);
        }
        return uri;
    }

    protected boolean doesBranchExist(@NonNull Resource repositoryRes, @NonNull URI branchURI) {
        var inTX = repoDataset.isInTransaction();
        if (!inTX) {
            repoDataset.begin(ReadWrite.READ);
        }

        Resource branchRes = ResourceFactory.createResource(branchURI.toString());
        boolean doesContain = repoDataset.getDefaultModel().contains(branchRes, AES.partOfRepository, repositoryRes);
        if (!inTX) {
            repoDataset.end();
        }
        return doesContain;
    }
}

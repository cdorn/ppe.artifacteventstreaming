package at.jku.isse.artifacteventstreaming.branch;

import at.jku.isse.artifacteventstreaming.api.*;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryEventStore;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import io.micrometer.observation.ObservationRegistry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.*;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class BranchBuilder extends CoreBranchBuilder {

    private final List<CommitHandler> incomingCommitHandlers = new LinkedList<>();
    private final Set<CommitHandler> outgoingCommitDistributers = new HashSet<>();
    protected BranchStateUpdater stateKeeper;

    public BranchBuilder(@NonNull URI repositoryURI, @Nullable Dataset metadataBranchDataset, @NonNull ObservationRegistry observationRegistry) {
        super(repositoryURI, metadataBranchDataset, observationRegistry);
    }

    /**
     * @param repositoryURI       the identifier of the repository we are building this branch in
     * @param observationRegistry
     */
    public BranchBuilder(@NonNull URI repositoryURI, @NonNull ObservationRegistry observationRegistry) {
        super(repositoryURI, null, observationRegistry);
    }

    /**
     * if not used, by default a in memory statekeeper will be used.
     */
    public BranchBuilder setStateKeeper(@NonNull BranchStateUpdater stateKeeper) {
        this.stateKeeper = stateKeeper;
        return this;
    }

    /**
     * if not used, no commits will be merged into this branch
     */
    public BranchBuilder addIncomingCommitMerger(CommitHandler handler) {
        this.incomingCommitHandlers.add(handler);
        return this;
    }

    public BranchBuilder addOutgoingCommitDistributer(CommitHandler distributer) {
        this.outgoingCommitDistributers.add(distributer);
        return this;
    }


    @Override
    public Branch build() {
        if (branchDataset == null) {
            setDataset(DatasetFactory.createTxnMem());
        }
        OntIndividual branchResource = prepareBranch(branchURI, owner);
        BlockingQueue<Commit> inQueue = new LinkedBlockingQueue<>();
        BlockingQueue<Commit> outQueue = new LinkedBlockingQueue<>();
        branchDataset.begin(ReadWrite.WRITE);
        OntModel model = OntModelFactory.createModel(branchDataset.getDefaultModel().getGraph(), modelSpec);
        if (stateKeeper == null) {
            stateKeeper = new StateKeeperImpl(branchURI, new InMemoryBranchStateCache(), new InMemoryEventStore());
        }

        BranchImpl branch = new BranchImpl(branchDataset, model, branchResource,
                metadataModel, metadataBranchDataset
                ,stateKeeper, inQueue, outQueue, timeStampProvider, observationRegistry);
        if (metaOntologyProvider != null) {
            branch.setSchemaUtils(new MetaModelSchemaTypes(model, metaOntologyProvider.getMetaModelOntology()));
        }
        addCommitHandlers(branch);
        branchDataset.commit();
        branchDataset.end();
        return branch;
    }


    private void addCommitHandlers(Branch branch) {
        incomingCommitHandlers.stream().forEach(branch::appendIncomingCommitMerger);
        services.stream().forEach(branch::appendBranchInternalCommitService);
        outgoingCommitDistributers.stream().forEach(branch::appendOutgoingCommitDistributer);

    }

    @Override
    public BranchBuilder setBranchLocalName(@NonNull String branchName) throws BranchConfigurationException {
        super.setBranchLocalName(branchName);
        return this;
    }

    @Override
    public BranchBuilder setBranchURI(@NonNull URI branchURI) throws BranchConfigurationException {
        super.setBranchURI(branchURI);
        return this;
    }

    @Override
    public BranchBuilder setDataset(@NonNull Dataset dataset) {
        super.setDataset(dataset);
        return this;
    }

    @Override
    public BranchBuilder setModelReasoner(@NonNull OntSpecification spec) {
        super.setModelReasoner(spec);
        return this;
    }

    @Override
    public BranchBuilder setTimeStampProvider(@NonNull TimeStampProvider timeStampProvider) {
        super.setTimeStampProvider(timeStampProvider);
        return this;
    }

    @Override
    public BranchBuilder addBranchInternalCommitService(IncrementalCommitHandler service) {
        super.addBranchInternalCommitService(service);
        return this;
    }

    @Override
    public BranchBuilder addOwner(String ownerId) {
        super.addOwner(ownerId);
        return this;
    }

    @Override
    public BranchBuilder setMetaModelOntologyProvider(MetaModelOntologyProvider metaOntologyProvider) {
        super.setMetaModelOntologyProvider(metaOntologyProvider);
        return this;
    }
}

package at.jku.isse.artifacteventstreaming.branch;

import at.jku.isse.artifacteventstreaming.api.*;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.shared.Lock;
import org.apache.jena.sparql.core.Transactional;

import java.util.*;
import java.util.concurrent.BlockingQueue;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CoreBranchImpl implements CoreBranch {

    @Getter protected final Dataset dataset;
    @Getter protected final OntModel model;
    @Getter protected final OntIndividual branchResource;
    protected final String branchResourceURI;
    protected final String branchResourceLabel;
    @Getter protected final Dataset branchMetadataDataset;
    @Getter protected final OntModel branchMetadataModel;

    protected final TimeStampProvider timeStampProvider;
    protected final ObservationRegistry observationRegistry;

    private String lastCommitId;
    protected final Map<String, CommitHandler> handlers = Collections.synchronizedMap(new LinkedHashMap<>());

    protected final StatementAggregator stmtAggregator = new StatementAggregator();
    protected final Map<String, IncrementalCommitHandler> services = Collections.synchronizedMap(new LinkedHashMap<>());

    protected final AtomicBoolean isReady = new AtomicBoolean(false);

    @Getter @Setter MetaModelSchemaTypes schemaUtils;

    public CoreBranchImpl(@NonNull Dataset dataset
            , @NonNull OntModel model
            , @NonNull OntIndividual branchResource
            , @NonNull OntModel metadataModel
            , @NonNull Dataset metadataBranchDataset
            , @NonNull TimeStampProvider timeStampProvider
            , @NonNull ObservationRegistry observationRegistry) {
        super();
        this.observationRegistry = observationRegistry;
        this.dataset = dataset;
        this.model = model;
        this.branchResource = branchResource;
        this.branchResourceURI = branchResource.getURI(); // cached so we wont have to access different dataset
        this.branchResourceLabel = branchResource.getLocalName();
        this.branchMetadataDataset = metadataBranchDataset;
        this.branchMetadataModel = metadataModel;
        this.timeStampProvider = timeStampProvider;
        stmtAggregator.registerWithModel(model);
    }

    @Override
    public void startCommitHandlers() throws BranchConfigurationException, PersistenceException {
        // if we have collected any model changes until here, they would have come from setup logic that we do not persist in commits,
        // hence we clear the statement aggregator first
        stmtAggregator.retrieveAddedStatements();
        stmtAggregator.retrieveRemovedStatements();
        isReady.set(true);
    }

    @Override
    public void deactivate() {
        isReady.set(false);
    }

    @Override
    public String getBranchId() {
        return branchResourceURI;
    }

    @Override
    public String getBranchName() {
        return branchResourceLabel;
    }

    @Override
    public String getRepositoryURI() {
        return branchResource.getProperty(AES.partOfRepository).getResource().getURI();
    }

    @Override
    public String toString() {
        return "Branch[" + branchResourceLabel + "]";
    }

    // local changes handling ---------------------------------------------------------------

    public Set<IncrementalCommitHandler> getRegisteredLocalCommitHandlers() {
        return new HashSet<>(services.values());
    }

    @Override
    public void appendBranchInternalCommitService(@NonNull IncrementalCommitHandler service) {
        Seq configs = this.createOrGetListResource(AES.localCommitService);
        if (services.containsKey(service.getURI())) {
            var serviceKeys = services.keySet().stream().toList();
            int pos = serviceKeys.indexOf(service.getURI());
            configs.remove(pos+1); // RDF are 1-indexed!
        }
        services.put(service.getURI(), service);
        // ensure we only add if there is no such handler yet (we might just add handler here from persisted config)
        var configNode = service.getConfigResource();
        if (configs.indexOf(configNode) <= 0) { // RDF lists are 1-indexed
            configs.add(configNode);
        }
    }


    @Override
    public void removeBranchInternalCommitService(@NonNull IncrementalCommitHandler service) {
        Seq configs = this.createOrGetListResource(AES.localCommitService);
        if (services.containsKey(service.getURI())) {
            var serviceKeys = services.keySet().stream().toList();
            int pos = serviceKeys.indexOf(service.getURI());
            configs.remove(pos+1); // RDF are 1-indexed!
            services.remove(service.getURI());
        }
    }

    @Override
    public List<OntIndividual> getLocalCommitServiceConfig() {
        Seq list = createOrGetListResource(AES.localCommitService);
        return fromSeqResourceToContent(list);
    }

    // Transaction handling
    @Override
    public void startReadTransaction() {
        Observation.createNotStarted("rdfbackend.transaction.readstart", observationRegistry)
                .highCardinalityKeyValue("branch.id", getBranchId())
                .observe(() -> dataset.begin(TxnType.READ_PROMOTE)); // it needs to be READ_PROMOTE to ensure detection when promotion needs to get a new transaction (see promoteToWriteTransaction() )
    }

    @Override
    public void completeReadTransaction() {
        Observation.createNotStarted("rdfbackend.transaction.readend", observationRegistry)
                .highCardinalityKeyValue("branch.id", getBranchId())
                .observe(dataset::end);
    }

    @Override
    public Lock promoteToWriteTransaction() {
        if (!dataset.isInTransaction()) {
            // we cannot promote a non-existing transaction
            return null;
        }
        if (dataset.transactionMode().equals(ReadWrite.WRITE)) {
            // already in a write transaction
            return null;
        } else {
            // promote
            var writeLock = dataset.getLock();
            writeLock.enterCriticalSection(false);
            var isPromoted = dataset.promote();
            if (isPromoted) {
                return writeLock;
            } else {
                // still in read mode
                dataset.end();
                // start a new write transaction
                dataset.begin(ReadWrite.WRITE);
                return writeLock;
            }
        }
    }

    @Override
    public Lock startWriteTransaction() {
        return Observation.createNotStarted("rdfbackend.transaction.writestart", observationRegistry)
                .highCardinalityKeyValue("branch.id", getBranchId())
                .observe(() -> {
                    var writeLock = dataset.getLock();
                    writeLock.enterCriticalSection(false);
                    dataset.begin(ReadWrite.WRITE);
                    return writeLock;
                });
    }

    @Override
    public void abortWriteTransaction(@NonNull Lock lock) {
        Observation.createNotStarted("rdfbackend.transaction.writeabort", observationRegistry)
                .highCardinalityKeyValue("branch.id", getBranchId())
                .observe(() -> {
                    dataset.abort();
                    dataset.end();
                    lock.leaveCriticalSection();
                });
    }

    @Override
    public Commit concludeTransaction(@NonNull Lock writeLock, String commitMsg) throws BranchConfigurationException, PersistenceException {
        Commit commit = null;
        if (dataset.transactionMode() != null && dataset.transactionMode().equals(ReadWrite.WRITE)) {
            try {
                commit = this.commitChanges(commitMsg);
                // dataset write transaction end set by commitChanges() logic
            } finally {
                writeLock.leaveCriticalSection();
            }
        } else {
            dataset.end();
            writeLock.leaveCriticalSection();
        }
        return commit;
    }

    /**
     * assumes no other tread is making changes to the model while services are processing
     */
    @Override
    public Commit commitChanges(String commitMsg) throws BranchConfigurationException, PersistenceException {
        if (!isReady.get()) {
            this.undoNoncommitedChanges();
            throw new BranchConfigurationException(String.format("Branch %s with objectid %s has been deactivated, cannot make changes on non-active branch, please create a new branch object", getBranchId(), this.hashCode()));
        }
        return Observation.createNotStarted("rdfbackend.transaction.writecommit", observationRegistry)
                .highCardinalityKeyValue("branch.id", getBranchId())
                .observeChecked(() -> {
                    if (!stmtAggregator.hasAdditions() && !stmtAggregator.hasRemovals()) {
                        log.debug("Commit not created as no changes occurred since last commit", lastCommitId);
                        // we still are expected to be in a transaction, hence close the transaction here
                        if (dataset.isInTransaction()) {
                            dataset.abort();
                            dataset.end();
                        }
                        return null;
                    } else {
                        var commit = new StatementCommitImpl(branchResourceURI, commitMsg, lastCommitId, timeStampProvider.getCurrentTimeStamp(), stmtAggregator.retrieveAddedStatements(), stmtAggregator.retrieveRemovedStatements());
                        handleCommitInternally(commit);
                        return commit;
                    }
                });
    }

    protected void handleCommitInternally(Commit commit) throws PersistenceException {
        log.debug("Handling commit {} in branch {}", commit.getCommitId(), branchResourceURI);
        // clear the changes
            if (!services.isEmpty() && !commit.isEmpty()) {
                executeServiceLoop(commit);
            }
            // persist augmented commit and  mark preliminary commit as processed
            Observation.createNotStarted("rdfbackend.transaction.postservicecommit", observationRegistry)
                    .highCardinalityKeyValue("branch.id", getBranchId())
                    .observeChecked(() -> {
                        log.debug("Branch {} contains now {} statements", branchResourceLabel, model.size());
                        dataset.commit(); // together with commit persistence
                        dataset.end();
                        lastCommitId = commit.getCommitId();
                    });

    }

    private void executeServiceLoop(Commit commit) {
//        Observation.createNotStarted("rdfbackend.transaction.preservicecommit", observationRegistry)
//                .highCardinalityKeyValue("branch.id", getBranchId())
//                .observe(() -> {
//                    try { // first persist initial commit
//                        //stateKeeper.beforeServices(commit);
//                        dataset.commit(); // together with commit/events persistence, here persists state of model
//                    } catch (Exception e) {
//                        log.info(String.format("Failed to persist pre-service commit %s %s with exception %s", commit.getCommitMessage(), commit.getCommitId(), e.getMessage()));
//                    } finally {
//                        dataset.end();
//                    }
//                });
//        Observation.createNotStarted("rdfbackend.transaction.serviceprepare", observationRegistry)
//                .highCardinalityKeyValue("branch.id", getBranchId())
//                .observe(() -> {
//                    dataset.begin(ReadWrite.WRITE);
//                    // we now have the local changes persisted and have a restart point established
//                    // next we iterated through services
//                });

        int baseAdds = commit.getAdditionCount();
        int baseRemoves = commit.getRemovalCount();
        int addsCount = baseAdds;
        int removesCount = baseRemoves;
        int newAdds = 0;
        int newRemoves = 0;
        int rounds = 0;
        // store for each service the last seen offset
        Map<IncrementalCommitHandler, Integer> offsetAdds = initServiceOffsets();
        Map<IncrementalCommitHandler, Integer> offsetRemoves = initServiceOffsets();
        Integer perIterationAdds = 0;
        Integer perIterationsRemovals = 0;
        do {
            perIterationAdds = 0;
            perIterationsRemovals = 0;
            for (IncrementalCommitHandler service : services.values()) {
                Observation.createNotStarted("rdfbackend.transaction.servicerun", observationRegistry)
                        .highCardinalityKeyValue("branch.id", getBranchId())
                        .highCardinalityKeyValue("service.id", service.getURI())
                        .observe(() ->
                                        service.handleCommitFromOffset(commit, offsetAdds.get(service), offsetRemoves.get(service))
                                // any changes by a service are now in the statement lists
                        );
                // provide changes immediately to next service:
                commit.appendAddedStatements(stmtAggregator.retrieveAddedStatements());
                newAdds = commit.getAdditionCount() - addsCount;
                addsCount = commit.getAdditionCount();
                perIterationAdds += newAdds;

                commit.appendRemovedStatement(stmtAggregator.retrieveRemovedStatements());
                newRemoves = commit.getRemovalCount() - removesCount;
                removesCount = commit.getRemovalCount();
                perIterationsRemovals += newRemoves;

                // store these changes as seen by this service (and also consider those produced by this service)
                offsetAdds.put(service, commit.getAdditionCount());
                offsetRemoves.put(service, commit.getRemovalCount());

            }
            rounds++;
            //continue while new changes happen and max 100 rounds to avoid infinite loops
        } while ((perIterationAdds > 0 || perIterationsRemovals > 0) && rounds < 100);

        if ((perIterationAdds > 0 || perIterationsRemovals > 0) && rounds >= 100) {
            log.warn(String.format("Service loop for commit '%s' reached maximum iteration count of 100 while still new statements available", commit.getCommitMessage()));
        }
        commit.removeEffectlessStatements(baseAdds, baseRemoves);


        if (commit.isEmpty()) {
            log.info(String.format("Commit %s of branch %s has no changes after local service processing", commit.getCommitId(), branchResourceURI));
        }
    }

    private Map<IncrementalCommitHandler, Integer> initServiceOffsets() {
        Map<IncrementalCommitHandler, Integer> offsets = new HashMap<>();
        services.values().stream().forEach(service -> offsets.put(service, 0));
        return offsets;
    }

    @Override
    public void undoNoncommitedChanges() {
        if (dataset.isInTransaction()) {
            dataset.abort();
            dataset.end();
        }
        stmtAggregator.retrieveAddedStatements();
        stmtAggregator.retrieveRemovedStatements();
    }

    protected Seq createOrGetListResource(Property refToList) {
        Resource listResource = branchResource.getPropertyResourceValue(refToList);
        Seq list;
        if (listResource == null) {
            list = branchMetadataModel.createSeq(branchResource.getURI()+"#"+refToList.getLocalName());
            branchResource.addProperty(refToList, list);
        } else {
            list = branchMetadataModel.getSeq(listResource);
        }
        return list;
    }

    protected List<OntIndividual> fromSeqResourceToContent(Seq list) {
        NodeIterator iter = list.iterator();
        List<OntIndividual> elements = new ArrayList<>();
        while(iter.hasNext()) {
            var uri = iter.next().asResource().getURI();
            var el = branchMetadataModel.getIndividual(uri);
            elements.add(el);
        }
        return elements;
    }
}

package at.jku.isse.artifacteventstreaming.branch;

import at.jku.isse.artifacteventstreaming.api.*;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.outgoing.CrossBranchStreamer;
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
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.shared.Lock;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class BranchImpl  implements Branch, Runnable {

	private final Dataset dataset;
	private final OntModel model;
	private final OntIndividual branchResource;
	private final String branchResourceURI;
	private final String branchResourceLabel;
	@Getter private final BranchStateUpdater stateKeeper;
	private final TimeStampProvider timeStampProvider;
    private final ObservationRegistry observationRegistry;
	
	@Getter private final BlockingQueue<Commit> inQueue;
	private final Map<String, CommitHandler> handlers = Collections.synchronizedMap(new LinkedHashMap<>());
	private final ExecutorService inExecutor = Executors.newSingleThreadExecutor();
	private final ExecutorService outExecutor = Executors.newSingleThreadExecutor();
	
	private final StatementAggregator stmtAggregator = new StatementAggregator();
	private final Map<String, IncrementalCommitHandler> services = Collections.synchronizedMap(new LinkedHashMap<>());
	@Getter private final BlockingQueue<Commit> outQueue;
	private final CrossBranchStreamer crossBranchStreamer;
	private final AtomicBoolean isReady = new AtomicBoolean(false);
	
	@Getter @Setter MetaModelSchemaTypes schemaUtils;
	
	public BranchImpl(@NonNull Dataset dataset
			, @NonNull OntModel model
			, @NonNull OntIndividual branchResource
			, @NonNull BranchStateUpdater stateKeeper
			, @NonNull BlockingQueue<Commit> inQueue
			, @NonNull BlockingQueue<Commit> outQueue
			, @NonNull TimeStampProvider timeStampProvider
            , @NonNull ObservationRegistry observationRegistry) {
		super();
        this.observationRegistry = observationRegistry;
		this.dataset = dataset;
		this.model = model;
		this.branchResource = branchResource;
		this.branchResourceURI = branchResource.getURI(); // cached so we wont have to access different dataset
		this.branchResourceLabel = branchResource.getLocalName();
		this.stateKeeper = stateKeeper;
		this.inQueue = inQueue;
		this.outQueue = outQueue;
		this.timeStampProvider = timeStampProvider;
		this.crossBranchStreamer = new CrossBranchStreamer(branchResource.getURI(), stateKeeper, outQueue);
		stmtAggregator.registerWithModel(model);
	}
	
	@Override
	public void startCommitHandlers(Commit unfinishedPreliminaryCommit) throws BranchConfigurationException, PersistenceException {
		// if we have collected any model changes until here, they would have come from setup logic that we do not persist in commits,
		// hence we clear the statement aggregator first
		stmtAggregator.retrieveAddedStatements();
		stmtAggregator.retrieveRemovedStatements();
		
		if (unfinishedPreliminaryCommit != null) {
			// continue aborted processing: e.g., single internal commit not yet completely processed by services (can only be one)
			dataset.begin();
			this.handleCommitInternally(unfinishedPreliminaryCommit);
		}
		// re-forward all nonforwarded commits
		crossBranchStreamer.recoverState();
		// then recover all inqueued but not yet processed commits
		for (Commit nonMergedCommit : stateKeeper.getNonMergedCommits()) {
			this.enqueueIncomingCommit(nonMergedCommit);
		}
		// create thread for incoming commits to be merged
		inExecutor.execute(this);
		outExecutor.execute(crossBranchStreamer);		
		isReady.set(true);
	}
	
	@Override
	public void deactivate() {
		isReady.set(false);
		inQueue.add(PoisonPillCommit.POISONPILL);
		outQueue.add(PoisonPillCommit.POISONPILL);
		
	}
	
	@Override
	public OntModel getModel() {
		return model;
	}
	
	@Override
	public Dataset getDataset() {
		return dataset;
	}

	@Override
	public Commit getLastCommit() {
		return stateKeeper.getLastCommit().orElse(null);
	}
	
	private String getLastCommitId() {
		return getLastCommit() != null ? getLastCommit().getCommitId() : "";
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
	public OntIndividual getBranchResource() {
		return branchResource;
	}
	
	@Override
	public String getRepositoryURI() {
		return branchResource.getProperty(AES.partOfRepository).getResource().getURI(); 
	}
	
    @Override
	public String toString() {
		return "Branch[" + branchResourceLabel + "]";
	}
	
	// incoming commit handling -------------------------------------------------------------------------

	@Override
	public List<OntIndividual> getIncomingCommitHandlerConfig() {
		Seq list = createOrGetListResource(AES.incomingCommitMerger);
		return fromSeqResourceToContent(list);
	}
	
	@Override
	public void appendIncomingCommitMerger(@NonNull CommitHandler handler) {
		Seq configs = this.createOrGetListResource(AES.incomingCommitMerger);
		if (handlers.containsKey(handler.getURI())) {
            var handlerKeys = handlers.keySet().stream().toList();
            int pos = handlerKeys.indexOf(handler.getURI());
			configs.remove(pos+1); // RDF are 1-indexed!
		}
		handlers.put(handler.getURI(), handler);
        // ensure we only add if there is no such handler yet
		var configNode = handler.getConfigResource();
        if (configs.indexOf(configNode) <= 0) { // RDF lists are 1-indexed
            configs.add(configNode);
        }
		if (isShutdown) {
			isShutdown = false;
			inExecutor.execute(this);	
		}
	}
	
	@Override
	public void removeIncomingCommitMerger(@NonNull CommitHandler handler) {
		Seq configs = this.createOrGetListResource(AES.incomingCommitMerger);
        if (handlers.containsKey(handler.getURI())) {
            var handlerKeys = handlers.keySet().stream().toList();
            int pos = handlerKeys.indexOf(handler.getURI());
            configs.remove(pos+1); // RDF are 1-indexed!
            handlers.remove(handler.getURI());
        }
		if (handlers.isEmpty() && !isShutdown) { 
			log.debug(String.format("Shutting down inQueue thread for branch: %s", this.getBranchName()));
			inQueue.add(PoisonPillCommit.POISONPILL);
			// this stop dequeuing of commits, restarted upon anning one again
		}
		
	}

    @Override
	public void enqueueIncomingCommit(Commit commit) throws BranchConfigurationException, PersistenceException {
		// if we have processed this commit before, then wont do it again to avoid loops		
		if (!stateKeeper.hasSeenCommit(commit) && !inQueue.contains(commit)) {									
			//persist which commits we have received but not merged yet, 
			stateKeeper.beforeMerge(commit);
			// we also ensure that they dont map to prior (brach external) model here
			commit.getAddedStatementsAsSet().stream().forEach(stmt -> stmt.transferToModel(model));
			commit.getRemovedStatementsAsSet().stream().forEach(stmt -> stmt.transferToModel(model));
			// if this crashes before returning this call, then cross branch streamer has to assume failure and retry adding/enqueuing upon restart
			if (handlers.isEmpty()) { // there are not handlers to process , thus no queuing and error thrown
				String msg = String.format("Branch %s received incomming commit %s to merge but no merge handlers are registered, dropping commit", this.getBranchName(), commit.getCommitId());
				log.warn(msg);
				throw new BranchConfigurationException(msg);				
			}
			inQueue.add(commit);
		} else {
			log.info(String.format("Ignoring incoming commit %s that has been seen by this branch before", commit.getCommitId()));
		}
	}

    private boolean isShutdown = false;
	
	public void run() {
		try {
            while (true) {
            	//FIXME: only do this if no service is active, and no changes have happened: how to ensure this?
            	// --> stay within transaction, but lock before change? has problem that if there was another write, we need to end the transaction first, 
            	// and create a new one which again might result in a change between service invokations and iterations
            	Commit commit = inQueue.take();
                if (commit == PoisonPillCommit.POISONPILL) { // shutdown signal
                	log.info(String.format("Received shutdown command for branch %s", this.getBranchId()));
                	isShutdown = true;
                	inExecutor.shutdown();
                	return;
                } else {
                	log.info(String.format("Processing incoming commit %s on branch %s ", commit.getCommitId(), this.getBranchId()));
                	forwardCommit(commit);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	}
	
	private void forwardCommit(Commit commit) {					
	//	if (dataset.isInTransaction()) // this is too brittle, we need to ensure this is not happening by other means
	//		dataset.abort();
		//TODO: obtain a write lock here, otherwise we might interfer with regular commit operation
		dataset.begin(ReadWrite.WRITE);
		handlers.values().stream().forEach(handler -> handler.handleCommit(commit));
//		dataset.commit(); this is done by internal commit handler
//		dataset.end();
		// now we signal the internal commit router that these changes were due to a commit merge 
		// (as we want to maintain commit history)
		try {
			this.commitMergeOf(commit);
			stateKeeper.finishedMerge(commit);
		} catch (Exception e) {
			log.warn(String.format("Merge of commit %s into %s failed with: %s", commit.getCommitId(), this.getBranchId() ,e.getMessage()));
		}
	}
	
	
	// local changes handling ---------------------------------------------------------------

    @Override
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
        // ensure we only add if there is no such handler yet
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
                .observe(() -> dataset.begin(ReadWrite.READ));
	}

	@Override
	public void completeReadTransaction() {
        Observation.createNotStarted("rdfbackend.transaction.readend", observationRegistry)
                .highCardinalityKeyValue("branch.id", getBranchId())
                .observe(dataset::end);
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
	public Commit concludeTransaction(@NonNull Lock writeLock, String commitMsg) {
        return Observation.createNotStarted("rdfbackend.transaction.writecommit", observationRegistry)
                .highCardinalityKeyValue("branch.id", getBranchId())
                .observe(() -> {
                    Commit commit = null;
                    if (dataset.transactionMode() != null && dataset.transactionMode().equals(ReadWrite.WRITE)) {
                        try {
                            commit = this.commitChanges(commitMsg);
                            // dataset write transaction end set by commitChanges() logic
                        } catch (PersistenceException | BranchConfigurationException e) {
                            log.error("Failed to persist commit: " + commitMsg, e);
                        } finally {
                            writeLock.leaveCriticalSection();
                        }
                    } else {
                        dataset.end();

                    }
                    return commit;
                });
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
		
		if (!stmtAggregator.hasAdditions() && !stmtAggregator.hasRemovals()) {
			log.debug("Commit not created as no changes occurred since last commit: "+getLastCommitId());
			// we still are expected to be in a transaction, hence close the transaction here
			if (dataset.isInTransaction()) {
				dataset.abort();
				dataset.end();
			}
			return null;
		} else {
			var commit = new StatementCommitImpl( branchResourceURI , commitMsg, getLastCommitId(), timeStampProvider.getCurrentTimeStamp(), stmtAggregator.retrieveAddedStatements(), stmtAggregator.retrieveRemovedStatements());
			handleCommitInternally(commit);
			outQueue.add(commit); 
			return commit;
		}
	}
	
	@Override
	public Commit commitMergeOf(Commit mergedCommit) throws PersistenceException {
		//we always create a local commit upon a merge to signal that we received and processed that commit
		var commit = new StatementCommitImpl( branchResourceURI , mergedCommit.getCommitId(), mergedCommit.getCommitMessage(), getLastCommitId(), timeStampProvider.getCurrentTimeStamp(), stmtAggregator.retrieveAddedStatements(), stmtAggregator.retrieveRemovedStatements());
		if (commit.isEmpty()) {
			log.info("MergeCommit {} merged into branch {} has no changes after incoming processing", commit.getCommitId(), this.branchResource.getURI());
		}
		handleCommitInternally(commit);
		outQueue.add(commit); 
		return commit;
		
	}
	
	private void handleCommitInternally(Commit commit) throws PersistenceException {
		log.debug("Handling commit {} in branch {}", commit.getCommitId(), branchResourceURI);
		// clear the changes

        try {
			if (!services.isEmpty() && !commit.isEmpty()) {
				executeServiceLoop(commit);
			}
		// persist augmented commit and  mark preliminary commit as processed
            Observation.createNotStarted("rdfbackend.transaction.postservicecommit", observationRegistry)
                    .highCardinalityKeyValue("branch.id", getBranchId())
                    .observeChecked(() -> {
                        stateKeeper.afterServices(commit);
                        log.debug("Branch {} contains now {} statements", branchResourceLabel, model.size());
                        dataset.commit(); // together with commit persistence
                    });
		} catch (Exception e) {
			log.warn("Failed to persist post-service commit {} {} with exception {}", commit.getCommitMessage(), commit.getCommitId(), e.getMessage(), e);
			//SHOULD WE: rethrow e to signal that we cannot continue here as we would lose persisted commit history.
			undoNoncommitedChanges();
			throw e; // if so, then we need to abort transaction before rethrowing
		} finally {
			dataset.end();
		}
	}
	
	private void executeServiceLoop(Commit commit) {
        Observation.createNotStarted("rdfbackend.transaction.preservicecommit", observationRegistry)
                .highCardinalityKeyValue("branch.id", getBranchId())
                .observe(() -> {
                    try { // first persist initial commit
                        stateKeeper.beforeServices(commit);
                        dataset.commit(); // together with commit/events persistence, here persists state of model
                    } catch (Exception e) {
                        log.info(String.format("Failed to persist pre-service commit %s %s with exception %s", commit.getCommitMessage(), commit.getCommitId(), e.getMessage()));
                    } finally {
                        dataset.end();
                    }
                });
        Observation.createNotStarted("rdfbackend.transaction.serviceprepare", observationRegistry)
                .highCardinalityKeyValue("branch.id", getBranchId())
                .observe(() -> {
                    dataset.begin(ReadWrite.WRITE);
                    // we now have the local changes persisted and have a restart point established
                    // next we iterated through services
                });

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

	@Override
	public void appendOutgoingCommitDistributer(@NonNull CommitHandler crossBranchHandler) {
		crossBranchStreamer.addOutgoingCommitHandler(crossBranchHandler);
		Seq configs = this.createOrGetListResource(AES.outgoingCommitDistributer);
		configs.add(crossBranchHandler.getConfigResource());
	}

	@Override
	public void removeOutgoingCommitDistributer(@NonNull CommitHandler crossBranchHandler) {		
		crossBranchStreamer.removeOutgoingCommitHandler(crossBranchHandler);
		Seq configs = this.createOrGetListResource(AES.outgoingCommitDistributer);
		int pos = configs.indexOf(crossBranchHandler.getConfigResource());
		if (pos > 0) {
			configs.remove(pos);
		}
	}

	@Override
	public List<OntIndividual> getOutgoingCommitDistributerConfig() {
		Seq list = createOrGetListResource(AES.outgoingCommitDistributer);
		return fromSeqResourceToContent(list);
	}

	private Seq createOrGetListResource(Property refToList) {
		Resource listResource = branchResource.getPropertyResourceValue(refToList);
		Seq list;
		if (listResource == null) {
			list = branchResource.getModel().createSeq(branchResource.getURI()+"#"+refToList.getLocalName());
			branchResource.addProperty(refToList, list);
		} else {
			list = branchResource.getModel().getSeq(listResource);
			//list = (Seq)listResource;
		}
		return list;
	}
	
	private List<OntIndividual> fromSeqResourceToContent(Seq list) {
		NodeIterator iter = list.iterator();
		List<OntIndividual> elements = new ArrayList<>();
		while(iter.hasNext()) {
			elements.add(branchResource.getModel().getIndividual(iter.next().asResource().getURI()));
		}
		return elements;
	}

}

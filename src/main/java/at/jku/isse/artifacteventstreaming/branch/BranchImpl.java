package at.jku.isse.artifacteventstreaming.branch;

import at.jku.isse.artifacteventstreaming.api.*;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.outgoing.RecoveringCrossBranchStreamer;
import io.micrometer.observation.ObservationRegistry;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Seq;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class BranchImpl extends CoreBranchImpl implements Branch, Runnable {

	private final ExecutorService inExecutor = Executors.newSingleThreadExecutor();
	private final ExecutorService outExecutor = Executors.newSingleThreadExecutor();
	private final RecoveringCrossBranchStreamer crossBranchStreamer;
	@Getter private final BlockingQueue<Commit> inQueue;
	@Getter private final BlockingQueue<Commit> outQueue;
	@Getter final BranchStateUpdater stateKeeper;

	public BranchImpl(@NonNull Dataset dataset
			, @NonNull OntModel model
			, @NonNull OntIndividual branchResource
			, @NonNull BranchStateUpdater stateKeeper
			, @NonNull BlockingQueue<Commit> inQueue
			, @NonNull BlockingQueue<Commit> outQueue
			, @NonNull TimeStampProvider timeStampProvider
            , @NonNull ObservationRegistry observationRegistry) {
		super(dataset, model, branchResource, timeStampProvider, observationRegistry);
		this.stateKeeper = stateKeeper;
		this.inQueue = inQueue;
		this.outQueue = outQueue;
		this.crossBranchStreamer = new RecoveringCrossBranchStreamer(outQueue, branchResource.getURI(), stateKeeper);
	}

	@Override
	public Commit getLastCommit() {
		return stateKeeper.getLastCommit().orElse(null);
	}

	protected String getLastCommitId() {
		return getLastCommit() != null ? getLastCommit().getCommitId() : "";
	}

	@Override
	public void startCommitHandlers() throws BranchConfigurationException, PersistenceException {
		super.startCommitHandlers();
		isReady.set(false);

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
		super.deactivate();
		inQueue.add(PoisonPillCommit.POISONPILL);
		outQueue.add(PoisonPillCommit.POISONPILL);
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

	/**
	 * @param mergedCommit
	 * behaves like for a local commitTransaction, except that it takes the merged commit content as base,
	 * and before persisting splits the commit into base commit and local augmentation
	 * originating branch becomes the local branch (as this is now in the history of the local branch)
	 * @return the augmentation commit by any service additions, if no augmentation, returns merged commit
	 * @throws PersistenceException
	 */
	private Commit commitMergeOf(Commit mergedCommit) throws PersistenceException {
		//we always create a local commit upon a merge to signal that we received and processed that commit
		var commit = new StatementCommitImpl( branchResourceURI , mergedCommit.getCommitId(), mergedCommit.getCommitMessage(), getLastCommitId(), timeStampProvider.getCurrentTimeStamp(), stmtAggregator.retrieveAddedStatements(), stmtAggregator.retrieveRemovedStatements());
		if (commit.isEmpty()) {
			log.info("MergeCommit {} merged into branch {} has no changes after incoming processing", commit.getCommitId(), this.branchResource.getURI());
		}
		handleCommitInternally(commit);
		outQueue.add(commit); 
		return commit;
		
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

	@Override
	public Commit commitChanges(String commitMsg) throws PersistenceException, BranchConfigurationException {
		var commit = super.commitChanges(commitMsg);
		if (commit != null) {
			stateKeeper.afterServices(commit);

			outQueue.add(commit);
		}
		return commit;
	}

}

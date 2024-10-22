package at.jku.isse.artifacteventstreaming.branch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchInternalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.StateKeeper;
import at.jku.isse.artifacteventstreaming.branch.outgoing.CrossBranchStreamer;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BranchImpl  implements Branch, Runnable {

	private final Dataset dataset;
	private final OntModel model;
	private final OntIndividual branchResource;
	private final StateKeeper stateKeeper;
	
	@Getter private final BlockingQueue<Commit> inQueue;
	private final List<CommitHandler> handlers = Collections.synchronizedList(new LinkedList<>());
	private final ExecutorService inExecutor = Executors.newSingleThreadExecutor();
	private final ExecutorService outExecutor = Executors.newSingleThreadExecutor();
	
	private StatementAggregator stmtAggregator = new StatementAggregator();
	private final List<BranchInternalCommitHandler> services = Collections.synchronizedList(new LinkedList<>());
	@Getter private final BlockingQueue<Commit> outQueue;
	private final CrossBranchStreamer crossBranchStreamer;
	
	public BranchImpl(@NonNull Dataset dataset
			, @NonNull OntModel model
			, @NonNull OntIndividual branchResource
			, @NonNull StateKeeper stateKeeper
			, @NonNull BlockingQueue<Commit> inQueue
			, @NonNull BlockingQueue<Commit> outQueue			
			) {
		super();
		this.dataset = dataset;
		this.model = model;
		this.branchResource = branchResource;
		this.stateKeeper = stateKeeper;
		this.inQueue = inQueue;
		this.outQueue = outQueue;
		this.crossBranchStreamer = new CrossBranchStreamer(branchResource.getURI(), outQueue);
		model.register(stmtAggregator);
	}
	
	@Override
	public void startCommitHandlers() {
		// create thread for incoming commits to be merged
		inExecutor.execute(this);
		outExecutor.execute(crossBranchStreamer);
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
		return stateKeeper.getLastCommit();
	}
	
	private String getLastCommitId() {
		return getLastCommit() != null ? getLastCommit().getCommitId() : "";
	}

	@Override
	public String getBranchId() {
		return branchResource.getURI();
	}

	@Override
	public String getBranchName() {
		return branchResource.getLabel();
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
		return "Branch[" + branchResource.getLabel() + "]";
	}
    
	private Seq createOrGetListResource(Property refToList) {
		Resource listResource = branchResource.getPropertyResourceValue(refToList);
		Seq list;
		if (listResource == null) {
			list = branchResource.getModel().createSeq(branchResource.getURI()+"#"+refToList.getLocalName());
		} else {
			list = (Seq)listResource;
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
	
	// incoming commit handling -------------------------------------------------------------------------
	
	@Override
	public void enqueueIncomingCommit(Commit commit) {
		// if we have processed this commit before, then wont do it again to avoid loops
		if (!stateKeeper.hasSeenCommit(commit)) {
			inQueue.add(commit);
		} else {
			log.info(String.format("Ignoring incoming commit %s that has been seen by this branch before", commit.getCommitId()));
		}
	}
	
	@Override
	public List<OntIndividual> getIncomingCommitHandlerConfig() {
		Seq list = createOrGetListResource(AES.incomingCommitMerger);
		return fromSeqResourceToContent(list);
	}
	
	@Override
	public void appendIncomingCommitHandler(@NonNull CommitHandler handler) {
		Seq configs = this.createOrGetListResource(AES.incomingCommitMerger);
		int pos = handlers.indexOf(handler);
		if (pos >= 0) {
			handlers.remove(handler);
			configs.remove(pos);
		}
		handlers.add(handler);
		configs.add(handler.getConfigResource());
	}
	
	@Override
	public void removeIncomingCommitHandler(@NonNull CommitHandler handler) {
		Seq configs = this.createOrGetListResource(AES.incomingCommitMerger);
		int pos = handlers.indexOf(handler);
		if (pos >= 0) {
			handlers.remove(handler);
			configs.remove(pos);
		}
	}

	@Getter
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
                	forwardCommit(commit);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	}
	
	private void forwardCommit(Commit commit) {
		if (dataset.isInTransaction())
			dataset.abort();
		dataset.begin(ReadWrite.WRITE);
		handlers.stream().forEach(handler -> handler.handleCommit(commit));
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
	public void appendCommitService(@NonNull BranchInternalCommitHandler service) {
		Seq configs = this.createOrGetListResource(AES.localCommitService);
		int pos = services.indexOf(service);
		if (pos >= 0) {
			services.remove(service);
			configs.remove(pos);
		}
		services.add(service);
		configs.add(service.getConfigResource());
	}
	
	@Override
	public void removeCommitService(@NonNull BranchInternalCommitHandler service) {
		Seq configs = this.createOrGetListResource(AES.localCommitService);
		int pos = services.indexOf(service);
		if (pos >= 0) {
			services.remove(service);
			configs.remove(pos);
		}
	}
	
	@Override
	public List<OntIndividual> getLocalCommitServiceConfig() {
		Seq list = createOrGetListResource(AES.localCommitService);
		return fromSeqResourceToContent(list);
	}
	
	/**
	 * assumes no other tread is making changes to the model while services are processing
	 */
	@Override
	public Commit commitChanges(String commitMsg) throws Exception {
		if (!stmtAggregator.hasAdditions() && !stmtAggregator.hasRemovals()) {
			log.debug("Commit not created as no changes occurred since last commit: "+getLastCommitId());
			return null;
		} else {
			var commit = new StatementCommitImpl( branchResource.getURI() , commitMsg, getLastCommitId(), stmtAggregator.retrieveAddedStatements(), stmtAggregator.retrieveRemovedStatements());
			handleCommitInternally(commit);
			outQueue.add(commit); 
			return commit;
		}
	}
	
	@Override
	public Commit commitMergeOf(Commit mergedCommit) throws Exception {
		//we always create a local commit upon a merge to signal that we received and processed that commit
		var commit = new StatementCommitImpl( branchResource.getURI() , mergedCommit.getCommitId(), mergedCommit.getCommitMessage(), getLastCommitId(), stmtAggregator.retrieveAddedStatements(), stmtAggregator.retrieveRemovedStatements());
		if (commit.isEmpty()) {
			log.info(String.format("MergeCommit %s merged into branch %s has no changes after incoming processing",commit.getCommitId(), this.branchResource.getURI()));
		}
		handleCommitInternally(commit);
		outQueue.add(commit); 
		return commit;
		
	}
	
	private void handleCommitInternally(StatementCommitImpl commit) throws Exception {
		log.debug(String.format("Created commit %s in branch %s", commit.getCommitId(), branchResource.getURI()));
		// clear the changes
		if (services.size() > 0 && !commit.isEmpty()) {
			executeServiceLoop(commit);
		}		 
		// persist augmented commit and  mark preliminary commit as processed
		try {
			stateKeeper.afterServices(commit);
			dataset.commit(); // together with commit persistence
			log.debug(String.format("Branch %s contains now %s statements", branchResource.getLabel(), model.size()));
		} catch (Exception e) {
			log.warn(String.format("Failed to persist post-service commit %s %s with exception %s", commit.getCommitMessage(), commit.getCommitId(), e.getMessage()));
			//SHOULD WE: rethrow e to signal that we cannot continue here as we would loose persisted commit history.
			dataset.abort();
			throw e; // if so, then we need to abort transaction before rethrowing
		} finally {
			dataset.end();
		}
		dataset.begin(); // prepare for next round of transactions
	}
	
	private void executeServiceLoop(StatementCommitImpl commit) {
		try { // first persist initial commit
			stateKeeper.beforeServices(commit);
			dataset.commit(); // together with commit/events persistence, here persists state of model
		} catch(Exception e) {
			log.info(String.format("Failed to persist pre-service commit %s %s with exception %s", commit.getCommitMessage(),commit.getCommitId(), e.getMessage()));
		} finally {
			dataset.end();
		}
		dataset.begin();
		// we now have the local changes persisted and have a restart point established
		// next we iterated through services
		int baseAdds = commit.getAdditionCount();
		int baseRemoves = commit.getRemovalCount();
		int addsCount = baseAdds;
		int removesCount = baseRemoves;
		int newAdds = 0;
		int newRemoves = 0;
		int rounds = 0;
		// store for each service the last seen offset
		Map<BranchInternalCommitHandler, Integer> offsetAdds = initServiceOffsets();
		Map<BranchInternalCommitHandler, Integer> offsetRemoves = initServiceOffsets();
		Integer perIterationAdds = 0;
		Integer perIterationsRemovals = 0;
		do {
			perIterationAdds = 0;
			perIterationsRemovals = 0;
			for (BranchInternalCommitHandler service : services) {
				service.handleCommitFromOffset(commit, offsetAdds.get(service), offsetRemoves.get(service));
				// any changes by a service are now in the statement lists
				
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
				offsetRemoves.put(service,  commit.getRemovalCount());
				
			}
			rounds++;
			//continue while new changes happen and max 10 rounds to avoid infinite loops
		} while ((perIterationAdds > 0 || perIterationsRemovals > 0) && rounds < 10);
		
		if ((perIterationAdds > 0 || perIterationsRemovals > 0) && rounds >= 10) {
			log.warn(String.format("Service loop for commit '%s' reached maximum iteration count of 10 while still new statements available", commit.getCommitMessage()));
		}
		commit.removeEffectlessStatements(baseAdds, baseRemoves);
		if (commit.isEmpty()) {
			log.info(String.format("Commit %s of branch %s has no changes after local service processing", commit.getCommitId(), this.branchResource.getURI()));
		}
	}
	
	private Map<BranchInternalCommitHandler, Integer> initServiceOffsets() {
		Map<BranchInternalCommitHandler, Integer> offsets = new HashMap<>();
		services.stream().forEach(service -> offsets.put(service, 0));
		return offsets;
	}

	@Override
	public void undoNoncommitedChanges() {
		dataset.abort();
		dataset.end();
		
		stmtAggregator.retrieveAddedStatements();
		stmtAggregator.retrieveRemovedStatements();

		dataset.begin();
	}

	@Override
	public void appendOutgoingCrossBranchCommitHandler(@NonNull CommitHandler crossBranchHandler) {
		crossBranchStreamer.addOutgoingCommitHandler(crossBranchHandler);
		Seq configs = this.createOrGetListResource(AES.outgoingCommitDistributer);
		configs.add(crossBranchHandler.getConfigResource());
	}

	@Override
	public void removeOutgoingCrossBranchCommitHandler(@NonNull CommitHandler crossBranchHandler) {		
		crossBranchStreamer.removeOutgoingCommitHandler(crossBranchHandler);
		//TODO: update rdf persisted config in repoDataset/Model
	}

	@Override
	public List<OntIndividual> getOutgoingCommitDistributerConfig() {
		Seq list = createOrGetListResource(AES.outgoingCommitDistributer);
		return fromSeqResourceToContent(list);
	}


}

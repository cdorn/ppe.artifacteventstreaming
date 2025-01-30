package at.jku.isse.artifacteventstreaming.branch.outgoing;

import org.apache.jena.ontapi.model.OntIndividual;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import at.jku.isse.artifacteventstreaming.replay.InMemoryHistoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitToHistoryHandler extends AbstractHandlerBase {

	public static final String SERVICE_TYPE_URI = CommitHandler.serviceTypeBaseURI+CommitToHistoryHandler.class.getSimpleName();
	public static final String CACHE_ENTRY_PREFIX = "LAST_COMMIT_TRANSFORMED_TO_HISTORY";
	
	private final Branch sourceBranch;
	private final InMemoryHistoryRepository historyRepo;
	private final BranchStateCache cache;	
	
	public CommitToHistoryHandler(Branch sourceBranch, InMemoryHistoryRepository historyRepo,
			 BranchStateCache cache) {
		super(CommitToHistoryHandler.class.getSimpleName()+sourceBranch.getBranchName(), sourceBranch.getBranchResource().getModel());
		this.sourceBranch = sourceBranch;
		this.historyRepo = historyRepo;
		this.cache = cache;		
	}
	
	public void init() throws Exception {
		// Unclear yet what need/should be initialized
		 
//		Commit lastCommit = sourceBranch.getLastCommit();
//		if (lastCommit == null) // no commit yet
//			return;
//		
//		String lastForwardedCommit = cache.get(getCacheKey());
//		BranchStateKeeper sourceState = sourceBranch.getStateKeeper();
//		if (lastForwardedCommit == null) {
//			// we never have forwarded anything, hence get the history and forward it
//			sourceState.getHistory().stream().forEach(commit -> handleCommit(commit));
//		} else {
//			if (lastForwardedCommit.equals(lastCommit.getCommitId())) {// all is up to date
//				return; 
//			} else {
//			// get only any commits after the last forwarded one	
//				List<Commit> commitsToForward = sourceState.getCommitsForwardIncludingFrom(lastForwardedCommit);
//				if (commitsToForward.size() > 1) { 					
//					commitsToForward.subList(1, commitsToForward.size()).stream().forEach(commit -> handleCommit(commit));
//				} // else wont be reached as covered above
//			}
//		}
	}
	
	@Override
	public void handleCommit(Commit commit) {
		try {
			log.debug(String.format("About to process history %s from branch %s ", commit.getCommitId(), sourceBranch.getBranchId()));
			historyRepo.appendHistory(commit.getCommitId(),  commit.getOriginatingBranchId(),  commit.getTimeStamp(), commit.getAddedStatementsAsSet(), commit.getRemovedStatementsAsSet());
			cache.put(getCacheKey(), commit.getCommitId());
		} catch (Exception e) {
			log.warn(String.format("Error during state caching commit %s  from branch %s: %s", commit.getCommitId(), sourceBranch.getBranchId(), e.getMessage()));
			//TODO: retry later necessary
		}
	}
	
	@Override
	protected String getServiceTypeURI() {
		return SERVICE_TYPE_URI;
	}
	
	private String getCacheKey() {
		// KEY is unique per combination of source and destination and service
		return CACHE_ENTRY_PREFIX+SERVICE_TYPE_URI+sourceBranch.getBranchId();
	}
	
	public static ServiceFactory getServiceFactory(BranchStateCache cache, InMemoryHistoryRepository historyRepo) {
		if (factory == null) {
			factory = new DefaultServiceFactory(cache, historyRepo);
		}
		return factory;
	}
	
	private static DefaultServiceFactory factory = null;
	
	@RequiredArgsConstructor
	public static class DefaultServiceFactory implements ServiceFactory {

		private final BranchStateCache cache;
		private final InMemoryHistoryRepository historyRepo;
		
		@Override
		public CommitHandler getCommitHandlerInstanceFor(Branch sourceBranch
				, OntIndividual serviceConfigEntryPoint
				) throws Exception {
			// simple, as we dont have any config to do
			 CommitToHistoryHandler streamer = new CommitToHistoryHandler(sourceBranch, historyRepo, cache);
			 streamer.init();
			 return streamer;
		}
		
	}


}

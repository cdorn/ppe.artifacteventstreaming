package at.jku.isse.artifacteventstreaming.branch.outgoing;

import java.net.URI;
import java.util.List;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.BranchStateKeeper;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.BranchRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DefaultDirectBranchCommitStreamer extends AbstractHandlerBase {

	public static final String SERVICE_TYPE_URI = CommitHandler.serviceTypeBaseURI+DefaultDirectBranchCommitStreamer.class.getSimpleName();
	public static final String CACHE_ENTRY_PREFIX = "LAST_FORWARDED_COMMIT";
	
	private final Branch sourceBranch;
	private final Branch destinationBranch;
	private final BranchStateCache cache;	
	
	public DefaultDirectBranchCommitStreamer(Branch sourceBranch,
			Branch destinationBranch, BranchStateCache cache) {
		super(DefaultDirectBranchCommitStreamer.class.getSimpleName()+sourceBranch.getBranchName()+destinationBranch.getBranchName(), sourceBranch.getBranchResource().getModel());
		this.sourceBranch = sourceBranch;
		this.destinationBranch = destinationBranch;
		this.cache = cache;
		config = getConfigResource();
	}
	
	// at this point, both branches have state loaded but not necessarily any serice, but this is enought to enqueue any commits that havent been delivered yet
	public void init() throws Exception {
		Commit lastCommit = sourceBranch.getLastCommit();
		if (lastCommit == null) // no commit yet
			return;
		String lastForwardedCommit = cache.get(getCacheKey());
		BranchStateKeeper sourceState = sourceBranch.getStateKeeper();
		if (lastForwardedCommit == null) {
			// we never have forwarded anything, hence get the history and forward it
			sourceState.getHistory().stream().forEach(this::handleCommit);
		} else {
			if (lastForwardedCommit.equals(lastCommit.getCommitId())) {// all is up to date
				
			} else {
			// get only any commits after the last forwarded one	
				List<Commit> commitsToForward = sourceState.getCommitsForwardIncludingFrom(lastForwardedCommit);
				if (commitsToForward.size() > 1) { 					
					commitsToForward.subList(1, commitsToForward.size()).stream().forEach(this::handleCommit);
				} // else wont be reached as covered above
			}
		}
	}
	
	@Override
	public void handleCommit(Commit commit) {
		try {
			log.debug(String.format("About to enqueue %s from brach %s to branch %s", commit.getCommitId(), sourceBranch.getBranchId(), destinationBranch.getBranchId()));
			destinationBranch.enqueueIncomingCommit(commit);
			cache.put(getCacheKey(), commit.getCommitId());
		} catch (Exception e) {
			log.warn(String.format("Error during enqueuing commit %s  from branch %s to branch %s: %s", commit.getCommitId(), sourceBranch.getBranchId(), destinationBranch.getBranchId(), e.getMessage()));
			//TODO: retry later necessary
		}
	}

	public String getLastForwardedCommitId() {
		try {
			return cache.get(getCacheKey());
		} catch (PersistenceException e) {
			return null;
		}
	}
	
	@Override
	public OntIndividual getConfigResource() {
		if (config == null) {
			config = super.getConfigResource();
			config.addProperty(AES.destinationBranch, repoModel.getResource( destinationBranch.getBranchResource().getURI()));
		}
		return config;
	}
	
	@Override
	protected String getServiceTypeURI() {
		return SERVICE_TYPE_URI;
	}
	
	private String getCacheKey() {
		// KEY is unique per combination of source and destination and service
		return CACHE_ENTRY_PREFIX+SERVICE_TYPE_URI+sourceBranch.getBranchId()+destinationBranch.getBranchId();
	}
	
	public static ServiceFactory getServiceFactory(BranchRepository branchRepo, BranchStateCache cache) {
		if (factory == null) {
			factory = new DefaultServiceFactory(branchRepo, cache);
		}
		return factory;
	}
	
	private static DefaultServiceFactory factory = null;
	
	@RequiredArgsConstructor
	public static class DefaultServiceFactory implements ServiceFactory {
		
		private final BranchRepository branchRepo;
		private final BranchStateCache cache;
		
		@Override
		public CommitHandler getCommitHandlerInstanceFor(Branch sourceBranch, OntIndividual serviceConfigEntryPoint) throws Exception {
			// simple, as we dont have any config to do
			Resource destBranchRes = serviceConfigEntryPoint.getPropertyResourceValue(AES.destinationBranch);
			Branch destBranch = branchRepo.getOrLoadBranch(URI.create(destBranchRes.getURI()));
			 DefaultDirectBranchCommitStreamer streamer = new DefaultDirectBranchCommitStreamer(sourceBranch, destBranch, cache);
			 streamer.init();
			 return streamer;
		}
		
	}


}

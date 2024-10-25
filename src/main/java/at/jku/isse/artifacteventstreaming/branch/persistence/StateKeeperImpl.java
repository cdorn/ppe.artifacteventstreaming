package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;

import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitDeliveryEvent;
import at.jku.isse.artifacteventstreaming.api.PerBranchEventStore;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonDeserializer;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonSerializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StateKeeperImpl implements BranchStateUpdater {

	
	public static final String LAST_PROCESSED_INCOMING_COMMIT = "LAST_PROCESSED_INCOMING_COMMIT";
	public static final String LAST_OPEN_PRELEMINARY_COMMIT_ID = "LAST_OPEN_PRELEMINARY_COMMIT_ID";
	public static final String LAST_OPEN_PRELEMINARY_COMMIT_CONTENT = "LAST_OPEN_PRELEMINARY_COMMIT_CONTENT";
	public static final String LAST_PRODUCED_COMMIT = "LAST_PRODUCED_COMMIT";
	public static final String LAST_FORWARDED_COMMIT = "LAST_FORWARDED_COMMIT";


	private final LinkedHashMap<String, Commit> producedCommits = new LinkedHashMap<>();
	private final Set<String> seenCommitIds = new LinkedHashSet<>();
	private Commit lastCommit = null;
	private final String branchURI;
	private final BranchStateCache cache;
	private final PerBranchEventStore eventDBclient;
	private final JsonMapper jsonMapper = new JsonMapper();

	public StateKeeperImpl(URI branchURI,  BranchStateCache cache, PerBranchEventStore eventDBclient) {
		this.cache = cache;
		this.branchURI = branchURI.toString();
		this.eventDBclient = eventDBclient;
		StatementJsonSerializer.registerSerializationModule(jsonMapper);	
		StatementJsonDeserializer.registerDeserializationModule(jsonMapper);
	}

	@Override
	public Commit loadState() throws Exception {
		loadHistory();
		// make cache entries consistent:
		// if last open 
		String lastProducedCommit = cache.get(LAST_PRODUCED_COMMIT+branchURI); 
		String lastPrelimCommitId = cache.get(LAST_OPEN_PRELEMINARY_COMMIT_ID+branchURI);
		String lastPrelimCommit = cache.get(LAST_OPEN_PRELEMINARY_COMMIT_CONTENT+branchURI);

		if (lastPrelimCommitId != null && lastPrelimCommitId.length() > 0 && lastPrelimCommit != null) {
			// apparently we crashed while processing a local commit
			// lets ensure, we might not have just crashed just between finish processing it and caching the result
			if (lastCommit == null // we have crashed upon first commit, we need to rerun
					|| !hasSeenCommit(lastCommit) // we have not recorded this commit as completed
					) {
				// then we reprocess this commit before doing anything else
				StatementCommitImpl commit = jsonMapper.readValue(lastPrelimCommit, StatementCommitImpl.class);
				return commit; // not the statekeeper's job to trigger replay
			} else { // we have processed this but could not set the cache entries anymore
				// clean the cache, done below
			}
		} 
		// ensure clean/consistent cache
		cache.put(LAST_OPEN_PRELEMINARY_COMMIT_ID+branchURI, "");
		cache.put(LAST_OPEN_PRELEMINARY_COMMIT_CONTENT+branchURI, "");
		return null;
	}

	private void loadHistory() throws Exception {		
		// for now, we do a inefficient sequential load of all commits and keep them in memory
		List<Commit> commits = eventDBclient.loadAllCommits();
		for (Commit commit : commits) {
				producedCommits.put(commit.getCommitId(), commit);
				seenCommitIds.add(commit.getCommitId());
				lastCommit = commit;
		}		
	}
	
	@Override
	public void beforeMerge(Commit commit) throws Exception {
		// add to event db the info that we received this commit
		CommitDeliveryEvent event = new CommitDeliveryEvent(commit.getCommitId(), commit, commit.getOriginatingBranchId(), this.branchURI);
		eventDBclient.appendCommitDelivery(event);
	}
	
	@Override
	public List<Commit> getNonMergedCommits() throws Exception {
		String lastMergedCommitId = cache.get(LAST_PROCESSED_INCOMING_COMMIT+branchURI);
		return eventDBclient.loadAllIncomingCommitsForBranchFromCommitIdOnward(lastMergedCommitId);
	}

	@Override
	public void finishedMerge(Commit commit) throws Exception {
		cache.put(LAST_PROCESSED_INCOMING_COMMIT+branchURI, commit.getCommitId());
		log.debug("Finished merge of" +commit.getCommitId());
	}

	@Override
	public void beforeServices(Commit commit) throws Exception {

		try {
			cache.put(LAST_OPEN_PRELEMINARY_COMMIT_ID+branchURI, commit.getCommitId());
			cache.put(LAST_OPEN_PRELEMINARY_COMMIT_CONTENT+branchURI, jsonMapper.writeValueAsString(commit));
		} catch (Exception e) {
			log.warn(String.format("Error writing preliminary commit %s of branch %s to cache with error %s", commit.getCommitId(), branchURI, e.getMessage()));
			throw e;
		}

		seenCommitIds.add(commit.getCommitId());
		log.debug("Pre Services: "+commit.getCommitId());
	}

	@Override
	public void afterServices(Commit commit) throws Exception {
		// first store the commit
		try {
			eventDBclient.appendCommit(commit);
		} catch (JsonProcessingException | InterruptedException | ExecutionException e) {
			log.warn(String.format("Error writing commit %s event to branch %s with error %s", commit.getCommitId(), branchURI, e.getMessage()));
			throw e;
		}
		// then store the cache entry
		cache.put(LAST_PRODUCED_COMMIT+branchURI, commit.getCommitId()); // first store what we have processed
		cache.put(LAST_OPEN_PRELEMINARY_COMMIT_ID+branchURI, "");
		cache.put(LAST_OPEN_PRELEMINARY_COMMIT_CONTENT+branchURI, "");

		producedCommits.put(commit.getCommitId(), commit);
		seenCommitIds.add(commit.getCommitId());
		lastCommit = commit;
		log.debug("Post Services: "+commit.getCommitId());
	}

	@Override
	public boolean hasSeenCommit(Commit commit) {
		return hasSeenCommit(commit.getCommitId());
	}

	private boolean hasSeenCommit(String commitId) {
		return seenCommitIds.contains(commitId);
	}

	@Override
	public List<Commit> getHistory() {
		return producedCommits.values().stream().collect(Collectors.toList());
	}

	@Override
	public Optional<Commit> getLastCommit() {
		return Optional.ofNullable(lastCommit);
	}

	@Override
	public void afterForwarded(Commit commit) throws Exception {
		cache.put(LAST_FORWARDED_COMMIT+branchURI, commit.getCommitId()); 
	}

	@Override
	public Optional<String> getLastMergedCommitId() {
		try {
			return Optional.ofNullable(cache.get(LAST_PROCESSED_INCOMING_COMMIT+branchURI));
		} catch (Exception e) {
			log.warn("Error reading from cache "+e.getMessage());
			return Optional.empty();
		}
	}

	@Override
	public List<Commit> getNonForwardedCommits() {
		Optional<Commit> lastCommit = getLastCommit();
		Optional<String> lastForwardedCommitId = getLastForwardedCommitId();
		if (lastCommit.isPresent()) {
			List<Commit> commitsToRequeue;
			if (lastForwardedCommitId.isPresent()) {
				if (lastForwardedCommitId.get().equals(lastCommit.get().getCommitId())) {
					// all up to date, hence noop
					return Collections.emptyList();
				} else {
					commitsToRequeue = getCommitsForwardIncludingFrom(lastForwardedCommitId.get());
					if (commitsToRequeue.size() > 0) {
						// remove the one already forwarded
						commitsToRequeue.remove(0);
					}
				}
			} else {
				// we apparently never forwarded any commit, hence add complete history
				commitsToRequeue = getHistory();
			}
			return commitsToRequeue;
		} // else no commit available to requeue/forward
		return Collections.emptyList();
	}
	
	@Override
	public Optional<String> getLastForwardedCommitId() {
		try {
			return Optional.ofNullable(cache.get(LAST_FORWARDED_COMMIT+branchURI));
		} catch (Exception e) {
			log.warn("Error reading from cache "+e.getMessage());
			return Optional.empty();
		}
	}

	@Override
	public List<Commit> getCommitsForwardIncludingFrom(String commitId) {
		Commit commit = producedCommits.get(commitId);
		if (commit == null) {
			log.warn("Asked to find a commit that we haven't seen by lookup id: "+commitId);
			return Collections.emptyList();
		} else {
			if (commit.getCommitId().equals(lastCommit.getCommitId())) {
				return List.of(commit);
			} else {
				List<String> ids = new ArrayList<>(seenCommitIds);
				int pos = ids.indexOf(commitId);
				if (pos < 0) {
					log.error("Inconsistency between commit map and commit id index when trying to lookup id: "+commitId);
					return Collections.emptyList();
				} else {
					return ids.subList(pos, ids.size()).stream()
							.map(id -> producedCommits.get(id))
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
				}
			}
		}
	}
	
	

}

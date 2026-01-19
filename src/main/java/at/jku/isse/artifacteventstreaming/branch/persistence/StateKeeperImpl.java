package at.jku.isse.artifacteventstreaming.branch.persistence;

import at.jku.isse.artifacteventstreaming.api.*;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonDeserializer;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.util.*;

@Slf4j
public class StateKeeperImpl implements BranchStateUpdater {

	
	public static final String LAST_PROCESSED_INCOMING_COMMIT = "LAST_PROCESSED_INCOMING_COMMIT";
	public static final String LAST_PRODUCED_COMMIT = "LAST_PRODUCED_COMMIT";
	public static final String LAST_FORWARDED_COMMIT = "LAST_FORWARDED_COMMIT";


    //TODO this cache must be size restricted or persisted differently if branches have very long histories
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
	public void loadState() throws PersistenceException {
		loadHistory();
	}

	private void loadHistory() throws PersistenceException {		
		// for now, we do a inefficient sequential load of all commits and keep them in memory
		List<Commit> commits = eventDBclient.loadAllCommits();
		for (Commit commit : commits) {
				producedCommits.put(commit.getCommitId(), commit);
				seenCommitIds.add(commit.getCommitId());
				lastCommit = commit;
		}		
	}
	
	@Override
	public void beforeMerge(Commit commit) throws PersistenceException {
		// add to event db the info that we received this commit
		CommitDeliveryEvent event = new CommitDeliveryEvent(commit.getCommitId(), commit, commit.getOriginatingBranchId(), this.branchURI);
		eventDBclient.appendCommitDelivery(event);
	}
	
	@Override
	public List<Commit> getNonMergedCommits() throws PersistenceException {
		String lastMergedCommitId = cache.get(LAST_PROCESSED_INCOMING_COMMIT+branchURI);
		return eventDBclient.loadAllIncomingCommitsForBranchFromCommitIdOnward(lastMergedCommitId);
	}

	@Override
	public void finishedMerge(Commit commit) throws PersistenceException {
		cache.put(LAST_PROCESSED_INCOMING_COMMIT+branchURI, commit.getCommitId());
		log.debug("Finished merge of" +commit.getCommitId());
	}

	@Override
	public void afterServices(Commit commit) throws PersistenceException {
		// first store the commit
		eventDBclient.appendCommit(commit);
	
		// then store the cache entry
		cache.put(LAST_PRODUCED_COMMIT+branchURI, commit.getCommitId()); // first store what we have processed

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
		return producedCommits.values().stream().toList();
	}

	@Override
	public Optional<Commit> getLastCommit() {
		return Optional.ofNullable(lastCommit);
	}

	@Override
	public void afterForwarded(@NonNull Commit commit) throws PersistenceException {
		cache.put(LAST_FORWARDED_COMMIT+branchURI, commit.getCommitId()); 
	}

	@Override
	public List<Commit> getNonForwardedCommits() throws PersistenceException {
		Optional<Commit> lastCommitOpt = getLastCommit();
		Optional<String> lastForwardedCommitId = getLastForwardedCommitId();
		if (lastCommitOpt.isPresent()) {
			List<Commit> commitsToRequeue;
			if (lastForwardedCommitId.isPresent()) {
				if (lastForwardedCommitId.get().equals(lastCommitOpt.get().getCommitId())) {
					// all up to date, hence noop
					return Collections.emptyList();
				} else {
					commitsToRequeue = getCommitsForwardIncludingFrom(lastForwardedCommitId.get());
					if (!commitsToRequeue.isEmpty()) {
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
	public Optional<String> getLastForwardedCommitId() throws PersistenceException {
		try {
			return Optional.ofNullable(cache.get(LAST_FORWARDED_COMMIT+branchURI));
		} catch (Exception e) {
			String msg = String.format("Cannot access cache for last forwarded commit id for branch %s with error %s", branchURI, e.getMessage()); 
			log.warn(msg);
			throw new PersistenceException(msg);
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
							.map(producedCommits::get)
							.filter(Objects::nonNull)
							.toList();
				}
			}
		}
	}
	
	

}

package at.jku.isse.artifacteventstreaming.rdf;

import java.net.URI;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.ehcache.Cache;

import com.eventstore.dbclient.EventStoreDBClient;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.StateKeeper;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DBBasedStateKeeper implements StateKeeper {
	
	public static final String LAST_PROCESSED_INCOMING_COMMIT = "LAST_PROCESSED_INCOMING_COMMIT";
	public static final String LAST_OPEN_PRELEMINARY_COMMIT = "LAST_OPEN_PRELEMINARY_COMMIT";
	public static final String LAST_PRODUCED_COMMIT = "LAST_PRODUCED_COMMIT";
	
	private final Set<Commit> producedCommits = new LinkedHashSet<>();
	private final Set<String> seenCommitIds = new HashSet<>();
	private Commit lastCommit = null;
	private final String branchURI;
	private final Cache<String, String> cache;
	private final EventStoreDBClient eventDBclient;
	
	public DBBasedStateKeeper(URI branchURI, Cache<String, String> cache, EventStoreDBClient eventDBclient) {
		this.cache = cache;
		this.branchURI = branchURI.toString();
		this.eventDBclient = eventDBclient;
	}
	
	
	@Override
	public void finishedMerge(Commit commit) {
		cache.put(LAST_PROCESSED_INCOMING_COMMIT+branchURI, commit.getCommitId());
		log.debug("Finished merge of" +commit.getCommitId());
	}

	@Override
	public void beforeServices(Commit commit) {
		// TODO store commit
		cache.put(LAST_OPEN_PRELEMINARY_COMMIT+branchURI, commit.getCommitId());
		
		//TODO: store via event db
		seenCommitIds.add(commit.getCommitId());
		log.debug("Pre Services: "+commit.getCommitId());
	}

	@Override
	public void afterServices(Commit commit) {
		cache.put(LAST_OPEN_PRELEMINARY_COMMIT+branchURI, "");
		cache.put(LAST_PRODUCED_COMMIT+branchURI, commit.getCommitId());
		
		//TODO: store via event db
		producedCommits.add(commit);
		seenCommitIds.add(commit.getCommitId());
		lastCommit = commit;
		log.debug("Post Services: "+commit.getCommitId());
	}

	// below need to be done via EventDB
	
	@Override
	public boolean hasSeenCommit(Commit commit) {
		return seenCommitIds.contains(commit.getCommitId());
	}

	@Override
	public List<Commit> getHistory() {
		return new LinkedList<>(producedCommits);
	}

	@Override
	public Commit getLastCommit() {
		return lastCommit;
	}

}

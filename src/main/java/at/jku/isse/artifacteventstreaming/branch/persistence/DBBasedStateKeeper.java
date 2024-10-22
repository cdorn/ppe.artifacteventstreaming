package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntModel;

import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.ExpectedRevision;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.json.JsonMapper;

import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.StateKeeper;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.events.StatementJsonDeserializer;
import at.jku.isse.artifacteventstreaming.branch.events.StatementJsonSerializer;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory.EventMetaData;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DBBasedStateKeeper implements StateKeeper {
	
	public static final String LAST_PROCESSED_INCOMING_COMMIT = "LAST_PROCESSED_INCOMING_COMMIT";
	public static final String LAST_OPEN_PRELEMINARY_COMMIT_ID = "LAST_OPEN_PRELEMINARY_COMMIT_ID";
	public static final String LAST_OPEN_PRELEMINARY_COMMIT_CONTENT = "LAST_OPEN_PRELEMINARY_COMMIT_CONTENT";
	public static final String LAST_PRODUCED_COMMIT = "LAST_PRODUCED_COMMIT";
	
	private final Set<Commit> producedCommits = new LinkedHashSet<>();
	private final Set<String> seenCommitIds = new HashSet<>();
	private Commit lastCommit = null;
	private final String branchURI;
	private final BranchStateCache cache;
	private final EventStoreDBClient eventDBclient;
	private final JsonMapper jsonMapper = new JsonMapper();
	
	public DBBasedStateKeeper(URI branchURI,  BranchStateCache cache, EventStoreDBClient eventDBclient) {
		this.cache = cache;
		this.branchURI = branchURI.toString();
		this.eventDBclient = eventDBclient;
		StatementJsonSerializer.registerSerializationModule(jsonMapper);		
	}
	
	@Override
	public void loadState(OntModel model) throws StreamReadException, DatabindException, IOException {
		StatementJsonDeserializer.registerDeserializationModule(jsonMapper, model);
		// for now, we do a inefficient sequential load of all commits and keep them in memory
		ReadStreamOptions options = ReadStreamOptions.get()
		        .forwards()
		        .fromStart();
		ReadResult result = null;
		try {
		    result = eventDBclient.readStream(branchURI, options)
		            .get();
		} catch (ExecutionException | InterruptedException e) {
		    Throwable innerException = e.getCause();
		    if (innerException instanceof StreamNotFoundException) {
		        return; //done
		    }
		}
		for (ResolvedEvent resolvedEvent : result.getEvents()) {
		    RecordedEvent recordedEvent = resolvedEvent.getOriginalEvent();
		    StatementCommitImpl commit = jsonMapper.readValue(recordedEvent.getEventData(), StatementCommitImpl.class);
		    if (commit != null) {
		    	producedCommits.add(commit);
		    	seenCommitIds.add(commit.getCommitId());
		    	lastCommit = commit;
		    }
		}		
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
			cache.put(LAST_OPEN_PRELEMINARY_COMMIT_CONTENT, jsonMapper.writeValueAsString(commit));
		} catch (Exception e) {
			log.warn(String.format("Error writing preliminary commit %s of branch %s to cache with error %s", commit.getCommitId(), branchURI, e.getMessage()));
			throw e;
		}
				
		seenCommitIds.add(commit.getCommitId());
		log.debug("Pre Services: "+commit.getCommitId());
	}

	@Override
	public void afterServices(Commit commit) throws Exception {
		cache.put(LAST_OPEN_PRELEMINARY_COMMIT_ID+branchURI, "");
		cache.put(LAST_OPEN_PRELEMINARY_COMMIT_CONTENT+branchURI, "");
		cache.put(LAST_PRODUCED_COMMIT+branchURI, commit.getCommitId());
		
		try {
			EventMetaData metadata = new EventMetaData(commit.getCommitId());
			byte[] metaByte = jsonMapper.writeValueAsBytes(metadata);
			
			//TODO: check for commit serialization size to avoid write failures if there are too many statements in a commit!
			//store via event db
			EventData eventData = EventData
			        .builderAsBinary("CommitEventType", jsonMapper.writeValueAsBytes(commit)) // we cannot use commit UUID as its reused for each branch		
			        .metadataAsBytes(metaByte)
			        .build();
			
			AppendToStreamOptions options = AppendToStreamOptions.get()
			        .expectedRevision(ExpectedRevision.any());
			
			eventDBclient.appendToStream(branchURI, options, eventData) 
			.get();
		} catch (JsonProcessingException | InterruptedException | ExecutionException e) {
			log.warn(String.format("Error writing commit %s event to branch %s with error %s", commit.getCommitId(), branchURI, e.getMessage()));
			throw e;
		}
		
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
		return producedCommits.stream().collect(Collectors.toList());
	}

	@Override
	public Commit getLastCommit() {
		return lastCommit;
	}

}

package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBClientSettings;
import com.eventstore.dbclient.ExpectedRevision;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitDeliveryEvent;
import at.jku.isse.artifacteventstreaming.api.PerBranchEventStore;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonDeserializer;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonSerializer;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventStoreFactory {

	 private final EventStoreDBClient client;
	 private final JsonMapper jsonMapper = new JsonMapper();
	 private final static SimpleModule commitModule = new SimpleModule().addAbstractTypeMapping(Commit.class, StatementCommitImpl.class);
	 
	 public EventStoreFactory() {
		 EventStoreDBClientSettings settings = EventStoreDBClientSettings.builder()
				  .addHost("localhost", 2113)
				  .tls(false)
				  .defaultCredentials("admin", "changeit")
				  .maxDiscoverAttempts(1)
				  .buildConnectionSettings();
		client = EventStoreDBClient.create(settings);		
		StatementJsonSerializer.registerSerializationModule(jsonMapper);	
		StatementJsonDeserializer.registerDeserializationModule(jsonMapper);		
		jsonMapper.registerModule(commitModule);
	 }
	 
	 public EventStoreDBClient getClient() {
		 return client;
	 }
	
	 public PerBranchEventStore getEventStore(String branchURI) {
		 return new EventStoreImpl(branchURI, client, jsonMapper);
	 }
	 
	 @Data
	 public static class EventMetaData {
		 final String commitUUID;		 
	 }
	 
	 @RequiredArgsConstructor
	 public static class EventStoreImpl implements PerBranchEventStore {
		 public static final String INCOMING_COMMITS_STREAM_POSTFIX = "IncomingCommits";
		 
		 final String branchURI;
		 final EventStoreDBClient eventDBclient;
		 final JsonMapper jsonMapper;

		@Override
		public List<Commit> loadAllCommits() throws PersistenceException{
			// for now, we do a inefficient sequential load of all commits and keep them in memory
			List<Commit> commits = new LinkedList<>();
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
					return Collections.emptyList(); //done
				}
			}
			
			try {
				for (ResolvedEvent resolvedEvent : result.getEvents()) {
					RecordedEvent recordedEvent = resolvedEvent.getOriginalEvent();
					StatementCommitImpl commit = jsonMapper.readValue(recordedEvent.getEventData(), StatementCommitImpl.class);
					if (commit != null) {
						commits.add(commit);
					}
				}	
			} catch (IOException e) {
				String msg = String.format("Error loading commits for branch %s with error %s", branchURI, e.getMessage());
				log.warn(msg);
				throw new PersistenceException(msg);
			}  catch (NullPointerException e) {
				String msg = String.format("Error accessing event stream for branch %s with error %s", branchURI, e.getMessage());
				log.warn(msg);
				throw new PersistenceException(msg);
			}
			return commits;
		}

		@Override
		public List<Commit> loadAllIncomingCommitsForBranchFromCommitIdOnward(String fromCommitIdOnwards)  throws PersistenceException{
			ReadStreamOptions options;
			Boolean isReverse = false;
			if (fromCommitIdOnwards == null) { // we read forward as we need to append all commits
				options = ReadStreamOptions.get()
						.forwards()
						.fromStart();
			} else { // we read backwards until we hit last merged commit or the beginning (should not happen)
				options = ReadStreamOptions.get()
						.backwards()
						.fromEnd();
				isReverse = true;
			}
			ReadResult result = null;
			try {
				result = eventDBclient.readStream(branchURI+INCOMING_COMMITS_STREAM_POSTFIX, options)
						.get();
			} catch (ExecutionException | InterruptedException e) {
				Throwable innerException = e.getCause();
				if (innerException instanceof StreamNotFoundException) {
					return Collections.emptyList();
				}
			}
			List<Commit> commits = new LinkedList<>();
			
			for (ResolvedEvent resolvedEvent : result.getEvents()) {
				RecordedEvent recordedEvent = resolvedEvent.getOriginalEvent();
				try {
					CommitDeliveryEvent event = jsonMapper.readValue(recordedEvent.getEventData(), CommitDeliveryEvent.class);
					if (event != null) {
						if (event.getCommitId().equals(fromCommitIdOnwards)) {
							break;
						} else {
							commits.add(event.getCommit());
						}
					}
				} catch (IOException e) {
					String msg = String.format("Error loading CommitDeliveryEvents for branch %s with error %s", branchURI, e.getMessage());
					log.warn(msg);
					throw new PersistenceException(msg);
				}
			}
			if (isReverse) {
				Collections.reverse(commits); // to the earliest commits are at the beginning
			}
			return commits;
		}

		@Override
		public void appendCommit(Commit commit) throws PersistenceException {
			EventMetaData metadata = new EventMetaData(commit.getCommitId());					
			try {
				byte[] metaByte = jsonMapper.writeValueAsBytes(metadata);
				byte[] eventByte = jsonMapper.writeValueAsBytes(commit);
				long size = metaByte.length + eventByte.length;
				
				//TODO: check for commit serialization size to avoid write failures if there are too many statements in a commit!
				//store via event db
				EventData eventData = EventData
						.builderAsBinary("CommitEventType", eventByte) // we cannot use commit UUID as its reused for each branch		
						.metadataAsBytes(metaByte)
						.build();

				AppendToStreamOptions options = AppendToStreamOptions.get()
						.expectedRevision(ExpectedRevision.any());

				var result = eventDBclient.appendToStream(branchURI, options, eventData) 
				.get();			
				log.trace("Obtained log position upon write: "+result.getLogPosition().toString());
			} catch (JsonProcessingException e) {
				String msg = String.format("Error serializing commit %s event to branch %s with error %s", commit.getCommitId(), branchURI, e.getMessage()) ;
				log.warn(msg);
				throw new PersistenceException(msg);
			} catch (InterruptedException | ExecutionException e) {
				String msg = String.format("Error storing commit %s event to branch %s with error %s", commit.getCommitId(), branchURI, e.getMessage()) ;
				log.warn(msg);
				throw new PersistenceException(msg);
			} 
		}

		@Override
		public void appendCommitDelivery(@NonNull CommitDeliveryEvent event) throws PersistenceException {
			try {
			EventData eventData = EventData
					.builderAsBinary("CommitDeliveryType", jsonMapper.writeValueAsBytes(event)) 	
					.build();

			AppendToStreamOptions options = AppendToStreamOptions.get()
					.expectedRevision(ExpectedRevision.any());

			eventDBclient.appendToStream(branchURI+INCOMING_COMMITS_STREAM_POSTFIX, options, eventData) 
			.get();
			} catch (JsonProcessingException e) {
				String msg = String.format("Error serializing commitdeliveryevent %s event to branch %s with error %s", event.getCommitId(), branchURI, e.getMessage()) ;
				log.warn(msg);
				throw new PersistenceException(msg);
			} catch (InterruptedException | ExecutionException e) {
				String msg = String.format("Error storing commitdeliveryevent %s event to branch %s with error %s", event.getCommitId(), branchURI, e.getMessage()) ;
				log.warn(msg);
				throw new PersistenceException(msg);
			} 
		}
		 
	 }
	 
}

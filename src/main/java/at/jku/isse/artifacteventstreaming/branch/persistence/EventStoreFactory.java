package at.jku.isse.artifacteventstreaming.branch.persistence;

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
import com.fasterxml.jackson.databind.json.JsonMapper;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitDeliveryEvent;
import at.jku.isse.artifacteventstreaming.api.PerBranchEventStore;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonDeserializer;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonSerializer;
import lombok.Data;
import lombok.RequiredArgsConstructor;

public class EventStoreFactory {

	 private final EventStoreDBClient client;
	 private final JsonMapper jsonMapper = new JsonMapper();
	 
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
		public List<Commit> loadAllCommits() throws Exception{
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
			for (ResolvedEvent resolvedEvent : result.getEvents()) {
				RecordedEvent recordedEvent = resolvedEvent.getOriginalEvent();
				StatementCommitImpl commit = jsonMapper.readValue(recordedEvent.getEventData(), StatementCommitImpl.class);
				if (commit != null) {
					commits.add(commit);
				}
			}		
			return commits;
		}

		@Override
		public List<Commit> loadAllIncomingCommitsForBranchFromCommitIdOnward(String fromCommitIdOnwards)  throws Exception{
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
				CommitDeliveryEvent event = jsonMapper.readValue(recordedEvent.getEventData(), CommitDeliveryEvent.class);
				if (event != null) {
					if (event.getCommitId().equals(fromCommitIdOnwards)) {
						break;
					} else {
						commits.add(event.getCommit());
					}
				}
			}
			if (isReverse) {
				Collections.reverse(commits); // to the earliest commits are at the beginning
			}
			return commits;
		}

		@Override
		public void appendCommit(Commit commit) throws Exception {
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
		}

		@Override
		public void appendCommitDelivery(CommitDeliveryEvent event) throws Exception {
			EventData eventData = EventData
					.builderAsBinary("CommitDeliveryType", jsonMapper.writeValueAsBytes(event)) 	
					.build();

			AppendToStreamOptions options = AppendToStreamOptions.get()
					.expectedRevision(ExpectedRevision.any());

			eventDBclient.appendToStream(branchURI+INCOMING_COMMITS_STREAM_POSTFIX, options, eventData) 
			.get();
		}
		 
	 }
	 
}

package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventDataBuilder;
import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBClientSettings;
import com.eventstore.dbclient.EventStoreDBProjectionManagementClient;
import com.eventstore.dbclient.ExpectedRevision;
import com.eventstore.dbclient.ReadResult;
import com.eventstore.dbclient.ReadStreamOptions;
import com.eventstore.dbclient.RecordedEvent;
import com.eventstore.dbclient.ResolvedEvent;
import com.eventstore.dbclient.StreamNotFoundException;
import com.fasterxml.jackson.annotation.JsonProperty;
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
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventStoreFactory {

	 private final EventStoreDBClient client;
	 private final EventStoreDBProjectionManagementClient projectionClient;
	 @Getter
	 private final JsonMapper jsonMapper = new JsonMapper();
	 private static final SimpleModule commitModule = new SimpleModule().addAbstractTypeMapping(Commit.class, StatementCommitImpl.class);
	 
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
		
		projectionClient = EventStoreDBProjectionManagementClient.create(settings);
	 }
	 
	 public EventStoreDBClient getClient() {
		 return client;
	 }
	 
	 public EventStoreDBProjectionManagementClient getProjectionClient() {
		 return projectionClient;
	 }
	
	 public PerBranchEventStore getEventStore(String branchURI) {
		 return new EventStoreImpl(branchURI, client, jsonMapper);
	 }
	 
	 @Data
	 public static class EventMetaData {
		 
		 final String commitUUID;		 
		 final int batchId;
		 final int totalBatches; 
		 
		 public EventMetaData( @JsonProperty("commitUUID") String commitUUID 
				 ,@JsonProperty("batchId") int batchId
				, @JsonProperty("totalBatches") int totalBatches) {			
			this.commitUUID = commitUUID;
			this.batchId = batchId;
			this.totalBatches = totalBatches;
		}
		
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
					.forwards() // if this is ever changed, make sure to update batch joining below!!!!!
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
				String msg = String.format("Error loading commits for branch %s with error %s", branchURI, e.getMessage());
				log.warn(msg);
				throw new PersistenceException(msg);
			}
			
			try {
				var joiner = new CommitJoiner();
				for (ResolvedEvent resolvedEvent : result.getEvents()) {
					RecordedEvent recordedEvent = resolvedEvent.getOriginalEvent();					
					EventMetaData metadata = jsonMapper.readValue(recordedEvent.getUserMetadata(), EventMetaData.class);
					StatementCommitImpl commit = jsonMapper.readValue(recordedEvent.getEventData(), StatementCommitImpl.class);
					if (metadata.getTotalBatches() <= 1) {						
						if (commit != null) {						
							commits.add(commit);
						}
					} else { // multipart
						joiner.addCommit(commit);
						if (metadata.getBatchId() == metadata.getTotalBatches()) {// last batch
							commits.add(joiner.join());
							joiner = new CommitJoiner(); //reset for next batch
						}
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
			if (Boolean.TRUE.equals(isReverse)) {
				Collections.reverse(commits); // to the earliest commits are at the beginning
			}
			return commits;
		}

		@Override
		public void appendCommit(Commit commit) throws PersistenceException {
								
			try {
				var splitter = new CommitSplitter(jsonMapper);
				List<byte[]> payloads = splitter.split(commit).toList();
				if (payloads.isEmpty()) { 
					log.warn("Empty commit "+commit.getCommitId()+", not persisting any events");
					return;				
				} 
				if (payloads.size() > 1) {
					log.info(String.format("Splitting commit %s into %s events, having to replit events %s times", commit.getCommitId(), payloads.size(), splitter.getResplitCounter()));
				}								
				for (int i = 0; i < payloads.size(); i++) {
					EventMetaData metadata = new EventMetaData(commit.getCommitId(), i+1, payloads.size());
					byte[] metaByte = jsonMapper.writeValueAsBytes(metadata);
					EventData eventData = EventDataBuilder
							.json(null, "CommitEventType", payloads.get(i)) // we cannot use commit UUID as its reused for each branch		
							.metadataAsBytes(metaByte)
							.build();
					AppendToStreamOptions options = AppendToStreamOptions.get()
							.expectedRevision(ExpectedRevision.any());

					var result = eventDBclient.appendToStream(branchURI, options, eventData) //multiple events cannot be stored here at once, as we run into maxsize !!
							.get();			
					log.trace("Obtained log position upon write: "+result.getLogPosition().toString());
				}
				
			} catch (JsonProcessingException e) {
				String msg = String.format("Error serializing commit %s event to branch %s with error %s", commit.getCommitId(), branchURI, e.getMessage()) ;
				log.warn(msg);
				throw new PersistenceException(msg);
			} catch (Exception e) {
				String msg = String.format("Error storing commit %s event to branch %s with error %s", commit.getCommitId(), branchURI, e.getMessage()) ;
				log.warn(msg);
				throw new PersistenceException(msg);
			} 
		}

		@Override
		public void appendCommitDelivery(@NonNull CommitDeliveryEvent event) throws PersistenceException {
			try {
			EventData eventData = EventDataBuilder
					.json(null, "CommitDeliveryType", jsonMapper.writeValueAsBytes(event)) 	
					.build();
			//TODO: as we receive large commits within the framework, we need to also do splitting here!!!
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

package at.jku.isse.artifacteventstreaming.branch.persistence;

import com.eventstore.dbclient.EventStoreDBClient;
import com.eventstore.dbclient.EventStoreDBClientSettings;

import lombok.Data;

public class EventStoreFactory {

	 private final EventStoreDBClient client;
	 
	 public EventStoreFactory() {
		 EventStoreDBClientSettings settings = EventStoreDBClientSettings.builder()
				  .addHost("localhost", 2113)
				  .tls(false)
				  .defaultCredentials("admin", "changeit")
				  .maxDiscoverAttempts(1)
				  .buildConnectionSettings();
		client = EventStoreDBClient.create(settings);		
	 }
	
	 public EventStoreDBClient getClient() {
		 return client;
	 }
	 
	 @Data
	 public static class EventMetaData {
		 final String commitUUID;		 
	 }
}

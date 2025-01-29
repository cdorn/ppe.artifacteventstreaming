package at.jku.isse.artifacteventstreaming.api.manualtests;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventDataBuilder;
import com.eventstore.dbclient.ExpectedRevision;

import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;

class CreateTestStreams {

	
	private static EventStoreFactory factory = new EventStoreFactory();
	
	String testProjectionName = "testProjection";
	
	
	
	@Test @Disabled
	void testCreateProjection() throws Exception {
		var client = factory.getClient();
		EventData eventData = EventDataBuilder
				.json(null, "TestType", "{\"Test\": 1}".getBytes()) // we cannot use commit UUID as its reused for each branch		
				.build();
		AppendToStreamOptions options = AppendToStreamOptions.get()
				.expectedRevision(ExpectedRevision.any());

		var result = client.appendToStream("projectionResults", options, eventData) //multiple events cannot be stored here at once, as we run into maxsize !!
				.get();	
	}

}

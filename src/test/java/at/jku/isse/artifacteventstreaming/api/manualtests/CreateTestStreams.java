package at.jku.isse.artifacteventstreaming.api.manualtests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.eventstore.dbclient.AppendToStreamOptions;
import com.eventstore.dbclient.CreateProjectionOptions;
import com.eventstore.dbclient.DeleteStreamOptions;
import com.eventstore.dbclient.EventData;
import com.eventstore.dbclient.EventDataBuilder;
import com.eventstore.dbclient.EventStoreDBProjectionManagementClient;
import com.eventstore.dbclient.ExpectedRevision;
import com.fasterxml.jackson.annotation.JsonProperty;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import lombok.Data;

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

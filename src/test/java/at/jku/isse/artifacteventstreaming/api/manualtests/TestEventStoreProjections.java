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
import org.junit.jupiter.api.Test;

import com.eventstore.dbclient.CreateProjectionOptions;
import com.eventstore.dbclient.DeleteStreamOptions;
import com.eventstore.dbclient.EventStoreDBProjectionManagementClient;
import com.fasterxml.jackson.annotation.JsonProperty;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import lombok.Data;

class TestEventStoreProjections {

	public static URI NS = URI.create("eventstoreprojection");
	
	private static EventStoreFactory factory = new EventStoreFactory();
	
	String testProjectionName = "testProjection";
	
	EventStoreDBProjectionManagementClient projectionClient;
	
	@BeforeEach
	void removeStream() {
		var client = factory.getClient();
		try {
			client.getStreamMetadata(NS.toString()); //throws exception if doesn't exist, then we wont need to delete
			client.deleteStream(NS.toString(), DeleteStreamOptions.get()).get();
		}catch (Exception e) {
			// ignore
		}	
		projectionClient = factory.getProjectionClient();
		try {
		    projectionClient.disable(testProjectionName).get();
		    projectionClient.delete(testProjectionName).get();
		} catch (Exception ex) {
		    if (ex.getMessage().contains("NotFound")) {
		        // all good
		    } else {
		    	ex.printStackTrace();
		    }
		}
	}
	
	private void createTestStreamContent() throws Exception{
		OntModel m = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF);
		var listener = new StatementAggregator();
		listener.registerWithModel(m);
		
		var schemaUtils = new PropertyCardinalityTypes(m); 
		var props = new LinkedList<OntRelationalProperty>();
		var types = new LinkedList<OntClass>();
		
		for (int i = 0; i < 5; i++) {
			var type = m.createOntClass(NS+"type"+i);
			types.add(type);
			for (int j = 0; j < 1; j++) {
				props.add(schemaUtils.getListType().addObjectListProperty(type, NS+"prop"+i+"-"+j, type));				
			}			
		}
		var commit = new StatementCommitImpl(NS.toString(), "TestCommit", null, 0, listener.retrieveAddedStatements(), listener.retrieveRemovedStatements());
		EventStoreFactory factory = new EventStoreFactory();
		var store = factory.getEventStore(NS.toString());
		store.appendCommit(commit);
		
		List<Commit> allCommits = store.loadAllCommits();
		assertEquals(1, allCommits.size());
	}
	
	String projectionTemplate = "fromStream('"+NS.toString()+"')\r\n"
			+ ".when({\r\n"
			+ "        $init: function() {\r\n"
			+ "            return { \r\n"
			+ "                count: 0,\r\n"
			+ "                commitIds: [],\r\n"
			+ "            };\r\n"
			+ "        },\r\n"
			+ "        $any: function(s, e) {\r\n"
			+ "            s.count += 1;\r\n"
			+ "            var id = e.body.commitId;\r\n"
			+ "            if (!s.commitIds.includes(id)) {\r\n"
			+ "                s.commitIds.push(id);\r\n"
			+ "            }\r\n"
			+ "        }\r\n"
			+ "    })\r\n"
			+ ".outputState();";
	
	@Test
	void testCreateProjection() throws Exception {
		createTestStreamContent();
		Thread.sleep(500);
		//String projectionContent = String.format(projectionTemplate, NS.toString());
		projectionClient.create(testProjectionName, projectionTemplate).get();
		Thread.sleep(500);
		CommitIdCollection commitIds = projectionClient.getState(testProjectionName, CommitIdCollection.class).get();
		System.out.println(commitIds);
		assertEquals(1, commitIds.count);
		assertNotNull(commitIds.getCommitIds());
		assertEquals(1, commitIds.getCommitIds().length);
	}

	@Data
	public static class CommitIdCollection{ 
		final int count;
		final String[] commitIds;
		
		public CommitIdCollection( @JsonProperty("count") int count, @JsonProperty("commitIds") String[] commitIds) {
			this.commitIds = commitIds;
			this.count = count;
		}
	} 
}

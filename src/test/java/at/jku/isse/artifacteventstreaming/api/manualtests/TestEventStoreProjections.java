package at.jku.isse.artifacteventstreaming.api.manualtests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.URI;
import java.util.LinkedList;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.eventstore.dbclient.DeleteStreamOptions;
import com.eventstore.dbclient.EventStoreDBProjectionManagementClient;
import com.fasterxml.jackson.annotation.JsonProperty;

import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import lombok.Data;

class TestEventStoreProjections {

	public static URI NS = URI.create("http://eventstoreprojection#main");
	
	private static EventStoreFactory factory = new EventStoreFactory();
	
	String testProjectionName = "testProjection";
	
	EventStoreDBProjectionManagementClient projectionClient;
	// NOTE!!! for testing run eventstore with param: --dev --insecure --run-projections=All
	// then you can disable all system projection except for event type!
	
	
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
	
	private void createTestStreamContent(String nsPostfix, int counterOffset) throws Exception{
		OntModel m = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF);
		var listener = new StatementAggregator();
		listener.registerWithModel(m);
		
		var schemaUtils = new PropertyCardinalityTypes(m); 
		var props = new LinkedList<OntRelationalProperty>();
		var types = new LinkedList<OntClass>();
		
		for (int i = counterOffset; i < counterOffset+1; i++) {
			var type = m.createOntClass(NS+"type"+i);
			types.add(type);
			for (int j = 0; j < 1; j++) {
				props.add(schemaUtils.getListType().addObjectListProperty(type, NS+"prop"+i+"-"+j, type));				
			}			
		}
		var commit = new StatementCommitImpl(NS.toString()+nsPostfix, "TestCommit", null, 0, listener.retrieveAddedStatements(), listener.retrieveRemovedStatements());
		EventStoreFactory factory = new EventStoreFactory();
		var store = factory.getEventStore(NS.toString()+nsPostfix);
		store.appendCommit(commit);
		
		//List<Commit> allCommits = store.loadAllCommits();
		//assertEquals(1, allCommits.size());
	}
	
	@Test @Disabled
	void createTestEvents() throws Exception {
		createTestStreamContent("1", 0);
		createTestStreamContent("2", 0);
		createTestStreamContent("1", 1);
		createTestStreamContent("1", 2);
	}
	
	String convertionTemplate = "fromStream('$et-CommitEventType')\r\n"
			+ ".when({\r\n"
			+ "    $any: function(s, e) {\r\n"
			+ "        var subjects = \r\n"
			+ "            new Set(e.body.addedStatements\r\n"
			+ "            .filter(stmt => stmt.subject.startsWith('http'))\r\n"
			+ "            .map(stmt => stmt.subject) );\r\n"
			+ "        e.body.removedStatements\r\n"
			+ "            .filter(stmt => stmt.subject.startsWith('http'))\r\n"
			+ "            .map(stmt => stmt.subject)\r\n"
			+ "            .forEach(subj => subjects.add(subj))\r\n"
			+ "        \r\n"
			+ "        subjects.forEach(subj => \r\n"
			+ "        linkTo('resource-'+subj, e));\r\n"
			+ "    }\r\n"
			+ "})";
	
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
	
	@Test @Disabled
	void testCreateProjection() throws Exception {
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

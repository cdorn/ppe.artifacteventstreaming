package at.jku.isse.artifacteventstreaming.rdfwrapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.ReadWrite;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.testutils.ModelDiff;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.config.FrontendEventStreamingWrapperFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.config.InMemoryEventStreamingSetupFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;

class TestInvalidateWrapperCache {
	
	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/wrappercacheinvalidation");
	
	
	CountDownLatch latch;
	SyncForTestingService serviceOut;
	OntModel sourceModel;
	OntModel destModel;
	Branch branchSource;	
	Branch branchDestination;
	RuleEnabledResolver resolverSource;
	RuleEnabledResolver resolverDest;
	TestSchema schema;
		
	@BeforeEach
	void setup() throws Exception {
		var backend = new InMemoryEventStreamingSetupFactory.FactoryBuilder().withBranchName("backend").withRepoURI(repoURI).build();
		var frontend = new FrontendEventStreamingWrapperFactory.FactoryBuilder(backend).withFrontendBranchName("frontend").build();
		sourceModel = backend.getBranch().getModel();
		destModel = frontend.getBranch().getModel();
		resolverDest = frontend.getResolver();
		resolverSource = backend.getResolver();
		branchSource = backend.getBranch();
		branchDestination = frontend.getBranch();	
		
		backend.getBranch().getDataset().begin(ReadWrite.READ);
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			ModelDiff.printDiff(sourceModel, destModel, true);
		}
		assertEquals(sizeSource, sizeDest);
		backend.getBranch().getDataset().end();
		
		frontend.signalExternalSetupComplete();
		backend.signalExternalSetupComplete();
		
		branchSource.getDataset().begin(ReadWrite.WRITE);
		schema = new TestSchema(resolverSource);
		branchSource.commitChanges("schema created");
	}
	
	@Test
	void testAddAndRemoveProperty() throws Exception {
		// setup test interceptor service
		latch = new CountDownLatch(2); // number of commits we will dispatch (incl the schema creation commit in the setup method)
		serviceOut = new SyncForTestingService("OutDestination", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	
						
		resolverSource.startWriteTransaction();
		var issue1 = resolverSource.createInstance(TestSchema.NS+"Issue1", schema.issueType);
		issue1.setSingleProperty(TestSchema.CoreProperties.state.getURI(), "Open");
		var task1 = resolverSource.createInstance(TestSchema.NS+"Task1", schema.taskType);
		task1.setSingleProperty(TestSchema.CoreProperties.key.getURI(), "TASK1");
		issue1.put(TestSchema.CoreProperties.config.getURI(), "EntryKey1", task1);
		resolverSource.concludeTransaction();
		
		//wait for changes to have propagated
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);

		// check if RDF data is same
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			var diff = ModelDiff.printDiff(sourceModel, destModel, true);
			assertEquals(0, diff.getKey());
			assertEquals(0, diff.getValue());
		}

		// now lets check destination branch for changes
		var issueCopy1 = resolverDest.findInstanceById(TestSchema.NS+"Issue1");
		var taskCopy1 = resolverDest.findInstanceById(TestSchema.NS+"Task1");
		assertTrue(issueCopy1.isPresent());
		assertTrue(taskCopy1.isPresent());
		
		// new latch:
		branchDestination.removeOutgoingCommitDistributer(serviceOut);
		latch = new CountDownLatch(1); // number of additional commits we will dispatch 
		serviceOut = new SyncForTestingService("OutDestination2", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	
		
		resolverSource.startWriteTransaction();
		// let change schema, adding and removing:
		var issueType = schema.getIssueType();
		// wrapper does not offer removal of properties, done via underlying schema support (hence wrapper at source will not notice schema change, but target wrapper should)
		resolverSource.getMetaschemata().getSingleType().removeSingleProperty(issueType.getType(), issueType.getPropertyType(TestSchema.CoreProperties.state.getURI()).getProperty());
		issueType.createListPropertyType(TestSchema.NS+"dynamicList", schema.taskType.getAsPropertyType());
		resolverSource.concludeTransaction();
		//wait for changes to have propagated
		success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);

		// check if RDF data is same
		sizeSource = sourceModel.size();
		sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			var diff = ModelDiff.printDiff(sourceModel, destModel, true);
			assertEquals(0, diff.getKey());
			assertEquals(0, diff.getValue());
		}

		// lets check if issue type and subtype have these changes visible:
		var issueTypeCopy = resolverDest.findNonDeletedInstanceTypeByFQN(issueType.getId());
		var taskTypeCopy = resolverDest.findNonDeletedInstanceTypeByFQN(schema.getTaskType().getId());
		assertTrue(issueTypeCopy.isPresent());
		assertTrue(taskTypeCopy.isPresent());
		assertNull(issueTypeCopy.get().getPropertyType(TestSchema.CoreProperties.state.getURI()));
		assertNull(taskTypeCopy.get().getPropertyType(TestSchema.CoreProperties.state.getURI()));
		assertNotNull(issueTypeCopy.get().getPropertyType(TestSchema.NS+"dynamicList"));
		assertNotNull(taskTypeCopy.get().getPropertyType(TestSchema.NS+"dynamicList"));
	}
	
	@Test
	void testCreateAndFindInstance() throws Exception {
		// setup test interceptor service
		latch = new CountDownLatch(2); // number of commits we will dispatch (incl the schema creation commit in the setup method)
		serviceOut = new SyncForTestingService("OutDestination", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	
				
		resolverSource.startWriteTransaction();
		var issue1 = resolverSource.createInstance(TestSchema.NS+"Issue1", schema.issueType);
		issue1.setSingleProperty(TestSchema.CoreProperties.state.getURI(), "Open");
		var task1 = resolverSource.createInstance(TestSchema.NS+"Task1", schema.taskType);
		task1.setSingleProperty(TestSchema.CoreProperties.key.getURI(), "TASK1");
		issue1.put(TestSchema.CoreProperties.config.getURI(), "EntryKey1", task1);
		resolverSource.concludeTransaction();
		
		//wait for changes to have propagated
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);
		
		// check if RDF data is same
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			var diff = ModelDiff.printDiff(sourceModel, destModel, true);
			assertEquals(0, diff.getKey());
			assertEquals(0, diff.getValue());
		}
		
		// now lets check destination branch for changes
		var issueCopy1 = resolverDest.findInstanceById(TestSchema.NS+"Issue1");
		var taskCopy1 = resolverDest.findInstanceById(TestSchema.NS+"Task1");
		assertTrue(issueCopy1.isPresent());
		assertTrue(taskCopy1.isPresent());
		assertEquals("Open", issueCopy1.get().getTypedProperty(TestSchema.CoreProperties.state.getURI(), String.class));
		assertEquals("TASK1", taskCopy1.get().getTypedProperty(TestSchema.CoreProperties.key.getURI(), String.class));
		Map<String, RDFInstance> configCopy = issueCopy1.get().getTypedProperty(TestSchema.CoreProperties.config.getURI(), Map.class);
		assertEquals(task1.getId(), configCopy.get("EntryKey1").getId());
	}
	
	@Test
	void testCreateRemoveAndDontFindInstance() throws Exception {
		// setup test interceptor service
		latch = new CountDownLatch(3); // number of commits we will dispatch (incl the schema creation commit in the setup method)
		serviceOut = new SyncForTestingService("OutDestination", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	
				
		resolverSource.startWriteTransaction();
		var issue1 = resolverSource.createInstance(TestSchema.NS+"Issue1", schema.issueType);
		issue1.setSingleProperty(TestSchema.CoreProperties.state.getURI(), "Open");
		var task1 = resolverSource.createInstance(TestSchema.NS+"Task1", schema.taskType);
		task1.setSingleProperty(TestSchema.CoreProperties.key.getURI(), "TASK1");
		task1.put(TestSchema.CoreProperties.config.getURI(), "EntryKey1", task1);
		resolverSource.concludeTransaction();
		
		// now delete and other changes
		resolverSource.startWriteTransaction();
		issue1.delete();
		var issue2 = resolverSource.createInstance(TestSchema.NS+"Issue2", schema.issueType);
		issue2.setSingleProperty(TestSchema.CoreProperties.state.getURI(), "Closed");
		task1.put(TestSchema.CoreProperties.config.getURI(), "EntryKey1", issue2);
		resolverSource.concludeTransaction();
		
		//wait for changes to have propagated
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);
		
		// check if RDF data is same
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			var diff = ModelDiff.printDiff(sourceModel, destModel, true);
			assertEquals(0, diff.getKey());
			assertEquals(0, diff.getValue());
		}
		
		// now lets check destination branch for changes
		var issueCopy1 = resolverDest.findInstanceById(TestSchema.NS+"Issue1");
		var taskCopy1 = resolverDest.findInstanceById(TestSchema.NS+"Task1");
		assertFalse(issueCopy1.isPresent());
		assertTrue(taskCopy1.isPresent());
		assertEquals("TASK1", taskCopy1.get().getTypedProperty(TestSchema.CoreProperties.key.getURI(), String.class));
		Map<String, RDFInstance> configCopy = task1.getTypedProperty(TestSchema.CoreProperties.config.getURI(), Map.class);
		assertEquals(1, configCopy.size());
		assertEquals(issue2.getId(), configCopy.get("EntryKey1").getId());
	}

	
}

package at.jku.isse.artifacteventstreaming.rule.derivedproperties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.rule.MockSchema;
import at.jku.isse.artifacteventstreaming.rule.RuleRepositoryInspector;
import at.jku.isse.artifacteventstreaming.rule.definition.DerivedPropertyRuleDefinition;
import at.jku.isse.artifacteventstreaming.testutils.ModelDiff;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;
import at.jku.isse.passiveprocessengine.rdfwrapper.config.FrontendEventStreamingWrapperFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.config.InMemoryEventStreamingSetupFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;

class TestDerivedPropertyPropagationAcrossBranches {
	
	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repoWithRepair");
	
	
	CountDownLatch latch;
	SyncForTestingService serviceOut;
	OntModel sourceModel;
	OntModel destModel;
	Branch branchSource;	
	Branch branchDestination;
	RuleEnabledResolver resolverSource;
	RuleEnabledResolver resolverDest;
	MockSchema schema;
		
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
		schema = new MockSchema(sourceModel, resolverSource.getMetaschemata());
		branchSource.commitChanges("schema created");
	}
	
	@Test
	void testDerivedRulePropagationToOtherBranch() throws Exception {				
		// setup test interceptor service
		latch = new CountDownLatch(2);
		serviceOut = new SyncForTestingService("OutDestination", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	
		
		branchSource.getDataset().begin(ReadWrite.WRITE);
		// get and activate a rule
		var ruleDef = resolverSource.getRuleRepo().getRuleBuilder()
				.withContextType(schema.issueType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.key.size()")
				.forDerivedProperty(schema.derivedLongProperty)
				.build();
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		assertTrue(ruleDef instanceof DerivedPropertyRuleDefinition);			
		branchSource.commitChanges("Init commit");
		
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);						
		assertEquals(2,	serviceOut.getReceivedCommits().size());
		
		var defs = resolverDest.getRuleRepo().getRuleDefinitions().stream().toList();
		assertEquals(1, defs.size());
		var def = defs.get(0);
		assertTrue(def instanceof DerivedPropertyRuleDefinition);
		
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			var diff = ModelDiff.printDiff(sourceModel, destModel, true);
			assertEquals(0, diff.getKey());
			assertEquals(0, diff.getValue());
		}
		
	}
	
	
	@Test
	void testDerivedSetRulePropagationToOtherBranch() throws Exception {				
		// setup test interceptor service
		latch = new CountDownLatch(2);
		serviceOut = new SyncForTestingService("OutDestination", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	
		
		branchSource.getDataset().begin(ReadWrite.WRITE);
		// get and activate a rule
		var ruleDef = resolverSource.getRuleRepo().getRuleBuilder()
				.withContextType(schema.issueType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.downstream")
				.forDerivedProperty(schema.upstreamProperty)
				.build();
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		assertTrue(ruleDef instanceof DerivedPropertyRuleDefinition);			
		branchSource.commitChanges("Init commit");
		
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);						
		assertEquals(2,	serviceOut.getReceivedCommits().size());
		
		var defs = resolverDest.getRuleRepo().getRuleDefinitions().stream().toList();
		assertEquals(1, defs.size());
		var def = defs.get(0);
		assertTrue(def instanceof DerivedPropertyRuleDefinition);
		
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			var diff = ModelDiff.printDiff(sourceModel, destModel, true);
			assertEquals(0, diff.getKey());
			assertEquals(0, diff.getValue());
		}
		
	}
	
	
		
	
	@Test
	void testDerivedPropertyPropagationToOtherBranch() throws Exception {		
		// setup test interceptor service
		latch = new CountDownLatch(3);
		serviceOut = new SyncForTestingService("OutDestination", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	

		branchSource.getDataset().begin(ReadWrite.WRITE);
		// get and activate a rule
		var ruleDef = resolverSource.getRuleRepo().getRuleBuilder()
				.withContextType(schema.issueType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.requirements")
				.forDerivedProperty(schema.downstreamProperty)
				.build();
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		assertTrue(ruleDef instanceof DerivedPropertyRuleDefinition);			
		branchSource.commitChanges("Init commit");
			
		// create instances
		branchSource.getDataset().begin();	
		var issue1 = schema.createIssue("issue1");
		var issue2 = schema.createIssue("issue2", issue1);
		var issue3 = schema.createIssue("issue3", issue1, issue2);
		
		branchSource.commitChanges("Creation");
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);
		
		var copy3 = branchDestination.getModel().getIndividual(issue3.getURI());
		var iter = copy3.listProperties(schema.downstreamProperty.asProperty());
		List<String> downstream = new LinkedList<>();
		while(iter.hasNext()) {
			downstream.add(iter.next().getResource().getURI());
		}
		assertEquals(2, downstream.size());
		assertTrue(downstream.contains(issue1.getURI()));
		assertTrue(downstream.contains(issue2.getURI()));
		
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			ModelDiff.printDiff(sourceModel, destModel, true);
		}
		assertEquals(sizeSource, sizeDest);
	}

	
}

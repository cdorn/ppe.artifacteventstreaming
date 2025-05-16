package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.testutils.ModelDiff;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;
import at.jku.isse.passiveprocessengine.rdfwrapper.config.FrontendEventStreamingWrapperFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.config.InMemoryEventStreamingSetupFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;

class TestRepairPropagationAcrossBranches {
	
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
		
		
	}
	
	
	
	@Test
	void testRepairPropagationToOtherBranch() throws Exception {		
		// setup test interceptor service
		latch = new CountDownLatch(3);
		serviceOut = new SyncForTestingService("OutDestination", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	
		
		branchSource.getDataset().begin(ReadWrite.WRITE);
		schema = new MockSchema(sourceModel, resolverSource.getMetaschemata()); // create types for testing
		branchSource.commitChanges("TestSchemaAdded");
		
		var inspectorSource = new RuleRepositoryInspector(resolverSource.getRuleSchema());
		var inspectorDest = new RuleRepositoryInspector(resolverDest.getRuleSchema());
		
		// get and activate a rule
		branchSource.getDataset().begin(ReadWrite.WRITE);	
		schema.getRegisteredRuleRequirementsSizeEq2(1, resolverSource.getRuleRepo());
		branchSource.commitChanges("Init commit");
		
		// create instances
		branchSource.getDataset().begin();	
		var issue1 = schema.createIssue("issue1");
		var issue2 = schema.createIssue("issue2", issue1);
		var issue3 = schema.createIssue("issue3", issue1, issue2);
		
		branchSource.commitChanges("Creation");
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);
		
		System.out.println("SCOPES AND REPAIRS IN SOURCE:");
		branchSource.getDataset().begin(ReadWrite.READ);	
		inspectorSource.getAllScopes().forEach(scope -> inspectorSource.printScope(scope));
		var evalWrappers1 = resolverSource.getRuleRepo().getEvaluations().getEvaluations();
		evalWrappers1.stream()
			.filter(eval -> !eval.isConsistent())
			.map(eval -> { System.out.println("ROOT "+eval.getRepairTree()); return eval;})
			.flatMap(eval -> inspectorSource.getFlatRepairTree(eval).stream())
			.forEach(dto -> System.out.println(" NODES:" +dto.toString()));
		assertEquals(3, evalWrappers1.size());
		branchSource.getDataset().end();	
		
		System.out.println("SCOPES IN DEST:");
		inspectorDest.getAllScopes().forEach(scope -> inspectorDest.printScope(scope));
		
		assertEquals(3,	serviceOut.getReceivedCommits().size());
		var outCommits = serviceOut.getReceivedCommits();
		var outCommit = outCommits.get(outCommits.size()-1);
		List<Statement> ruleResultStmts = outCommit.getAddedStatements().stream()
				.filter(stmt -> stmt.getPredicate().equals(resolverDest.getRuleSchema().getEvaluationHasConsistentResultProperty()))
				.map(Statement.class::cast).toList();
		assertEquals(3, ruleResultStmts.size());
		
		var evalWrappers = resolverDest.getRuleRepo().getEvaluations().getEvaluations();
		
		
		evalWrappers.stream()
			.filter(eval -> !eval.isConsistent())
			.map(eval -> { System.out.println(eval.getContextInstance().getLocalName()); return eval;})
			.flatMap(eval -> inspectorDest.getFlatRepairTree(eval).stream())
			.forEach(dto -> System.out.println(dto.toString()));
		assertEquals(3, evalWrappers.size());
		
		
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			ModelDiff.printDiff(sourceModel, destModel, true);
		}
		assertEquals(sizeSource, sizeDest);
	}

	@Test
	void testRepairUpdateToOtherBranch() throws Exception {		
		// setup test interceptor service
		latch = new CountDownLatch(4);
		serviceOut = new SyncForTestingService("OutDestination", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	
		
		branchSource.getDataset().begin(ReadWrite.WRITE);
		schema = new MockSchema(sourceModel, resolverSource.getMetaschemata()); // create types for testing
		branchSource.commitChanges("TestSchemaAdded");
		
		var inspectorSource = new RuleRepositoryInspector(resolverSource.getRuleSchema());
		var inspectorDest = new RuleRepositoryInspector(resolverDest.getRuleSchema());
		
		// get and activate a rule
		branchSource.getDataset().begin();	
		schema.getRegisteredRuleRequirementsSizeEq2(1, resolverSource.getRuleRepo());
		branchSource.commitChanges("Init commit");
		
		// create instances
		branchSource.getDataset().begin();	
		var issue1 = schema.createIssue("issue1");
		var issue2 = schema.createIssue("issue2", issue1);
		var issue3 = schema.createIssue("issue3", issue1, issue2);
		branchSource.commitChanges("Creation");
		
		// create instances
		branchSource.getDataset().begin();	
		schema.addRequirement(issue1, issue2);
		schema.addRequirement(issue1, issue3);
		schema.addRequirement(issue2, issue3);
		branchSource.commitChanges("RepairingUpdate");
				
		
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);
		
		System.out.println("SCOPES AND REPAIRS IN SOURCE:");
		branchSource.getDataset().begin(ReadWrite.READ);	
		inspectorSource.getAllScopes().forEach(scope -> inspectorSource.printScope(scope));
		var evalWrappers1 = resolverSource.getRuleRepo().getEvaluations().getEvaluations();
		evalWrappers1.stream()
			.filter(eval -> !eval.isConsistent())
			.map(eval -> { System.out.println("ROOT "+eval.getRepairTree()); return eval;})
			.flatMap(eval -> inspectorSource.getFlatRepairTree(eval).stream())
			.forEach(dto -> System.out.println(" NODES:" +dto.toString()));
		assertEquals(3, evalWrappers1.size());
		branchSource.getDataset().end();	
		
		System.out.println("SCOPES IN DEST:");
		inspectorDest.getAllScopes().forEach(scope -> inspectorDest.printScope(scope));
		
		assertEquals(4,	serviceOut.getReceivedCommits().size());
		
		var outCommits = serviceOut.getReceivedCommits();
		var outCommit = outCommits.get(outCommits.size()-1);
		List<Statement> ruleResultStmts = outCommit.getAddedStatements().stream()
				.filter(stmt -> stmt.getPredicate().equals(resolverDest.getRuleSchema().getEvaluationHasConsistentResultProperty()))
				.map(Statement.class::cast).toList();
		assertEquals(2, ruleResultStmts.size());
		
		var evalWrappers = resolverDest.getRuleRepo().getEvaluations().getEvaluations();
		evalWrappers.stream()
			.filter(eval -> !eval.isConsistent())
			.map(eval -> { System.out.println(eval.getContextInstance().getLocalName()); return eval;})
			.flatMap(eval -> inspectorDest.getFlatRepairTree(eval).stream())
			.forEach(dto -> System.out.println(dto.toString()));
		assertEquals(3, evalWrappers.size());
		assertEquals(0, evalWrappers.stream()
		.filter(eval -> !eval.isConsistent()).count());
		
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			ModelDiff.printDiff(sourceModel, destModel, true);
		}
		assertEquals(sizeSource, sizeDest);
	}
	
	@Test
	void testRepairTreeOnlyUpdateToOtherBranch() throws Exception {		
		// setup test interceptor service
		latch = new CountDownLatch(4);
		serviceOut = new SyncForTestingService("OutDestination", latch, branchDestination.getBranchResource().getModel());
		branchDestination.appendOutgoingCommitDistributer(serviceOut);	
		
		branchSource.getDataset().begin(ReadWrite.WRITE);
		schema = new MockSchema(sourceModel, resolverSource.getMetaschemata()); // create types for testing
		branchSource.commitChanges("TestSchemaAdded");
		
		var inspectorSource = new RuleRepositoryInspector(resolverSource.getRuleSchema());
		var inspectorDest = new RuleRepositoryInspector(resolverDest.getRuleSchema());
		
		// get and activate a rule
		branchSource.getDataset().begin();	
		schema.getRegisteredRuleRequirementsSizeEq2(1, resolverSource.getRuleRepo());
		branchSource.commitChanges("Init commit");
		
		// create instances
		branchSource.getDataset().begin();	
		var issue1 = schema.createIssue("issue1");
		var issue2 = schema.createIssue("issue2", issue1);
		var issue3 = schema.createIssue("issue3", issue1, issue2);
		branchSource.commitChanges("Creation");
		
		// create instances
		branchSource.getDataset().begin();	
		schema.addRequirement(issue1, issue2);
		branchSource.commitChanges("NonRepairingUpdate");
				
		
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);
		
		System.out.println("SCOPES AND REPAIRS IN SOURCE:");
		branchSource.getDataset().begin(ReadWrite.READ);	
		inspectorSource.getAllScopes().forEach(scope -> inspectorSource.printScope(scope));
		var evalWrappers1 = resolverSource.getRuleRepo().getEvaluations().getEvaluations();
		evalWrappers1.stream()
			.filter(eval -> !eval.isConsistent())
			.map(eval -> { System.out.println("ROOT "+eval.getRepairTree()); return eval;})
			.flatMap(eval -> inspectorSource.getFlatRepairTree(eval).stream())
			.forEach(dto -> System.out.println(" NODES:" +dto.toString()));
		assertEquals(3, evalWrappers1.size());
		branchSource.getDataset().end();	
		
		System.out.println("SCOPES IN DEST:");
		inspectorDest.getAllScopes().forEach(scope -> inspectorDest.printScope(scope));
		
		assertEquals(4,	serviceOut.getReceivedCommits().size());
		
		var outCommits = serviceOut.getReceivedCommits();
		var outCommit = outCommits.get(outCommits.size()-1);
		List<Statement> ruleResultStmts = outCommit.getAddedStatements().stream()
				.filter(stmt -> stmt.getPredicate().equals(resolverDest.getRuleSchema().getEvaluationHasConsistentResultProperty()))
				.map(Statement.class::cast).toList();
		assertEquals(0, ruleResultStmts.size());
		
		var evalWrappers = resolverDest.getRuleRepo().getEvaluations().getEvaluations();
		evalWrappers.stream()
			.filter(eval -> !eval.isConsistent())
			.map(eval -> { System.out.println(eval.getContextInstance().getLocalName()); return eval;})
			.flatMap(eval -> inspectorDest.getFlatRepairTree(eval).stream())
			.forEach(dto -> System.out.println(dto.toString()));
		assertEquals(3, evalWrappers.size());
		assertEquals(2, evalWrappers.stream()
		.filter(eval -> !eval.isConsistent()).count());
		
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			ModelDiff.printDiff(sourceModel, destModel, true);
		}
		assertEquals(sizeSource, sizeDest);
	}
}

package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.jena.graph.Graph;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.UnionGraph;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.ModelChangedListener;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.reasoner.InfGraph;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SimpleService;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestBranchWithRules {
	
	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repoWithRule1");
	
	CountDownLatch latch;
	SyncForTestingService serviceOut;
	OntModel model;
	Branch branch;	
	RuleTriggerObserver observer;
	RuleFactory factory;
	RuleRepository repo;
	MockSchema schema;
		
	@BeforeEach
	void setup() throws Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		
		// setup test interceptor service
		latch = new CountDownLatch(2);
		serviceOut = new SyncForTestingService("Out1", latch, repoModel);
		
		branch = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.setModelReasoner(OntSpecification.OWL2_DL_MEM_RDFS_INF)		
				.addOutgoingCommitDistributer(serviceOut)
				.build();		
		model = branch.getModel();
		schema = new MockSchema(model); // create types for testing
								
		// setup rule service
		RDFModelAccess modelAccess = new RDFModelAccess(model); // used by parser, TODO: make modelAccess non-static
		factory = new RuleFactory(model);
		repo = new RuleRepository(factory);
		observer = new RuleTriggerObserver("RuleTriggerObserver1", repoModel, factory, repo);
		// register rule service with branch
		branch.appendBranchInternalCommitService(observer);
		
		branch.startCommitHandlers(null);
		branch.getDataset().begin();
	}
	
	
	
	@Test
	void testRuleExecution() throws Exception {
		// get and activate a rule
		var def = schema.getRegisteredRuleRequirementsSizeGT1(1, repo);
		branch.commitChanges("Init commit");
		
		// create instances
		branch.getDataset().begin();	
		var issue1 = schema.createIssue("issue1");
		var issue2 = schema.createIssue("issue2");
		var issue3 = schema.createIssue("issue3", issue1, issue2);
		
		branch.commitChanges("First commit");
		boolean success = latch.await(1, TimeUnit.SECONDS);
		assert(success);
		
		assertEquals(2,	serviceOut.getReceivedCommits().size());
		var outCommit = serviceOut.getReceivedCommits().get(1);
		List<Statement> ruleResultStmts = outCommit.getAddedStatements().stream().filter(stmt -> stmt.getPredicate().equals(factory.getEvaluationHasConsistentResultProperty())).toList();
		assertEquals(3, ruleResultStmts.size());
	}

}

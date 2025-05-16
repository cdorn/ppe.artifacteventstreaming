package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Statement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestBranchWithRules {
	
	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repoWithRule1");
	
	CountDownLatch latch;
	SyncForTestingService serviceOut;
	OntModel model;
	Branch branch;	
	
	ActiveRuleTriggerObserver observer;	
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
		var metaModel = MetaModelOntology.buildInMemoryOntology(); 
		new RuleSchemaFactory(metaModel); // add rule schema to meta model		
		var cardUtil = new MetaModelSchemaTypes(model, metaModel);
		RuleTriggerObserverFactory observerFactory = new RuleTriggerObserverFactory(cardUtil);
		schema = new MockSchema(model, cardUtil); // create types for testing
								
		// setup rule service				
		observer = observerFactory.buildActiveInstance("RuleTriggerObserver1", model, repoModel);
		// register rule service with branch
		branch.appendBranchInternalCommitService(observer);
		
		branch.startCommitHandlers(null);
		branch.getDataset().begin();
	}
	
	
	
	@Test
	void testRuleExecution() throws Exception {
		// get and activate a rule
		var def = schema.getRegisteredRuleRequirementsSizeGT1(1, observer.getRepo());
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
		List<Statement> ruleResultStmts = outCommit.getAddedStatements().stream().filter(stmt -> stmt.getPredicate().equals(observer.getFactory().getEvaluationHasConsistentResultProperty())).map(Statement.class::cast).toList();
		assertEquals(3, ruleResultStmts.size());
		
		
	}

}

package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
import at.jku.isse.artifacteventstreaming.branch.incoming.CompleteCommitMerger;
import at.jku.isse.artifacteventstreaming.branch.outgoing.DefaultDirectBranchCommitStreamer;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.artifacteventstreaming.rule.evaluation.ActiveRuleTriggerObserver;
import at.jku.isse.artifacteventstreaming.rule.evaluation.PassiveRuleTriggerObserver;
import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleTriggerObserverFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.artifacteventstreaming.testutils.ModelDiff;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestRulePropagationAcrossBranches {
	
	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repoWithRule2");
	
	
	CountDownLatch latch;
	SyncForTestingService serviceOut;
	OntModel sourceModel;
	OntModel destModel;
	Branch branchSource;	
	Branch branchDestination;
	ActiveRuleTriggerObserver observerSource;
	PassiveRuleTriggerObserver observerDest;
	MockSchema schema;
		
	@BeforeEach
	void setup() throws Exception {
		var metaModel = MetaModelOntology.buildInMemoryOntology(); 
		new RuleSchemaFactory(metaModel); // add rule schema to meta model	
		
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		
		// setup test interceptor service
		latch = new CountDownLatch(2);
		serviceOut = new SyncForTestingService("OutDestination", latch, repoModel);
		
		branchDestination = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.setModelReasoner(OntSpecification.OWL2_DL_MEM_RDFS_INF)	
				.setBranchLocalName("destination")
				.addOutgoingCommitDistributer(serviceOut)
				.build();		
		//incoming merger
		var merger = new CompleteCommitMerger(branchDestination);
		branchDestination.appendIncomingCommitMerger(merger);
		//setup rules for destination branch
		destModel = branchDestination.getModel();
		var cardUtilDest = new MetaModelSchemaTypes(destModel, metaModel);
		RuleTriggerObserverFactory observerFactory = new RuleTriggerObserverFactory(cardUtilDest);
		observerDest = observerFactory.buildPassiveInstance("RuleTriggerObserverDestination", destModel, repoModel);
		branchDestination.appendBranchInternalCommitService(observerDest);		// register rule service with destination branch
		var schemaDest = new MockSchema(destModel, cardUtilDest); // create types for testing
		
		branchSource = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.setModelReasoner(OntSpecification.OWL2_DL_MEM_RDFS_INF)		
				.setBranchLocalName("source")
				.build();				
		sourceModel = branchSource.getModel();
		var cardUtilSource = new MetaModelSchemaTypes(sourceModel, metaModel);
		schema = new MockSchema(sourceModel, cardUtilSource); // create types for testing
		// setup rule service for source branch
		RuleTriggerObserverFactory observerFactorySource = new RuleTriggerObserverFactory(cardUtilSource);
		observerSource = observerFactorySource.buildActiveInstance("RuleTriggerObserverSource", sourceModel, repoModel);
		branchSource.appendBranchInternalCommitService(observerSource); // register rule service with branch
		// connect branches
		branchSource.appendOutgoingCommitDistributer(new DefaultDirectBranchCommitStreamer(branchSource, branchDestination, new InMemoryBranchStateCache()));
		
		branchDestination.startCommitHandlers(null);
		branchSource.startCommitHandlers(null);
		branchSource.getDataset().begin();
		
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			ModelDiff.printDiff(sourceModel, destModel, true);
		}
		//assertEquals(sizeSource, sizeDest);
	}
	
	
	
	@Test
	void testRulePropagationToOtherBranch() throws Exception {				
		// get and activate a rule
		var def = schema.getRegisteredRuleRequirementsSizeGT1(1, observerSource.getRepo());
		branchSource.commitChanges("Init commit");
		
		// create instances
		branchSource.getDataset().begin();	
		var issue1 = schema.createIssue("issue1");
		var issue2 = schema.createIssue("issue2");
		var issue3 = schema.createIssue("issue3", issue1, issue2);
		
		branchSource.commitChanges("Creation");
		boolean success = latch.await(5000, TimeUnit.SECONDS);
		assert(success);
		
		System.out.println("SCOPES IN SOURCE:");
		var inspectorSource = new RuleRepositoryInspector(observerSource.getFactory());
		inspectorSource.getAllScopes().forEach(scope -> inspectorSource.printScope(scope));
		
		System.out.println("SCOPES IN DEST:");
		var inspector = new RuleRepositoryInspector(observerDest.getFactory());
		inspector.getAllScopes().forEach(scope -> inspector.printScope(scope));
		
		assertEquals(2,	serviceOut.getReceivedCommits().size());
		var outCommit = serviceOut.getReceivedCommits().get(1);
		List<Statement> ruleResultStmts = outCommit.getAddedStatements().stream()
				.filter(stmt -> stmt.getPredicate().equals(observerDest.getFactory().getEvaluationHasConsistentResultProperty()))
				.map(Statement.class::cast).toList();
		assertEquals(3, ruleResultStmts.size());
		
		var sizeSource = sourceModel.size();
		var sizeDest = destModel.size();
		if (sizeSource != sizeDest) {
			ModelDiff.printDiff(sourceModel, destModel, true);
		}
		assertEquals(sizeSource, sizeDest);
	}
	
	

}

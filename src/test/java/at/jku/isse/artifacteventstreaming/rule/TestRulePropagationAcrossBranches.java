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
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.passiveprocessengine.rdf.trialcode.SyncForTestingService;

class TestRulePropagationAcrossBranches {
	
	public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repoWithRule2");
	
	
	CountDownLatch latch;
	SyncForTestingService serviceOut;
	OntModel model1;
	Branch branchSource;	
	Branch branchDestination;
	RuleTriggerObserver observerSource;
	RuleTriggerObserver observerDest;
	MockSchema schema;
		
	@BeforeEach
	void setup() throws Exception {
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
		var destModel = branchDestination.getModel();
		var metaModel = MetaModelOntology.buildInMemoryOntology(); 
		new RuleSchemaFactory(metaModel); // add rule schema to meta model		
		var cardUtil = new MetaModelSchemaTypes(destModel, metaModel);
		RuleTriggerObserverFactory observerFactory = new RuleTriggerObserverFactory(cardUtil);
		
		observerDest = observerFactory.buildInstance("RuleTriggerObserverDestination", destModel, repoModel);
		branchDestination.appendBranchInternalCommitService(observerDest);		// register rule service with destination branch
		
		branchSource = (BranchImpl) new BranchBuilder(repoURI, repoDataset, repoModel)
				.setModelReasoner(OntSpecification.OWL2_DL_MEM_RDFS_INF)		
				.setBranchLocalName("source")
				.build();		
		
		model1 = branchSource.getModel();
		schema = new MockSchema(model1, cardUtil); // create types for testing
		// setup rule service for source branch
		RuleTriggerObserverFactory observerFactorySource = new RuleTriggerObserverFactory(new MetaModelSchemaTypes(model1, metaModel));
		observerSource = observerFactorySource.buildInstance("RuleTriggerObserverSource", model1, repoModel);
		branchSource.appendBranchInternalCommitService(observerSource); // register rule service with branch
		// connect branches
		branchSource.appendOutgoingCommitDistributer(new DefaultDirectBranchCommitStreamer(branchSource, branchDestination, new InMemoryBranchStateCache()));
				
		branchSource.startCommitHandlers(null);
		branchDestination.startCommitHandlers(null);
		branchSource.getDataset().begin();
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
		
		branchSource.commitChanges("Creation commit");
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
		
		// currently model sizes are not equal, find out why:
		Set<Statement> sourceStmts = new HashSet<>();
		var iterSource = branchSource.getModel().listStatements();
		while(iterSource.hasNext()) {
			sourceStmts.add(iterSource.next());
		}
		Set<Statement> destStmts = new HashSet<>();
		var iterDest = branchDestination.getModel().listStatements();
		while(iterDest.hasNext()) {
			destStmts.add(iterDest.next());
		}
		Set<Statement> missingInSource = destStmts.stream().filter(stmt -> !sourceStmts.contains(stmt)).collect(Collectors.toSet());
		Set<Statement> missingInDest = sourceStmts.stream().filter(stmt -> !destStmts.contains(stmt)).collect(Collectors.toSet());
		
		System.out.println("MISSING IN SOURCE: "+missingInSource.size());	
		//missingInSource.stream().forEach(stmt -> System.out.println(stmt));
		System.out.println("MISSING IN DESTINATION: "+missingInDest.size());
		//missingInDest.stream().forEach(stmt -> System.out.println(stmt));
		
		// var diffModel = branchDestination.getModel().difference(branchSource.getModel());
		// RDFDataMgr.write(System.out, diffModel, Lang.TURTLE) ;
		// restrictions are generated in each branch with different anonIDs hence we have more in destination and duplicated restriction classes
		// fixed now
	}
	
	

}

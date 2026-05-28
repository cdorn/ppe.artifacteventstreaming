package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.api.CoreBranch;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.replay.CommitContainmentAugmenter;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestMapListContentSync {

	static final String NS = "http://at.jku.isse.test/cachesync#";
	static final String BRANCH_A_URI = NS + "branchA";

	OntModel modelA;
	MetaModelSchemaTypes schemaA;
	StatementAggregator aggrA;
	CommitContainmentAugmenter augmenterA;

	OntModel modelB;
	MetaModelSchemaTypes schemaB;
	MetaModelSchemaCacheSynchronizer updater;

	@BeforeEach
	void setup() {
		var metaModel = MetaModelOntology.buildInMemoryOntology();

		modelA = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_RDFS_INF);
		schemaA = new MetaModelSchemaTypes(modelA, metaModel);
		aggrA = new StatementAggregator();
		aggrA.registerWithModel(modelA);
		augmenterA = new CommitContainmentAugmenter(BRANCH_A_URI, modelA, schemaA);

		modelB = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_RDFS_INF);
		schemaB = new MetaModelSchemaTypes(modelB, metaModel);

		CoreBranch mockBranch = mock(CoreBranch.class);
		OntIndividual mockBranchResource = mock(OntIndividual.class);
		when(mockBranch.getBranchName()).thenReturn("branchB");
		when(mockBranch.getBranchId()).thenReturn(NS + "branchB");
		when(mockBranch.getBranchResource()).thenReturn(mockBranchResource);
		when(mockBranchResource.getModel()).thenReturn(modelB);

		updater = new MetaModelSchemaCacheSynchronizer(schemaB, mockBranch.getBranchId());
	}

	private void drainA() {
		aggrA.drainAddedStatements();
		aggrA.drainRemovedStatements();
	}

	private StatementCommitImpl drainToCommit(String msg) {
		return new StatementCommitImpl(BRANCH_A_URI, msg, "", 0,
				aggrA.drainAddedStatements(), aggrA.drainRemovedStatements());
	}

	/**
	 * Simulates receiving a commit from branch A on branch B:
	 * 1) augment commit on source model
	 * 2) process removals (before applying to model, per PropertyDefinitionRemovedCacheUpdater contract)
	 * 3) apply statement changes to target model
	 * 4) process additions (after applying to model, per PropertyDefinitionAddedCacheUpdater contract)
	 */
	private void syncCommitToModelB(StatementCommitImpl commit) {
		augmenterA.handleCommit(commit);

		for (ContainedStatement stmt : commit.getRemovedStatements()) {
			modelB.getGraph().delete(stmt.asTriple());
		}
		for (ContainedStatement stmt : commit.getAddedStatements()) {
			modelB.getGraph().add(stmt.asTriple());
		}
		updater.handleCommit(commit);
	}

	@Test
	void syncListInstanceChangesFromAToB() {
		var clsA = modelA.createOntClass(NS + "Issue");
		modelB.createOntClass(NS + "Issue");
		drainA();

		schemaA.getListType().addLiteralListProperty(clsA, NS + "labels", modelA.getDatatype(XSD.xstring));
		syncCommitToModelB(drainToCommit("addListSchema"));

		// Create instance in A, wrap the list, add initial entry, sync to B
		var listPropA = modelA.getObjectProperty(NS + "labels");
		var issueA = clsA.createIndividual(NS + "issue1");
		var listWrapperA = new UntypedListWrapper(issueA, listPropA, schemaA.getListType());
		listWrapperA.add(ResourceFactory.createStringLiteral("alpha"));
		syncCommitToModelB(drainToCommit("createInstance"));

		// Obtain list reference in B and verify initial state
		var issueB = modelB.getIndividual(NS + "issue1");
		assertNotNull(issueB, "individual must exist in B after sync");
		var listPropB = modelB.getObjectProperty(NS + "labels");
		var listWrapperB = new UntypedListWrapper(issueB, listPropB, schemaB.getListType());
		assertEquals(1, listWrapperB.size());
		assertEquals("alpha", listWrapperB.get(0).asLiteral().getString());

		// Apply further change in A and sync to B
		listWrapperA.add(ResourceFactory.createStringLiteral("beta"));
		syncCommitToModelB(drainToCommit("addToList"));

		// The same wrapper instance in B reflects the change because UntypedListWrapper
		// delegates to a Jena Seq that reads live from the graph
		assertEquals(2, listWrapperB.size());
		assertEquals("beta", listWrapperB.get(1).asLiteral().getString());
	}

	@Test
	void syncMapInstanceChangesFromAToB() {
		var clsA = modelA.createOntClass(NS + "Issue");
		modelB.createOntClass(NS + "Issue");
		drainA();

		schemaA.getMapType().addLiteralMapProperty(clsA, NS + "kvPairs", modelA.getDatatype(XSD.xstring));
		syncCommitToModelB(drainToCommit("addMapSchema"));

		// Create instance in A, wrap the map, add initial entry, sync to B
		var mapPropA = modelA.getObjectProperty(NS + "kvPairs");
		var issueA = clsA.createIndividual(NS + "issue1");
		var mapWrapperA = UntypedMapResource.asUnsafeMapResource(issueA, mapPropA, schemaA.getMapType());
		mapWrapperA.put("key1", ResourceFactory.createStringLiteral("value1"));
		syncCommitToModelB(drainToCommit("createInstance"));

		// Obtain map reference in B and verify initial state
		var issueB = modelB.getIndividual(NS + "issue1");
		assertNotNull(issueB, "individual must exist in B after sync");
		var mapPropB = modelB.getObjectProperty(NS + "kvPairs");
		var mapWrapperB = UntypedMapResource.asUnsafeMapResource(issueB, mapPropB, schemaB.getMapType());
		assertEquals(1, mapWrapperB.size());
		assertEquals("value1", mapWrapperB.get("key1").asLiteral().getString());

		// Apply further change in A and sync to B
		mapWrapperA.put("key2", ResourceFactory.createStringLiteral("value2"));
		syncCommitToModelB(drainToCommit("addToMap"));

		// UntypedMapResource snapshots its entries into a HashMap at construction, so
		// the originally-obtained wrapper does not see the new entry
		assertEquals(1, mapWrapperB.size());

		// Re-obtaining the wrapper from the now-updated modelB reflects all entries
		var mapWrapperBRefreshed = UntypedMapResource.asUnsafeMapResource(issueB, mapPropB, schemaB.getMapType());
		assertEquals(2, mapWrapperBRefreshed.size());
		assertEquals("value1", mapWrapperBRefreshed.get("key1").asLiteral().getString());
		assertEquals("value2", mapWrapperBRefreshed.get("key2").asLiteral().getString());
	}

}

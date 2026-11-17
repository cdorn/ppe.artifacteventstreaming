package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonDeserializer;
import at.jku.isse.artifacteventstreaming.branch.serialization.StatementJsonSerializer;
import at.jku.isse.artifacteventstreaming.replay.CommitContainmentAugmenter;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Two branches backed by real TDB2 datasets (which, unlike DatasetFactory.createTxnMem(),
 * throw when accessed outside of a transaction). The synchronizer must resolve properties
 * in the target branch's model within the target's transaction, never via the statements' own models.
 */
class TestCacheSyncAcrossTdb2Branches {

	static final String NS = "http://at.jku.isse.test/tdb2sync#";
	static final String BRANCH_A_URI = NS + "branchA";

	Dataset dsA;
	Dataset dsB;
	OntModel modelA;
	OntModel modelB;
	MetaModelSchemaTypes schemaA;
	MetaModelSchemaTypes schemaB;
	StatementAggregator aggrA;
	CommitContainmentAugmenter augmenterA;
	MetaModelSchemaCacheSynchronizer updater;
	ObjectMapper mapper;

	@BeforeEach
	void setup() {
		var metaModel = MetaModelOntology.buildInMemoryOntology();

		dsA = TDB2Factory.createDataset();
		dsB = TDB2Factory.createDataset();
		dsA.begin(ReadWrite.WRITE);
		dsB.begin(ReadWrite.WRITE);
		modelA = OntModelFactory.createModel(dsA.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF);
		modelB = OntModelFactory.createModel(dsB.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF);
		schemaA = new MetaModelSchemaTypes(modelA, metaModel);
		schemaB = new MetaModelSchemaTypes(modelB, metaModel);
		aggrA = new StatementAggregator();
		aggrA.registerWithModel(modelA);
		augmenterA = new CommitContainmentAugmenter(BRANCH_A_URI, modelA, schemaA);
		updater = new MetaModelSchemaCacheSynchronizer(schemaB, modelB);

		mapper = new ObjectMapper();
		StatementJsonSerializer.registerSerializationModule(mapper);
		StatementJsonDeserializer.registerDeserializationModule(mapper);

		// metamodel and setup noise is present in both branches, only subsequent changes are transmitted
		aggrA.drainAddedStatements();
		aggrA.drainRemovedStatements();
		// B continues in its own write transaction; A's is finished after creating a commit
	}

	@AfterEach
	void teardown() {
		for (var ds : new Dataset[]{dsA, dsB}) {
			if (ds != null && ds.isInTransaction()) {
				ds.abort();
				ds.end();
			}
		}
	}

	/** creates the schema on A inside A's transaction, returns the commit and ends A's transaction */
	private StatementCommitImpl createSchemaOnA() {
		var cls = modelA.createOntClass(NS + "Task");
		schemaA.getSingleType().createSingleDataPropertyType(NS + "taskName", cls, modelA.getDatatype(XSD.xstring));
		schemaA.getSingleType().createSingleObjectPropertyType(NS + "parent", cls, cls);
		schemaA.getSetType().createObjectPropertyType(NS + "assignees", cls, cls);
		var commit = new StatementCommitImpl(BRANCH_A_URI, "schema", "", 0,
				aggrA.drainAddedStatements(), aggrA.drainRemovedStatements());
		augmenterA.handleCommit(commit);
		dsA.commit();
		dsA.end(); // statements now reference a model whose transaction is over
		return commit;
	}

	private StatementCommitImpl viaJson(StatementCommitImpl commit) throws Exception {
		Set<ContainedStatement> added = new HashSet<>();
		for (var stmt : commit.getAddedStatements()) {
			added.add(mapper.readValue(mapper.writeValueAsString(stmt), ContainedStatement.class));
		}
		Set<ContainedStatement> removed = new HashSet<>();
		for (var stmt : commit.getRemovedStatements()) {
			removed.add(mapper.readValue(mapper.writeValueAsString(stmt), ContainedStatement.class));
		}
		return new StatementCommitImpl(BRANCH_A_URI, "schema", "", 0, added, removed);
	}

	private void applyAndSyncOnB(StatementCommitImpl commit) {
		for (ContainedStatement stmt : commit.getRemovedStatements()) {
			modelB.getGraph().delete(stmt.asTriple());
		}
		for (ContainedStatement stmt : commit.getAddedStatements()) {
			modelB.getGraph().add(stmt.asTriple());
		}
		updater.handleCommit(commit);
	}

	private void assertBMatchesExpectedSchema() {
		assertTrue(schemaB.getPrimaryPropertyType().existsPrimaryProperty(NS + "taskName"));
		assertTrue(schemaB.getPrimaryPropertyType().existsPrimaryProperty(NS + "parent"));
		assertTrue(schemaB.getPrimaryPropertyType().existsPrimaryProperty(NS + "assignees"));
		assertTrue(schemaB.getSingleType().isSingleProperty(NS + "taskName"), "functional data property must be cached as single");
		assertTrue(schemaB.getSingleType().isSingleProperty(NS + "parent"), "functional object property must be cached as single");
		assertFalse(schemaB.getSingleType().isSingleProperty(NS + "assignees"));
	}

	@Test
	void syncWithStatementsFromEndedTdb2TransactionOfOtherBranch() {
		var commit = createSchemaOnA();
		assertFalse(commit.getAddedStatements().isEmpty());

		// would throw "Not in a transaction" if the synchronizer touched A's model
		assertDoesNotThrow(() -> applyAndSyncOnB(commit));
		assertBMatchesExpectedSchema();
		assertTrue(dsB.isInTransaction());
		dsB.commit();
	}

	@Test
	void syncWithDeserializedStatements() throws Exception {
		var received = viaJson(createSchemaOnA());

		// deserialized statements live in an empty non-Ont model, functional properties must still be detected via B's model
		assertDoesNotThrow(() -> applyAndSyncOnB(received));
		assertBMatchesExpectedSchema();
		dsB.commit();
	}

	@Test
	void syncedCachesSurviveCommitAndAreVisibleInNextTransaction() throws Exception {
		var received = viaJson(createSchemaOnA());
		applyAndSyncOnB(received);
		dsB.commit();
		dsB.end();

		dsB.begin(ReadWrite.READ);
		try {
			assertTrue(schemaB.getSingleType().isSingleProperty(NS + "taskName"));
			// a fresh schema built from B's persisted state must agree with the synchronized caches
			var reinit = new MetaModelSchemaTypes(modelB);
			assertEquals(reinit.getPrimaryPropertyType().getKnownPropertyURIs(),
					schemaB.getPrimaryPropertyType().getKnownPropertyURIs());
			assertTrue(reinit.getSingleType().isSingleProperty(NS + "parent"));
		} finally {
			dsB.end();
		}
	}
}

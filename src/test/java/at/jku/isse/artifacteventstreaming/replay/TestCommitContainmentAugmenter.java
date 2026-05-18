package at.jku.isse.artifacteventstreaming.replay;

import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.artifacteventstreaming.schemasupport.UntypedMapResource;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.Seq;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TestCommitContainmentAugmenter {

	static final URI baseURI = URI.create("http://at.jku.isse.artifacteventstreaming/test/augment#");
	final String branchURI = baseURI + "testbranch";
	StatementAggregator aggr;
	OntModel m;
	MockSchema schema;
	MetaModelSchemaTypes schemaUtil;
	CommitContainmentAugmenter augmenter;
	OntObject issue1;

	@BeforeEach
	void setup() {
		m = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM_RDFS_INF);
		var metaModel = MetaModelOntology.buildInMemoryOntology();
		schemaUtil = new MetaModelSchemaTypes(m, metaModel);
		schema = new MockSchema(m, schemaUtil);
		aggr = new StatementAggregator();
		aggr.registerWithModel(m);
		augmenter = new CommitContainmentAugmenter(branchURI, m, schemaUtil);
		issue1 = schema.createIssue("Issue1");
		drain();
	}

	@Test
	void singlePropertyChangeIsNotAugmented() {
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(1L));
		var commit = drainToCommit("singleProp", 0);
		augmenter.handleCommit(commit);

		for (ContainedStatement stmt : commit.getAddedStatements()) {
			assertEquals(stmt.getSubject(), stmt.getContainerOrSubject());
			assertEquals(stmt.getPredicate(), stmt.getContainmentPropertyOrPredicate());
		}
	}

	@Test
	void singlePropertyUpdateIsNotAugmented() {
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(1L));
		drain();

		issue1.removeAll(schema.getPriorityProperty());
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(2L));
		var commit = drainToCommit("singleUpdate", 1);
		augmenter.handleCommit(commit);

		for (ContainedStatement stmt : commit.getAddedStatements()) {
			assertEquals(stmt.getSubject(), stmt.getContainerOrSubject());
		}
		for (ContainedStatement stmt : commit.getRemovedStatements()) {
			assertEquals(stmt.getSubject(), stmt.getContainerOrSubject());
		}
	}

	@Test
	void listAdditionIsAugmentedWithOwnerAndProperty() {
		Seq seq = schemaUtil.getListType().getOrCreateSequenceFor(issue1, schema.getLabelProperty());
		drain();

		seq.add(1, "First");
		var commit = drainToCommit("listAdd", 0);
		augmenter.handleCommit(commit);

		var augmented = commit.getAddedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertFalse(augmented.isEmpty(), "List entry statements should be augmented with the owning resource");
		for (ContainedStatement stmt : augmented) {
			assertEquals(schema.getLabelProperty().asProperty(), stmt.getContainmentPropertyOrPredicate());
		}
	}

	@Test
	void listMultipleAddsAugmented() {
		Seq seq = schemaUtil.getListType().getOrCreateSequenceFor(issue1, schema.getLabelProperty());
		drain();

		seq.add(1, "First");
		seq.add(2, "Second");
		var commit = drainToCommit("listMultiAdd", 0);
		augmenter.handleCommit(commit);

		var augmented = commit.getAddedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertTrue(augmented.size() >= 2, "Multiple list additions should all be augmented");
		for (ContainedStatement stmt : augmented) {
			assertEquals(schema.getLabelProperty().asProperty(), stmt.getContainmentPropertyOrPredicate());
		}
	}

	@Test
	void mapEntryAdditionIsAugmentedWithOwnerAndProperty() {
		var map = UntypedMapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType());
		drain();

		map.put("Key1", m.createTypedLiteral(42));
		var commit = drainToCommit("mapAdd", 0);
		augmenter.handleCommit(commit);

		var augmented = commit.getAddedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertFalse(augmented.isEmpty(), "Map entry statements should be augmented with the owning resource");
		for (ContainedStatement stmt : augmented) {
			assertEquals(schema.getKeyValueProperty().asProperty(), stmt.getContainmentPropertyOrPredicate());
		}
	}

	@Test
	void mapValueUpdateIsAugmented() {
		var map = UntypedMapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType());
		map.put("Key1", m.createTypedLiteral(42));
		drain();

		map.put("Key1", m.createTypedLiteral(99));
		var commit = drainToCommit("mapUpdate", 1);
		augmenter.handleCommit(commit);

		var addedAugmented = commit.getAddedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		var removedAugmented = commit.getRemovedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertFalse(addedAugmented.isEmpty(), "Map value add should be augmented");
		assertFalse(removedAugmented.isEmpty(), "Map value remove should be augmented");
		for (ContainedStatement stmt : addedAugmented) {
			assertEquals(schema.getKeyValueProperty().asProperty(), stmt.getContainmentPropertyOrPredicate());
		}
		for (ContainedStatement stmt : removedAugmented) {
			assertEquals(schema.getKeyValueProperty().asProperty(), stmt.getContainmentPropertyOrPredicate());
		}
	}

	@Test
	void mapEntryDeletionIsAugmented() {
		var map = UntypedMapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType());
		map.put("DelKey", m.createTypedLiteral(1));
		drain();

		map.remove("DelKey");
		var commit = drainToCommit("mapDel", 1);
		augmenter.handleCommit(commit);

		var removedAugmented = commit.getRemovedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertFalse(removedAugmented.isEmpty(), "Map entry deletion should be augmented with the owning resource");
		for (ContainedStatement stmt : removedAugmented) {
			assertEquals(schema.getKeyValueProperty().asProperty(), stmt.getContainmentPropertyOrPredicate());
		}
	}

	@Test
	void mixedChangesOnlyContainedStatementsAugmented() {
		Seq seq = schemaUtil.getListType().getOrCreateSequenceFor(issue1, schema.getLabelProperty());
		var map = UntypedMapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType());
		drain();

		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(5L));
		seq.add(1, "Label");
		map.put("MixKey", m.createTypedLiteral(7));
		var commit = drainToCommit("mixed", 0);
		augmenter.handleCommit(commit);

		var directOnIssue = commit.getAddedStatements().stream()
				.filter(stmt -> stmt.getSubject().equals(issue1))
				.toList();
		for (ContainedStatement stmt : directOnIssue) {
			assertEquals(stmt.getSubject(), stmt.getContainerOrSubject(),
					"Direct property changes should not be augmented");
		}

		var augmented = commit.getAddedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertFalse(augmented.isEmpty(), "List and map statements should be augmented");
	}

	@Test
	void handleCommitFromOffsetOnlyProcessesNewStatements() {
		issue1.addProperty(schema.getPriorityProperty(), m.createTypedLiteral(1L));
		Set<? extends ContainedStatement> firstAdded = aggr.drainAddedStatements();
		Set<? extends ContainedStatement> firstRemoved = aggr.drainRemovedStatements();

		Seq seq = schemaUtil.getListType().getOrCreateSequenceFor(issue1, schema.getLabelProperty());
		drain();

		seq.add(1, "OffsetLabel");
		Set<? extends ContainedStatement> secondAdded = aggr.drainAddedStatements();
		Set<? extends ContainedStatement> secondRemoved = aggr.drainRemovedStatements();

		var commit = new StatementCommitImpl(branchURI, "offsetTest", "", 0, firstAdded, firstRemoved);
		commit.appendAddedStatements(secondAdded);
		commit.appendRemovedStatement(secondRemoved);

		int addOffset = firstAdded.size();
		int remOffset = firstRemoved.size();
		augmenter.handleCommitFromOffset(commit, addOffset, remOffset);

		var beforeOffset = commit.getAddedStatements().subList(0, addOffset);
		for (ContainedStatement stmt : beforeOffset) {
			assertEquals(stmt.getSubject(), stmt.getContainerOrSubject(),
					"Statements before offset should not be processed");
		}
	}

	@Test
	void emptyCommitDoesNotFail() {
		var commit = new StatementCommitImpl(branchURI, "empty", "", 0,
				aggr.drainAddedStatements(), aggr.drainRemovedStatements());
		assertDoesNotThrow(() -> augmenter.handleCommit(commit));
	}

	@Test
	void listCreationAndFirstInsertAugmented() {
		Seq seq = schemaUtil.getListType().getOrCreateSequenceFor(issue1, schema.getLabelProperty());
		seq.add(1, "First");
		var commit = drainToCommit("listCreateAndInsert", 0);
		augmenter.handleCommit(commit);

		var augmented = commit.getAddedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertFalse(augmented.isEmpty(),
				"List creation and first insert should augment list-related statements");
	}

	@Test
	void mapCreationAndFirstInsertAugmented() {
		var map = UntypedMapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType());
		map.put("First", m.createTypedLiteral(1));
		var commit = drainToCommit("mapCreateAndInsert", 0);
		augmenter.handleCommit(commit);

		var augmented = commit.getAddedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertFalse(augmented.isEmpty(),
				"Map entry creation should augment entry-related statements");
	}

	@Test
	void twoMapEntriesAugmentedWithSameOwner() {
		var map = UntypedMapResource.asUnsafeMapResource(issue1, schema.getKeyValueProperty().asNamed(), schemaUtil.getMapType());
		drain();

		map.put("A", m.createTypedLiteral(1));
		map.put("B", m.createTypedLiteral(2));
		var commit = drainToCommit("twoMapEntries", 0);
		augmenter.handleCommit(commit);

		var augmented = commit.getAddedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertTrue(augmented.size() >= 2, "Both map entries should be augmented");
		for (ContainedStatement stmt : augmented) {
			assertEquals(schema.getKeyValueProperty().asProperty(), stmt.getContainmentPropertyOrPredicate());
		}
	}

	@Test
	void listDeletionIsAugmentedWithOwnerAndProperty() {
		Seq seq = schemaUtil.getListType().getOrCreateSequenceFor(issue1, schema.getLabelProperty());
		seq.add(1, "First");
		drain();

		issue1.removeAll(schema.getLabelProperty().asProperty());
		seq.removeProperties();
		var commit = drainToCommit("listDel", 1);
		augmenter.handleCommit(commit);

		var removedAugmented = commit.getRemovedStatements().stream()
				.filter(stmt -> !stmt.getSubject().equals(issue1))
				.filter(stmt -> stmt.getContainerOrSubject().equals(issue1))
				.toList();
		assertFalse(removedAugmented.isEmpty(), "List deletion should be augmented with the owning resource");
		for (ContainedStatement stmt : removedAugmented) {
			assertEquals(schema.getLabelProperty().asProperty(), stmt.getContainmentPropertyOrPredicate());
		}
	}

	@Test
	void individualDeletionPassesThroughWithoutException() {
		var issue2 = schema.createIssue("Issue2");
		drain();

		issue2.removeProperties();
		var commit = drainToCommit("individualDel", 1);
		assertDoesNotThrow(() -> augmenter.handleCommit(commit));

		for (ContainedStatement stmt : commit.getRemovedStatements()) {
			assertEquals(stmt.getSubject(), stmt.getContainerOrSubject(),
					"Individual deletion should not augment statements");
		}
	}

	@Test
	void schemaClassAdditionPassesThroughWithoutException() {
		m.createOntClass(MockSchema.TEST_SCHEMA_URI + "NewArtifactType");
		var commit = drainToCommit("schemaAdd", 0);
		assertDoesNotThrow(() -> augmenter.handleCommit(commit));
	}

	@Test
	void schemaClassDeletionPassesThroughWithoutException() {
		var tempClass = m.createOntClass(MockSchema.TEST_SCHEMA_URI + "TemporaryType");
		drain();

		tempClass.removeProperties();
		var commit = drainToCommit("schemaDel", 1);
		assertDoesNotThrow(() -> augmenter.handleCommit(commit));
	}

	private void drain() {
		aggr.drainAddedStatements();
		aggr.drainRemovedStatements();
	}

	private StatementCommitImpl drainToCommit(String msg, long timestamp) {
		return new StatementCommitImpl(branchURI, msg, "", timestamp,
				aggr.drainAddedStatements(), aggr.drainRemovedStatements());
	}
}

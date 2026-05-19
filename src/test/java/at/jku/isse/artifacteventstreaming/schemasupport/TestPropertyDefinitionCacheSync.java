package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.api.CoreBranch;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.artifacteventstreaming.branch.incoming.PropertyDefinitionAddedCacheUpdater;
import at.jku.isse.artifacteventstreaming.branch.incoming.PropertyDefinitionRemovedCacheUpdater;
import at.jku.isse.artifacteventstreaming.replay.CommitContainmentAugmenter;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestPropertyDefinitionCacheSync {

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

	private void assertCachesEqual() {
		assertEquals(
				schemaA.getPrimaryPropertyType().getKnownPropertyURIs(),
				schemaB.getPrimaryPropertyType().getKnownPropertyURIs(),
				"Primary property cache mismatch");

		for (String uri : schemaA.getPrimaryPropertyType().getKnownPropertyURIs()) {
			assertEquals(
					schemaA.getSingleType().isSingleProperty(uri),
					schemaB.getSingleType().isSingleProperty(uri),
					"Single/functional property cache mismatch for " + uri);
		}

		var listA = schemaA.getListType().getOwnershipPropertyCache().stream()
				.map(Property::getURI).collect(Collectors.toSet());
		var listB = schemaB.getListType().getOwnershipPropertyCache().stream()
				.map(Property::getURI).collect(Collectors.toSet());
		assertEquals(listA, listB, "List ownership cache mismatch");

		var mapA = schemaA.getMapType().getOwnsPropertyCache().stream()
				.map(Property::getURI).collect(Collectors.toSet());
		var mapB = schemaB.getMapType().getOwnsPropertyCache().stream()
				.map(Property::getURI).collect(Collectors.toSet());
		assertEquals(mapA, mapB, "Map ownership cache mismatch");

		var listSubA = schemaA.getListType().getSubclassesCache().stream()
				.map(OntClass::getURI).collect(Collectors.toSet());
		var listSubB = schemaB.getListType().getSubclassesCache().stream()
				.map(OntClass::getURI).collect(Collectors.toSet());
		assertEquals(listSubA, listSubB, "List subclasses cache mismatch");

		var mapSubA = schemaA.getMapType().getSubclassesCache().stream()
				.map(OntClass::getURI).collect(Collectors.toSet());
		var mapSubB = schemaB.getMapType().getSubclassesCache().stream()
				.map(OntClass::getURI).collect(Collectors.toSet());
		assertEquals(mapSubA, mapSubB, "Map subclasses cache mismatch");
	}

	private record CacheSnapshot(
			Set<String> primaryPropertyURIs,
			Set<String> functionalPropertyURIs,
			Set<String> listOwnershipURIs,
			Set<String> mapOwnsURIs,
			Set<String> listSubclassURIs,
			Set<String> mapSubclassURIs
	) {}

	private CacheSnapshot captureCache(MetaModelSchemaTypes schema) {
		var primaryURIs = schema.getPrimaryPropertyType().getKnownPropertyURIs();
		return new CacheSnapshot(
				primaryURIs,
				primaryURIs.stream()
						.filter(uri -> schema.getSingleType().isSingleProperty(uri))
						.collect(Collectors.toSet()),
				schema.getListType().getOwnershipPropertyCache().stream()
						.map(Property::getURI).collect(Collectors.toSet()),
				schema.getMapType().getOwnsPropertyCache().stream()
						.map(Property::getURI).collect(Collectors.toSet()),
				schema.getListType().getSubclassesCache().stream()
						.map(OntClass::getURI).collect(Collectors.toSet()),
				schema.getMapType().getSubclassesCache().stream()
						.map(OntClass::getURI).collect(Collectors.toSet())
		);
	}

	private void assertSnapshotEquals(CacheSnapshot expected, CacheSnapshot actual, String context) {
		assertEquals(expected.primaryPropertyURIs(), actual.primaryPropertyURIs(), context + ": primary property cache mismatch");
		assertEquals(expected.functionalPropertyURIs(), actual.functionalPropertyURIs(), context + ": functional property cache mismatch");
		assertEquals(expected.listOwnershipURIs(), actual.listOwnershipURIs(), context + ": list ownership cache mismatch");
		assertEquals(expected.mapOwnsURIs(), actual.mapOwnsURIs(), context + ": map ownership cache mismatch");
		assertEquals(expected.listSubclassURIs(), actual.listSubclassURIs(), context + ": list subclass cache mismatch");
		assertEquals(expected.mapSubclassURIs(), actual.mapSubclassURIs(), context + ": map subclass cache mismatch");
	}

	// ---- Create OntClass tests ----

	@Test
	void createClassWithSingleDataProperty() {
		drainA();
		var cls = modelA.createOntClass(NS + "Task");
		schemaA.getSingleType().createSingleDataPropertyType(NS + "taskName", cls, modelA.getDatatype(XSD.xstring));
		syncCommitToModelB(drainToCommit("createSingleData"));
		assertCachesEqual();
	}

	@Test
	void createClassWithSingleObjectProperty() {
		drainA();
		var cls = modelA.createOntClass(NS + "Task");
		schemaA.getSingleType().createSingleObjectPropertyType(NS + "parent", cls, cls);
		syncCommitToModelB(drainToCommit("createSingleObj"));
		assertCachesEqual();
	}

	@Test
	void createClassWithSetProperty() {
		drainA();
		var cls = modelA.createOntClass(NS + "Task");
		schemaA.getSetType().createObjectPropertyType(NS + "assignees", cls, cls);
		syncCommitToModelB(drainToCommit("createSet"));
		assertCachesEqual();
	}

	@Test
	void createClassWithLiteralListProperty() {
		drainA();
		var cls = modelA.createOntClass(NS + "Task");
		schemaA.getListType().addLiteralListProperty(cls, NS + "tags", modelA.getDatatype(XSD.xstring));
		syncCommitToModelB(drainToCommit("createLitList"));
		assertCachesEqual();
	}

	@Test
	void createClassWithObjectListProperty() {
		drainA();
		var cls = modelA.createOntClass(NS + "Task");
		var valueType = modelA.createOntClass(NS + "Label");
		schemaA.getListType().addObjectListProperty(cls, NS + "labels", valueType);
		syncCommitToModelB(drainToCommit("createObjList"));
		assertCachesEqual();
	}

	@Test
	void createClassWithLiteralMapProperty() {
		drainA();
		var cls = modelA.createOntClass(NS + "Task");
		schemaA.getMapType().addLiteralMapProperty(cls, NS + "attrs", modelA.getDatatype(XSD.xint));
		syncCommitToModelB(drainToCommit("createLitMap"));
		assertCachesEqual();
	}

	@Test
	void createClassWithObjectMapProperty() {
		drainA();
		var cls = modelA.createOntClass(NS + "Task");
		var valueType = modelA.createOntClass(NS + "Ref");
		schemaA.getMapType().addObjectMapProperty(cls, NS + "refs", valueType);
		syncCommitToModelB(drainToCommit("createObjMap"));
		assertCachesEqual();
	}

	@Test
	void createClassWithAllPropertyTypes() {
		drainA();
		var cls = modelA.createOntClass(NS + "FullTask");
		schemaA.getSingleType().createSingleDataPropertyType(NS + "title", cls, modelA.getDatatype(XSD.xstring));
		schemaA.getSingleType().createSingleObjectPropertyType(NS + "owner", cls, cls);
		schemaA.getSetType().createObjectPropertyType(NS + "deps", cls, cls);
		schemaA.getListType().addLiteralListProperty(cls, NS + "tags", modelA.getDatatype(XSD.xstring));
		schemaA.getMapType().addLiteralMapProperty(cls, NS + "metadata", modelA.getDatatype(XSD.xstring));
		syncCommitToModelB(drainToCommit("createAllTypes"));
		assertCachesEqual();
	}

	// ---- Update OntClass tests (add property to existing class) ----

	@Test
	void updateClassAddSingleProperty() {
		var clsA = modelA.createOntClass(NS + "Issue");
		modelB.createOntClass(NS + "Issue");
		drainA();

		schemaA.getSingleType().createSingleDataPropertyType(NS + "priority", clsA, modelA.getDatatype(XSD.xint));
		syncCommitToModelB(drainToCommit("addSingle"));
		assertCachesEqual();
	}

	@Test
	void updateClassAddSetProperty() {
		var clsA = modelA.createOntClass(NS + "Issue");
		modelB.createOntClass(NS + "Issue");
		drainA();

		schemaA.getSetType().createObjectPropertyType(NS + "related", clsA, clsA);
		syncCommitToModelB(drainToCommit("addSet"));
		assertCachesEqual();
	}

	@Test
	void updateClassAddListProperty() {
		var clsA = modelA.createOntClass(NS + "Issue");
		modelB.createOntClass(NS + "Issue");
		drainA();

		schemaA.getListType().addLiteralListProperty(clsA, NS + "labels", modelA.getDatatype(XSD.xstring));
		syncCommitToModelB(drainToCommit("addList"));
		assertCachesEqual();
	}

	@Test
	void updateClassAddMapProperty() {
		var clsA = modelA.createOntClass(NS + "Issue");
		modelB.createOntClass(NS + "Issue");
		drainA();

		schemaA.getMapType().addLiteralMapProperty(clsA, NS + "kvPairs", modelA.getDatatype(XSD.xstring));
		syncCommitToModelB(drainToCommit("addMap"));
		assertCachesEqual();
	}

	// ---- Update OntClass tests (remove individual property, keep class) ----

	@Test
	void updateClassRemoveSingleProperty() {
		var clsA = modelA.createOntClass(NS + "Issue");
		var clsB = modelB.createOntClass(NS + "Issue");
		var propA = schemaA.getSingleType().createSingleDataPropertyType(NS + "toRemove", clsA, modelA.getDatatype(XSD.xstring));
		schemaB.getSingleType().createSingleDataPropertyType(NS + "toRemove", clsB, modelB.getDatatype(XSD.xstring));
		drainA();

		schemaA.getSingleType().removeSingleProperty(propA);
		syncCommitToModelB(drainToCommit("removeSingle"));
		assertCachesEqual();
	}

	@Test
	void updateClassRemoveListProperty() {
		var clsA = modelA.createOntClass(NS + "Issue");
		var clsB = modelB.createOntClass(NS + "Issue");
		var propA = schemaA.getListType().addLiteralListProperty(clsA, NS + "toRemoveList", modelA.getDatatype(XSD.xstring));
		schemaB.getListType().addLiteralListProperty(clsB, NS + "toRemoveList", modelB.getDatatype(XSD.xstring));
		drainA();

		schemaA.getListType().removeListOwnershipPropertyDefinition(propA);
		syncCommitToModelB(drainToCommit("removeList"));
		assertCachesEqual();
	}

	@Test
	void updateClassRemoveMapProperty() {
		var clsA = modelA.createOntClass(NS + "Issue");
		var clsB = modelB.createOntClass(NS + "Issue");
		var propA = schemaA.getMapType().addLiteralMapProperty(clsA, NS + "toRemoveMap", modelA.getDatatype(XSD.xint));
		schemaB.getMapType().addLiteralMapProperty(clsB, NS + "toRemoveMap", modelB.getDatatype(XSD.xint));
		drainA();

		schemaA.getMapType().removeMapOwnershipPropertyDefinition(propA);
		syncCommitToModelB(drainToCommit("removeMap"));
		assertCachesEqual();
	}

	// ---- Delete OntClass tests ----

	@Test
	void deleteClassWithSingleProperty() {
		var clsA = modelA.createOntClass(NS + "Temp");
		var clsB = modelB.createOntClass(NS + "Temp");
		schemaA.getSingleType().createSingleDataPropertyType(NS + "tempProp", clsA, modelA.getDatatype(XSD.xstring));
		schemaB.getSingleType().createSingleDataPropertyType(NS + "tempProp", clsB, modelB.getDatatype(XSD.xstring));
		drainA();

		schemaA.deleteOntClassInclOwnedProperties(clsA);
		syncCommitToModelB(drainToCommit("deleteSingle"));
		assertCachesEqual();
	}

	@Test
	void deleteClassWithSetProperty() {
		var clsA = modelA.createOntClass(NS + "Temp");
		var clsB = modelB.createOntClass(NS + "Temp");
		schemaA.getSetType().createObjectPropertyType(NS + "tempSet", clsA, clsA);
		schemaB.getSetType().createObjectPropertyType(NS + "tempSet", clsB, clsB);
		drainA();

		schemaA.deleteOntClassInclOwnedProperties(clsA);
		syncCommitToModelB(drainToCommit("deleteSet"));
		assertCachesEqual();
	}

	@Test
	void deleteClassWithListProperty() {
		var clsA = modelA.createOntClass(NS + "Temp");
		var clsB = modelB.createOntClass(NS + "Temp");
		schemaA.getListType().addLiteralListProperty(clsA, NS + "tempList", modelA.getDatatype(XSD.xstring));
		schemaB.getListType().addLiteralListProperty(clsB, NS + "tempList", modelB.getDatatype(XSD.xstring));
		drainA();

		schemaA.deleteOntClassInclOwnedProperties(clsA);
		syncCommitToModelB(drainToCommit("deleteList"));
		assertCachesEqual();
	}

	@Test
	void deleteClassWithMapProperty() {
		var clsA = modelA.createOntClass(NS + "Temp");
		var clsB = modelB.createOntClass(NS + "Temp");
		schemaA.getMapType().addLiteralMapProperty(clsA, NS + "tempMap", modelA.getDatatype(XSD.xint));
		schemaB.getMapType().addLiteralMapProperty(clsB, NS + "tempMap", modelB.getDatatype(XSD.xint));
		drainA();

		schemaA.deleteOntClassInclOwnedProperties(clsA);
		syncCommitToModelB(drainToCommit("deleteMap"));
		assertCachesEqual();
	}

	@Test
	void deleteClassWithAllPropertyTypes() {
		var clsA = modelA.createOntClass(NS + "Full");
		var clsB = modelB.createOntClass(NS + "Full");
		schemaA.getSingleType().createSingleDataPropertyType(NS + "fullSingle", clsA, modelA.getDatatype(XSD.xstring));
		schemaB.getSingleType().createSingleDataPropertyType(NS + "fullSingle", clsB, modelB.getDatatype(XSD.xstring));
		schemaA.getSetType().createObjectPropertyType(NS + "fullSet", clsA, clsA);
		schemaB.getSetType().createObjectPropertyType(NS + "fullSet", clsB, clsB);
		schemaA.getListType().addLiteralListProperty(clsA, NS + "fullList", modelA.getDatatype(XSD.xstring));
		schemaB.getListType().addLiteralListProperty(clsB, NS + "fullList", modelB.getDatatype(XSD.xstring));
		schemaA.getMapType().addLiteralMapProperty(clsA, NS + "fullMap", modelA.getDatatype(XSD.xint));
		schemaB.getMapType().addLiteralMapProperty(clsB, NS + "fullMap", modelB.getDatatype(XSD.xint));
		drainA();

		schemaA.deleteOntClassInclOwnedProperties(clsA);
		syncCommitToModelB(drainToCommit("deleteAll"));
		assertCachesEqual();
	}

	@Test
	void deleteClassWithSubclasses() {
		var parentA = modelA.createOntClass(NS + "Parent");
		var childA = modelA.createOntClass(NS + "Child");
		childA.addSuperClass(parentA);
		var parentB = modelB.createOntClass(NS + "Parent");
		var childB = modelB.createOntClass(NS + "Child");
		childB.addSuperClass(parentB);

		schemaA.getSingleType().createSingleDataPropertyType(NS + "parentProp", parentA, modelA.getDatatype(XSD.xstring));
		schemaB.getSingleType().createSingleDataPropertyType(NS + "parentProp", parentB, modelB.getDatatype(XSD.xstring));
		schemaA.getListType().addLiteralListProperty(childA, NS + "childList", modelA.getDatatype(XSD.xstring));
		schemaB.getListType().addLiteralListProperty(childB, NS + "childList", modelB.getDatatype(XSD.xstring));
		drainA();

		schemaA.deleteOntClassInclSubclasses(parentA);
		syncCommitToModelB(drainToCommit("deleteWithSubclasses"));
		assertCachesEqual();
	}

	// ---- Transaction abort tests (cache rollback) ----

	@Test
	void abortRevertsAddedSinglePropertyInRemoteCache() {
		var clsA = modelA.createOntClass(NS + "Task");
		modelB.createOntClass(NS + "Task");
		drainA();

		var snapshotABefore = captureCache(schemaA);
		var snapshotBBefore = captureCache(schemaB);

		schemaA.getSingleType().createSingleDataPropertyType(NS + "priority", clsA, modelA.getDatatype(XSD.xint));
		var commit = drainToCommit("addSingle");

		schemaB.afterTransactionStarted();
		syncCommitToModelB(commit);
		schemaB.afterTransactionAborted();

		assertSnapshotEquals(snapshotBBefore, captureCache(schemaB), "B cache should revert after abort");
		assertSnapshotEquals(snapshotABefore, captureCache(schemaB), "B cache should match A's pre-change state");
	}

	@Test
	void abortRevertsAddedListPropertyInRemoteCache() {
		var clsA = modelA.createOntClass(NS + "Task");
		modelB.createOntClass(NS + "Task");
		drainA();

		var snapshotABefore = captureCache(schemaA);
		var snapshotBBefore = captureCache(schemaB);

		schemaA.getListType().addLiteralListProperty(clsA, NS + "tags", modelA.getDatatype(XSD.xstring));
		var commit = drainToCommit("addList");

		schemaB.afterTransactionStarted();
		syncCommitToModelB(commit);
		schemaB.afterTransactionAborted();

		assertSnapshotEquals(snapshotBBefore, captureCache(schemaB), "B cache should revert after abort");
		assertSnapshotEquals(snapshotABefore, captureCache(schemaB), "B cache should match A's pre-change state");
	}

	@Test
	void abortRevertsAddedMapPropertyInRemoteCache() {
		var clsA = modelA.createOntClass(NS + "Task");
		modelB.createOntClass(NS + "Task");
		drainA();

		var snapshotABefore = captureCache(schemaA);
		var snapshotBBefore = captureCache(schemaB);

		schemaA.getMapType().addLiteralMapProperty(clsA, NS + "attrs", modelA.getDatatype(XSD.xint));
		var commit = drainToCommit("addMap");

		schemaB.afterTransactionStarted();
		syncCommitToModelB(commit);
		schemaB.afterTransactionAborted();

		assertSnapshotEquals(snapshotBBefore, captureCache(schemaB), "B cache should revert after abort");
		assertSnapshotEquals(snapshotABefore, captureCache(schemaB), "B cache should match A's pre-change state");
	}

	@Test
	void abortRevertsRemovedSinglePropertyInRemoteCache() {
		var clsA = modelA.createOntClass(NS + "Issue");
		var clsB = modelB.createOntClass(NS + "Issue");
		var propA = schemaA.getSingleType().createSingleDataPropertyType(NS + "toRemove", clsA, modelA.getDatatype(XSD.xstring));
		schemaB.getSingleType().createSingleDataPropertyType(NS + "toRemove", clsB, modelB.getDatatype(XSD.xstring));
		drainA();

		var snapshotABefore = captureCache(schemaA);
		var snapshotBBefore = captureCache(schemaB);

		schemaA.getSingleType().removeSingleProperty(propA);
		var commit = drainToCommit("removeSingle");

		schemaB.afterTransactionStarted();
		syncCommitToModelB(commit);
		schemaB.afterTransactionAborted();

		assertSnapshotEquals(snapshotBBefore, captureCache(schemaB), "B cache should revert after abort");
		assertSnapshotEquals(snapshotABefore, captureCache(schemaB), "B cache should match A's pre-change state");
	}

	@Test
	void abortRevertsRemovedListPropertyInRemoteCache() {
		var clsA = modelA.createOntClass(NS + "Issue");
		var clsB = modelB.createOntClass(NS + "Issue");
		var propA = schemaA.getListType().addLiteralListProperty(clsA, NS + "toRemoveList", modelA.getDatatype(XSD.xstring));
		schemaB.getListType().addLiteralListProperty(clsB, NS + "toRemoveList", modelB.getDatatype(XSD.xstring));
		drainA();

		var snapshotABefore = captureCache(schemaA);
		var snapshotBBefore = captureCache(schemaB);

		schemaA.getListType().removeListOwnershipPropertyDefinition(propA);
		var commit = drainToCommit("removeList");

		schemaB.afterTransactionStarted();
		syncCommitToModelB(commit);
		schemaB.afterTransactionAborted();

		assertSnapshotEquals(snapshotBBefore, captureCache(schemaB), "B cache should revert after abort");
		assertSnapshotEquals(snapshotABefore, captureCache(schemaB), "B cache should match A's pre-change state");
	}

	@Test
	void abortRevertsRemovedMapPropertyInRemoteCache() {
		var clsA = modelA.createOntClass(NS + "Issue");
		var clsB = modelB.createOntClass(NS + "Issue");
		var propA = schemaA.getMapType().addLiteralMapProperty(clsA, NS + "toRemoveMap", modelA.getDatatype(XSD.xint));
		schemaB.getMapType().addLiteralMapProperty(clsB, NS + "toRemoveMap", modelB.getDatatype(XSD.xint));
		drainA();

		var snapshotABefore = captureCache(schemaA);
		var snapshotBBefore = captureCache(schemaB);

		schemaA.getMapType().removeMapOwnershipPropertyDefinition(propA);
		var commit = drainToCommit("removeMap");

		schemaB.afterTransactionStarted();
		syncCommitToModelB(commit);
		schemaB.afterTransactionAborted();

		assertSnapshotEquals(snapshotBBefore, captureCache(schemaB), "B cache should revert after abort");
		assertSnapshotEquals(snapshotABefore, captureCache(schemaB), "B cache should match A's pre-change state");
	}

	@Test
	void abortRevertsAllPropertyTypesInRemoteCache() {
		var clsA = modelA.createOntClass(NS + "FullTask");
		modelB.createOntClass(NS + "FullTask");
		drainA();

		var snapshotABefore = captureCache(schemaA);
		var snapshotBBefore = captureCache(schemaB);

		schemaA.getSingleType().createSingleDataPropertyType(NS + "title", clsA, modelA.getDatatype(XSD.xstring));
		schemaA.getSingleType().createSingleObjectPropertyType(NS + "owner", clsA, clsA);
		schemaA.getSetType().createObjectPropertyType(NS + "deps", clsA, clsA);
		schemaA.getListType().addLiteralListProperty(clsA, NS + "tags", modelA.getDatatype(XSD.xstring));
		schemaA.getMapType().addLiteralMapProperty(clsA, NS + "metadata", modelA.getDatatype(XSD.xstring));
		var commit = drainToCommit("addAllTypes");

		schemaB.afterTransactionStarted();
		syncCommitToModelB(commit);
		schemaB.afterTransactionAborted();

		assertSnapshotEquals(snapshotBBefore, captureCache(schemaB), "B cache should revert after abort");
		assertSnapshotEquals(snapshotABefore, captureCache(schemaB), "B cache should match A's pre-change state");
	}
}

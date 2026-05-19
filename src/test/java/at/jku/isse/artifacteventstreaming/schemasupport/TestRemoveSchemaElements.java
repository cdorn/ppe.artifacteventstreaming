package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestRemoveSchemaElements {

	public static final String NS = "http://test/schemamanagement#";
	
	MetaModelOntology metaModel;
	MetaModelSchemaTypes metaTypes;
	OntModel model;
	Dataset dataset;
	
	@BeforeEach
	void setup() {
		dataset = TDB2Factory.createDataset();
		dataset.begin(ReadWrite.WRITE);
		model = OntModelFactory.createModel(dataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		metaModel = MetaModelOntology.buildInMemoryOntology();
		metaTypes = new MetaModelSchemaTypes(model, metaModel);
		dataset.commit();
		dataset.end();
	}
	
	@Test
	void testSimpleAddAndRemoveClass() {
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
		var knownURIsBegin = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		// create class plus properties
		var ontClass = model.createOntClass(NS+"Demo");
		var sizeMiddle = model.size();
		var sizeMetaMiddle = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertTrue(sizeMiddle > sizeBegin);
		assertEquals(sizeMetaBegin, sizeMetaMiddle);
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		// remove class incl properties
		metaTypes.deleteOntClassInclOwnedProperties(ontClass);
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		assertEquals(sizeBegin, sizeEnd);
		var knownURIsEnd = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		assertEquals(knownURIsBegin.size(), knownURIsEnd.size());
	}
	
	@Test
	void testSimpleAddAndRemoveClassWithListProperty() {
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
		var knownURIsBegin = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		var modelBegin = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		modelBegin.add(model);
		// create class plus properties
		var ontClass = model.createOntClass(NS+"Demo");
		var prop = metaTypes.getListType().addLiteralListProperty(ontClass, "demoList", model.getDatatype(XSD.xstring));
		
		var sizeMiddle = model.size();
		var sizeMetaMiddle = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertTrue(sizeMiddle > sizeBegin);
		assertEquals(sizeMetaBegin, sizeMetaMiddle);
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		// remove class incl properties
		metaTypes.deleteOntClassInclOwnedProperties(ontClass);
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd != sizeBegin) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin, sizeEnd);
		var knownURIsEnd = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		assertEquals(knownURIsBegin.size(), knownURIsEnd.size());
	}
	
	@Test
	void testSimpleAddAndRemoveClassWithMapProperty() {
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
		var knownURIsBegin = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		var modelBegin = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		modelBegin.add(model);
		// create class plus properties
		var ontClass = model.createOntClass(NS+"Demo");
		var prop = metaTypes.getMapType().addLiteralMapProperty(ontClass, "demoMap", model.getDatatype(XSD.xstring));
		
		var sizeMiddle = model.size();
		var sizeMetaMiddle = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertTrue(sizeMiddle > sizeBegin);
		assertEquals(sizeMetaBegin, sizeMetaMiddle);
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		// remove class incl properties
		metaTypes.deleteOntClassInclOwnedProperties(ontClass);
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd != sizeBegin) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin, sizeEnd);
		var knownURIsEnd = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		assertEquals(knownURIsBegin.size(), knownURIsEnd.size());
	}

	@Test
	void testSimpleAddAndRemoveClassWithSingleProperty() {
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
		var knownURIsBegin = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		var modelBegin = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		modelBegin.add(model);
		// create class plus properties
		var ontClass = model.createOntClass(NS+"Demo");
		var prop = metaTypes.getSingleType().createSingleDataPropertyType(NS+"demoSingle", ontClass, model.getDatatype(XSD.xstring));
		
		var subPropsSize = metaTypes.getSingleType().getSinglePropertyCount();
		var sizeMiddle = model.size();
		var sizeMetaMiddle = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertTrue(sizeMiddle > sizeBegin);
		assertEquals(sizeMetaBegin, sizeMetaMiddle);
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		// remove class incl properties
		metaTypes.deleteOntClassInclOwnedProperties(ontClass);
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		var subPropsSizeEnd =  metaTypes.getSingleType().getSinglePropertyCount();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd != sizeBegin) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin, sizeEnd);
		assertEquals(subPropsSize, subPropsSizeEnd+1);
		var knownURIsEnd = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		assertEquals(knownURIsBegin.size(), knownURIsEnd.size());
	}
	
	@Test
	void testSimpleAddAndRemoveClassWithSetProperty() {
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
		var knownURIsBegin = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		var modelBegin = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		modelBegin.add(model);
		// create class plus properties
		var ontClass = model.createOntClass(NS+"Demo");
		var prop = metaTypes.getSingleType().createBaseDataPropertyType(NS+"demoSingle", ontClass, model.getDatatype(XSD.xstring));
		
		var sizeMiddle = model.size();
		var sizeMetaMiddle = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertTrue(sizeMiddle > sizeBegin);
		assertEquals(sizeMetaBegin, sizeMetaMiddle);
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		// remove class incl properties
		metaTypes.deleteOntClassInclOwnedProperties(ontClass);
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd != sizeBegin) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin, sizeEnd);
		var knownURIsEnd = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		assertEquals(knownURIsBegin.size(), knownURIsEnd.size());
	}
	
	@Test
	void testSubClassRemoval() {
		// create a class hierarchy, remove sub class,
		// ensure super class still exists with its properties	
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeMetaBegin = metaModel.getMetamodel().size();
		
		var modelBegin = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		modelBegin.add(model);
		// create class plus properties
		var ontClass = model.createOntClass(NS+"Demo");
		var propList = metaTypes.getListType().addLiteralListProperty(ontClass, "demoList", model.getDatatype(XSD.xstring));
		var propMap = metaTypes.getMapType().addLiteralMapProperty(ontClass, "demoMap", model.getDatatype(XSD.xstring));
		var propSet = metaTypes.getSingleType().createBaseDataPropertyType(NS+"demoSingle", ontClass, model.getDatatype(XSD.xstring));
		var propSingle = metaTypes.getSingleType().createSingleDataPropertyType(NS+"demoSingle", ontClass, model.getDatatype(XSD.xstring));
		
		var sizeBegin = model.size();
		var knownURIsBegin = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		var ontSubClass = model.createOntClass(NS+"SubDemo");
		ontClass.addSubClass(ontSubClass);
		var subpropList = metaTypes.getListType().addLiteralListProperty(ontSubClass, "demoListSub", model.getDatatype(XSD.xstring));
		var subpropMap = metaTypes.getMapType().addLiteralMapProperty(ontSubClass, "demoMapSub", model.getDatatype(XSD.xstring));
		var subpropSet = metaTypes.getSingleType().createBaseDataPropertyType(NS+"demoSingleSub", ontSubClass, model.getDatatype(XSD.xstring));
		var subpropSingle = metaTypes.getSingleType().createSingleDataPropertyType(NS+"demoSingleSub", ontSubClass, model.getDatatype(XSD.xstring));
		
		
		var sizeMiddle = model.size();
		var sizeMetaMiddle = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertTrue(sizeMiddle > sizeBegin);
		assertEquals(sizeMetaBegin, sizeMetaMiddle);
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		// remove class incl properties
		metaTypes.deleteOntClassInclOwnedProperties(ontSubClass);
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd != sizeBegin) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin, sizeEnd);
		var knownURIsEnd = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		assertEquals(knownURIsBegin.size(), knownURIsEnd.size());
	}
	
	@Test
	void testClassHierarchyRemoval() {
		// create a class hierarchy, remove all super class,
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
		var knownURIsBegin = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		var modelBegin = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		modelBegin.add(model);
		// create class plus properties
		var ontClass = model.createOntClass(NS+"Demo");
		var propList = metaTypes.getListType().addLiteralListProperty(ontClass, "demoList", model.getDatatype(XSD.xstring));
		var propMap = metaTypes.getMapType().addLiteralMapProperty(ontClass, "demoMap", model.getDatatype(XSD.xstring));
		var propSet = metaTypes.getSingleType().createBaseDataPropertyType(NS+"demoSingle", ontClass, model.getDatatype(XSD.xstring));
		var propSingle = metaTypes.getSingleType().createSingleDataPropertyType(NS+"demoSingle", ontClass, model.getDatatype(XSD.xstring));
		
		
		var ontSubClass = model.createOntClass(NS+"SubDemo");
		ontClass.addSubClass(ontSubClass);
		var subpropList = metaTypes.getListType().addLiteralListProperty(ontSubClass, "demoListSub", model.getDatatype(XSD.xstring));
		var subpropMap = metaTypes.getMapType().addLiteralMapProperty(ontSubClass, "demoMapSub", model.getDatatype(XSD.xstring));
		var subpropSet = metaTypes.getSingleType().createBaseDataPropertyType(NS+"demoSingleSub", ontSubClass, model.getDatatype(XSD.xstring));
		var subpropSingle = metaTypes.getSingleType().createSingleDataPropertyType(NS+"demoSingleSub", ontSubClass, model.getDatatype(XSD.xstring));
		
		
		var sizeMiddle = model.size();
		var sizeMetaMiddle = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertTrue(sizeMiddle > sizeBegin);
		assertEquals(sizeMetaBegin, sizeMetaMiddle);
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		// remove class incl properties
		metaTypes.deleteOntClassInclSubclasses(ontClass);
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd != sizeBegin) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin, sizeEnd);	
		var knownURIsEnd = metaTypes.getPrimaryPropertyType().getKnownPropertyURIs();
		assertEquals(knownURIsBegin.size(), knownURIsEnd.size());
	}
	
	private void printDiff(OntModel modelBegin, OntModel model) {
		dataset.begin(ReadWrite.READ);
		var modelDiff = model.size() > modelBegin.size()
				? model.difference(modelBegin)
				: modelBegin.difference(model);
		RDFDataMgr.write(System.out, modelDiff, Lang.TURTLE) ;
		dataset.end();
	}

	// ---- Cache snapshot infrastructure for tx abort tests ----

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

	// ---- Transaction abort tests (direct property creation/removal cache rollback) ----

	@Test
	void abortRevertsDirectSinglePropertyCreation() {
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var ontClass = model.createOntClass(NS + "Base");
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();

		var snapshotBefore = captureCache(metaTypes);

		metaTypes.afterTransactionStarted();
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		metaTypes.getSingleType().createSingleDataPropertyType(NS + "txProp", ontClass, model.getDatatype(XSD.xstring));
		dataset.abort();
		dataset.end();
		metaModel.getMetaontology().end();
		metaTypes.afterTransactionAborted();

		assertSnapshotEquals(snapshotBefore, captureCache(metaTypes), "Single property creation abort");
	}

	@Test
	void abortRevertsDirectSetPropertyCreation() {
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var ontClass = model.createOntClass(NS + "Base");
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();

		var snapshotBefore = captureCache(metaTypes);

		metaTypes.afterTransactionStarted();
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		metaTypes.getSetType().createObjectPropertyType(NS + "txSetProp", ontClass, ontClass);
		dataset.abort();
		dataset.end();
		metaModel.getMetaontology().end();
		metaTypes.afterTransactionAborted();

		assertSnapshotEquals(snapshotBefore, captureCache(metaTypes), "Set property creation abort");
	}

	@Test
	void abortRevertsDirectListPropertyCreation() {
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var ontClass = model.createOntClass(NS + "Base");
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();

		var snapshotBefore = captureCache(metaTypes);

		metaTypes.afterTransactionStarted();
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		metaTypes.getListType().addLiteralListProperty(ontClass, NS + "txListProp", model.getDatatype(XSD.xstring));
		dataset.abort();
		dataset.end();
		metaModel.getMetaontology().end();
		metaTypes.afterTransactionAborted();

		assertSnapshotEquals(snapshotBefore, captureCache(metaTypes), "List property creation abort");
	}

	@Test
	void abortRevertsDirectMapPropertyCreation() {
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var ontClass = model.createOntClass(NS + "Base");
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();

		var snapshotBefore = captureCache(metaTypes);

		metaTypes.afterTransactionStarted();
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		metaTypes.getMapType().addLiteralMapProperty(ontClass, NS + "txMapProp", model.getDatatype(XSD.xstring));
		dataset.abort();
		dataset.end();
		metaModel.getMetaontology().end();
		metaTypes.afterTransactionAborted();

		assertSnapshotEquals(snapshotBefore, captureCache(metaTypes), "Map property creation abort");
	}

	@Test
	void abortRevertsDirectAllPropertyTypeCreation() {
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var ontClass = model.createOntClass(NS + "Base");
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();

		var snapshotBefore = captureCache(metaTypes);

		metaTypes.afterTransactionStarted();
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		metaTypes.getSingleType().createSingleDataPropertyType(NS + "txSingle", ontClass, model.getDatatype(XSD.xstring));
		metaTypes.getSingleType().createSingleObjectPropertyType(NS + "txSingleObj", ontClass, ontClass);
		metaTypes.getSetType().createObjectPropertyType(NS + "txSet", ontClass, ontClass);
		metaTypes.getListType().addLiteralListProperty(ontClass, NS + "txList", model.getDatatype(XSD.xstring));
		metaTypes.getMapType().addLiteralMapProperty(ontClass, NS + "txMap", model.getDatatype(XSD.xstring));
		dataset.abort();
		dataset.end();
		metaModel.getMetaontology().end();
		metaTypes.afterTransactionAborted();

		assertSnapshotEquals(snapshotBefore, captureCache(metaTypes), "All property types creation abort");
	}

	@Test
	void abortRevertsDirectPropertyRemoval() {
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var ontClass = model.createOntClass(NS + "Base");
		var singleProp = metaTypes.getSingleType().createSingleDataPropertyType(NS + "toRemoveSingle", ontClass, model.getDatatype(XSD.xstring));
		var listProp = metaTypes.getListType().addLiteralListProperty(ontClass, NS + "toRemoveList", model.getDatatype(XSD.xstring));
		var mapProp = metaTypes.getMapType().addLiteralMapProperty(ontClass, NS + "toRemoveMap", model.getDatatype(XSD.xstring));
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();

		var snapshotBefore = captureCache(metaTypes);

		metaTypes.afterTransactionStarted();
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		metaTypes.getSingleType().removeSingleProperty(singleProp);
		metaTypes.getListType().removeListOwnershipPropertyDefinition(listProp);
		metaTypes.getMapType().removeMapOwnershipPropertyDefinition(mapProp);
		dataset.abort();
		dataset.end();
		metaModel.getMetaontology().end();
		metaTypes.afterTransactionAborted();

		assertSnapshotEquals(snapshotBefore, captureCache(metaTypes), "Property removal abort");
	}

	@Test
	void abortRevertsDirectClassDeletionWithProperties() {
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var ontClass = model.createOntClass(NS + "ToDelete");
		metaTypes.getSingleType().createSingleDataPropertyType(NS + "delSingle", ontClass, model.getDatatype(XSD.xstring));
		metaTypes.getListType().addLiteralListProperty(ontClass, NS + "delList", model.getDatatype(XSD.xstring));
		metaTypes.getMapType().addLiteralMapProperty(ontClass, NS + "delMap", model.getDatatype(XSD.xstring));
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();

		var snapshotBefore = captureCache(metaTypes);

		metaTypes.afterTransactionStarted();
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		metaTypes.deleteOntClassInclOwnedProperties(ontClass);
		dataset.abort();
		dataset.end();
		metaModel.getMetaontology().end();
		metaTypes.afterTransactionAborted();

		assertSnapshotEquals(snapshotBefore, captureCache(metaTypes), "Class deletion with properties abort");
	}

	@Test
	void testSuperClassRemoval() {
		// create a class hierarchy, remove super class,
		// ensure sub class still exists with its properties
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var modelBegin = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		modelBegin.add(model);
		var sizeBegin = model.size();
		// create class plus properties
		var ontClass = model.createOntClass(NS+"Demo");
		var propList = metaTypes.getListType().addLiteralListProperty(ontClass, "demoList", model.getDatatype(XSD.xstring));
		var propMap = metaTypes.getMapType().addLiteralMapProperty(ontClass, "demoMap", model.getDatatype(XSD.xstring));
		var propSet = metaTypes.getSingleType().createBaseDataPropertyType(NS+"demoSingle", ontClass, model.getDatatype(XSD.xstring));
		var propSingle = metaTypes.getSingleType().createSingleDataPropertyType(NS+"demoSingle", ontClass, model.getDatatype(XSD.xstring));
		
		var sizeMiddle = model.size();
		var diffBaseClass = sizeMiddle - sizeBegin;
		var ontSubClass = model.createOntClass(NS+"SubDemo");
		ontClass.addSubClass(ontSubClass);
		var subpropList = metaTypes.getListType().addLiteralListProperty(ontSubClass, "demoListSub", model.getDatatype(XSD.xstring));
		var subpropMap = metaTypes.getMapType().addLiteralMapProperty(ontSubClass, "demoMapSub", model.getDatatype(XSD.xstring));
		var subpropSet = metaTypes.getSingleType().createBaseDataPropertyType(NS+"demoSingleSub", ontSubClass, model.getDatatype(XSD.xstring));
		var subpropSingle = metaTypes.getSingleType().createSingleDataPropertyType(NS+"demoSingleSub", ontSubClass, model.getDatatype(XSD.xstring));
		var diffSubClass = model.size() - sizeMiddle - diffBaseClass;
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		// remove super class incl properties
		metaTypes.deleteOntClassInclOwnedProperties(ontClass);
		// ensure en model has same size without those statements of baseclass
		var sizeEnd = model.size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		
		if (sizeEnd-diffBaseClass != sizeBegin+diffSubClass) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin+diffSubClass, sizeEnd-diffBaseClass);
	}
}

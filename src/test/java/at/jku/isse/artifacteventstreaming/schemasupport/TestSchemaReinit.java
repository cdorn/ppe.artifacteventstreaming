package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TestSchemaReinit extends TestSchemaSetup {

	@Test
	void testBasePropertyCacheRecreatedOnReinit() {
		var originalURIs = metaSchema.getPrimaryPropertyType().getKnownPropertyURIs();
		assertFalse(originalURIs.isEmpty());

		var reinitSchema = new MetaModelSchemaTypes(m);
		var reinitURIs = reinitSchema.getPrimaryPropertyType().getKnownPropertyURIs();

		assertEquals(originalURIs, reinitURIs);
	}

	@Test
	void testSinglePropertyCacheRecreatedOnReinit() {
		assertTrue(metaSchema.getSingleType().isSingleProperty(NS + PRIORITY));
		assertTrue(metaSchema.getSingleType().isSingleProperty(NS + PARENT));
		int originalCount = metaSchema.getSingleType().getSinglePropertyCount();

		var reinitSchema = new MetaModelSchemaTypes(m);

		assertTrue(reinitSchema.getSingleType().isSingleProperty(NS + PRIORITY));
		assertTrue(reinitSchema.getSingleType().isSingleProperty(NS + PARENT));
		assertEquals(originalCount, reinitSchema.getSingleType().getSinglePropertyCount());
	}

	@Test
	void testListSubclassesCacheRecreatedOnReinit() {
		var listBaseClass = m.getOntClass(RDF.Seq);
		assertNotNull(listBaseClass);
		var listSubclasses = listBaseClass.subClasses().toList();
		assertFalse(listSubclasses.isEmpty());

		var listInstance = m.createIndividual(NS + "testListInst", listSubclasses.get(0));
		assertTrue(metaSchema.getListType().isListCollection(listInstance));

		var reinitSchema = new MetaModelSchemaTypes(m);
		assertTrue(reinitSchema.getListType().isListCollection(listInstance));
	}

	@Test
	void testListWasListCollectionAfterReinit() {
		var listBaseClass = m.getOntClass(RDF.Seq);
		var listSubclasses = listBaseClass.subClasses().toList();
		assertFalse(listSubclasses.isEmpty());

		List<Resource> types = List.of(listSubclasses.get(0).asResource());
		assertTrue(metaSchema.getListType().wasListCollection(types));

		var reinitSchema = new MetaModelSchemaTypes(m);
		assertTrue(reinitSchema.getListType().wasListCollection(types));
	}

	@Test
	void testMapSubclassesCacheRecreatedOnReinit() {
		var mapEntryBaseClass = m.getOntClass(MapResourceType.ENTRY_TYPE_URI);
		assertNotNull(mapEntryBaseClass);
		var mapSubclasses = mapEntryBaseClass.subClasses().toList();
		assertFalse(mapSubclasses.isEmpty());

		var mapInstance = m.createIndividual(NS + "testMapInst", mapSubclasses.get(0));
		assertTrue(metaSchema.getMapType().isMapEntry(mapInstance));

		var reinitSchema = new MetaModelSchemaTypes(m);
		assertTrue(reinitSchema.getMapType().isMapEntry(mapInstance));
	}

	@Test
	void testMapWasMapEntryAfterReinit() {
		var mapEntryBaseClass = m.getOntClass(MapResourceType.ENTRY_TYPE_URI);
		var mapSubclasses = mapEntryBaseClass.subClasses().toList();
		assertFalse(mapSubclasses.isEmpty());

		List<Resource> types = List.of(mapSubclasses.get(0).asResource());
		assertTrue(metaSchema.getMapType().wasMapEntry(types));

		var reinitSchema = new MetaModelSchemaTypes(m);
		assertTrue(reinitSchema.getMapType().wasMapEntry(types));
	}

	@Test
	void testDuplicatePreventionAfterReinit() {
		var reinitSchema = new MetaModelSchemaTypes(m);

		assertNull(reinitSchema.getSingleType().createSingleDataPropertyType(
				NS + PRIORITY, typeBase, intRange));
		assertNull(reinitSchema.getSingleType().createSingleObjectPropertyType(
				NS + PARENT, typeChild, typeBase));
		assertNull(reinitSchema.getSetType().createDataPropertyType(
				NS + "setOfString", typeBase, stringRange));
		assertNull(reinitSchema.getSetType().createObjectPropertyType(
				NS + "setofArt", typeBase, typeChild));
	}

	@Test
	void testReinitWithoutInference() {
		Dataset dataset = TDB2Factory.createDataset();
		dataset.begin(ReadWrite.WRITE);
		OntModel noInfModel = OntModelFactory.createModel(
				dataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		var meta = MetaModelOntology.buildInMemoryOntology();
		var schema1 = new MetaModelSchemaTypes(noInfModel, meta);

		String NS2 = "http://reinit.noinf.test#";
		var cls = noInfModel.createOntClass(NS2 + "TestClass");
		var valueType = noInfModel.createOntClass(NS2 + "ValueClass");
		var stringDt = noInfModel.getDatatype(XSD.xstring);

		schema1.getSingleType().createSingleDataPropertyType(NS2 + "singleData", cls, stringDt);
		schema1.getSingleType().createSingleObjectPropertyType(NS2 + "singleObj", cls, valueType);
		schema1.getSetType().createDataPropertyType(NS2 + "setData", cls, stringDt);
		schema1.getSetType().createObjectPropertyType(NS2 + "setObj", cls, valueType);
		schema1.getListType().addLiteralListProperty(cls, NS2 + "listLit", stringDt);
		schema1.getListType().addObjectListProperty(cls, NS2 + "listObj", valueType);
		schema1.getMapType().addLiteralMapProperty(cls, NS2 + "mapLit", stringDt);
		schema1.getMapType().addObjectMapProperty(cls, NS2 + "mapObj", valueType);

		var originalBaseURIs = schema1.getPrimaryPropertyType().getKnownPropertyURIs();
		int originalSingleCount = schema1.getSingleType().getSinglePropertyCount();
		dataset.commit();
		dataset.end();

		dataset.begin(ReadWrite.WRITE);
		var schema2 = new MetaModelSchemaTypes(noInfModel);
		var reinitBaseURIs = schema2.getPrimaryPropertyType().getKnownPropertyURIs();

		assertEquals(originalBaseURIs, reinitBaseURIs);
		assertEquals(originalSingleCount, schema2.getSingleType().getSinglePropertyCount());
		assertTrue(schema2.getSingleType().isSingleProperty(NS2 + "singleData"));
		assertTrue(schema2.getSingleType().isSingleProperty(NS2 + "singleObj"));
		assertFalse(schema2.getSingleType().isSingleProperty(NS2 + "setData"));

		assertNull(schema2.getSingleType().createSingleDataPropertyType(
				NS2 + "singleData", cls, stringDt));
		assertNull(schema2.getSetType().createDataPropertyType(NS2 + "setData", cls, stringDt));
		assertNull(schema2.getSetType().createObjectPropertyType(NS2 + "setObj", cls, valueType));

		var listBaseClass = noInfModel.getOntClass(RDF.Seq);
		var listSubs = listBaseClass.subClasses().toList();
		assertFalse(listSubs.isEmpty());
		var listInst = noInfModel.createIndividual(NS2 + "listInst", listSubs.get(0));
		assertTrue(schema2.getListType().isListCollection(listInst));

		var mapBaseClass = noInfModel.getOntClass(MapResourceType.ENTRY_TYPE_URI);
		var mapSubs = mapBaseClass.subClasses().toList();
		assertFalse(mapSubs.isEmpty());
		var mapInst = noInfModel.createIndividual(NS2 + "mapInst", mapSubs.get(0));
		assertTrue(schema2.getMapType().isMapEntry(mapInst));
		dataset.commit();
		dataset.end();
	}
}

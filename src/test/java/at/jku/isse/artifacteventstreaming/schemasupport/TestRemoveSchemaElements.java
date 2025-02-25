package at.jku.isse.artifacteventstreaming.schemasupport;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;

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
		
	}
	
	@Test
	void testSimpleAddAndRemoveClassWithListProperty() {
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
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
		
	}
	
	@Test
	void testSimpleAddAndRemoveClassWithMapProperty() {
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
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
		
	}

	@Test
	void testSimpleAddAndRemoveClassWithSingleProperty() {
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
		var modelBegin = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		modelBegin.add(model);
		// create class plus properties
		var ontClass = model.createOntClass(NS+"Demo");
		var prop = metaTypes.getSingleType().createSingleDataPropertyType(NS+"demoSingle", ontClass, model.getDatatype(XSD.xstring));
		
		var subPropsSize = metaTypes.getSingleType().getSingleLiteralProperty().subProperties(true).count();
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
		var subPropsSizeEnd =  metaTypes.getSingleType().getSingleLiteralProperty().subProperties(true).count();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd != sizeBegin) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin, sizeEnd);
		assertEquals(subPropsSize, subPropsSizeEnd+1);
		
	}
	
	@Test
	void testSimpleAddAndRemoveClassWithSetProperty() {
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
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
	}
	
	@Test
	void testClassHierarchyRemoval() {
		// create a class hierarchy, remove all super class,
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		var sizeBegin = model.size();
		var sizeMetaBegin = metaModel.getMetamodel().size();
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
	}
	
	private void printDiff(OntModel modelBegin, OntModel model) {
		dataset.begin(ReadWrite.READ);
		var modelDiff = model.size() > modelBegin.size() 
				? model.difference(modelBegin) 
				: modelBegin.difference(model);
		RDFDataMgr.write(System.out, modelDiff, Lang.TURTLE) ;
		dataset.end();
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

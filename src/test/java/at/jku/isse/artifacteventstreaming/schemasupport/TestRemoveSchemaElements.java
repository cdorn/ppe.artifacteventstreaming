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
		ontClass.removeProperties();
		// remove class incl properties
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
		ontClass.removeProperties();
		prop.removeProperties();
		// remove class incl properties
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd > sizeBegin) {
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
		ontClass.removeProperties();
		prop.removeProperties();
		// remove class incl properties
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd > sizeBegin) {
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
		
		var sizeMiddle = model.size();
		var sizeMetaMiddle = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertTrue(sizeMiddle > sizeBegin);
		assertEquals(sizeMetaBegin, sizeMetaMiddle);
		
		dataset.begin(ReadWrite.WRITE);
		metaModel.getMetaontology().begin(ReadWrite.READ);
		ontClass.removeProperties();
		prop.removeProperties();
		// remove class incl properties
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd > sizeBegin) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin, sizeEnd);
		
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
		ontClass.removeProperties();
		prop.removeProperties();
		// remove class incl properties
		// ensure model has same size --> no leftover statements
		var sizeEnd = model.size();
		var sizeMetaEnd = metaModel.getMetamodel().size();
		dataset.commit();
		dataset.end();
		metaModel.getMetaontology().end();
		assertEquals(sizeMetaBegin, sizeMetaEnd);
		
		if (sizeEnd > sizeBegin) {
			printDiff(modelBegin, model);
		}
		assertEquals(sizeBegin, sizeEnd);
		
	}
	
	private void printDiff(OntModel modelBegin, OntModel model) {
		dataset.begin(ReadWrite.READ);
		var modelDiff = model.difference(modelBegin);
		RDFDataMgr.write(System.out, modelDiff, Lang.TURTLE) ;
		dataset.end();
	}
	

}

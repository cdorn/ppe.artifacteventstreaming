package at.jku.isse.artifacteventstreaming.jena.performance;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedList;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;

class TestSchemaInspectionPerformance {

	public static String NS = "http://at.jku.isse.aes/performance#";
	
	@Test
	void test10kProps() {
		var dataset = DatasetFactory.createTxnMem();
		OntModel m = OntModelFactory.createModel(dataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		dataset.begin(ReadWrite.WRITE);
		var schemaUtils = new PropertyCardinalityTypes(m); 
		var props = new LinkedList<OntRelationalProperty>();
		var types = new LinkedList<OntClass>();
		
		// create large schema, 100 classes with 100 properties each
		for (int i = 0; i < 100; i++) {
			var type = m.createOntClass(NS+"type"+i);
			types.add(type);
			for (int j = 0; j < 100; j++) {
				props.add(schemaUtils.getListType().addObjectListProperty(type, NS+"prop"+i+"-"+j, type));				
			}			
		}
		assertEquals(10000, props.size());
		assertEquals(100, types.size());
		props.forEach(prop -> assertTrue(prop.ranges().findFirst().isPresent()));
		
	}

}

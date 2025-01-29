package at.jku.isse.artifacteventstreaming.rdfwrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.net.URI;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.persistence.FilebasedDatasetLoader;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType;

public class TestRDFMapWrapperOntologyPersistence  {

	public static final URI branchURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/rdfwrapper/testMapPersistanceBranch");
	
	@BeforeAll
	static void cleanDBDirectory() {
		var datasetLoader = new FilebasedDatasetLoader();	
		datasetLoader.removeDataset(branchURI);
	}
	
	
	@Test
	void createAndLoadPersistedModel() {
		var datasetLoader = new FilebasedDatasetLoader();		
		var modelDataset = datasetLoader.loadDataset(branchURI).get();
		modelDataset.begin(ReadWrite.WRITE);
		var m = OntModelFactory.createModel(modelDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		assertEquals(0, m.size());
		var cardType = new PropertyCardinalityTypes(m);
		cardType.createSingleDataPropertyType(branchURI+"testProp", cardType.getListType().getListClass(), m.getDatatype(XSD.xint));
		assertTrue(m.size() > 0);
		m.statements().forEach(stmt -> System.out.println(stmt));
		modelDataset.commit();
		modelDataset.end();
	
	
		datasetLoader = new FilebasedDatasetLoader();		
		modelDataset = datasetLoader.loadDataset(branchURI).get();
		modelDataset.begin(ReadWrite.WRITE);				
		m = OntModelFactory.createModel(modelDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		long oldSize = m.size();
		assertNotEquals(0, m.size());
		cardType = new PropertyCardinalityTypes(m);
		cardType.createSingleDataPropertyType(branchURI+"testProp", cardType.getListType().getListClass(), m.getDatatype(XSD.xint));
		long newSize = m.size();
		m.statements().forEach(stmt -> System.out.println(stmt));
		assertEquals(oldSize, newSize);
		modelDataset.commit();
		modelDataset.end();
	}
	

}

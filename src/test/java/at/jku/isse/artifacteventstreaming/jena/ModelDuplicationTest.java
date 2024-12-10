package at.jku.isse.artifacteventstreaming.jena;

import static at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType.MAP_NS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.schemasupport.MapResource;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.ResourceMismatchException;

class ModelDuplicationTest {

	public static String NS = "http://at.jku.isse.jena#";
	
	
	@Test
	void testModelDuplication() throws ResourceMismatchException {
		OntModel m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		m.setNsPrefix("isse", NS);
		m.setNsPrefix("map", MAP_NS);
		MapResourceType mapTypeDef = new MapResourceType(m);
		OntClass artifactType = m.createOntClass(NS+"artifactType");		
		OntClass mapType = mapTypeDef.getMapEntryClass();
		
		OntObjectProperty.Named hasMapProp = m.createObjectProperty(NS+"hasMapEntry");
		hasMapProp.addDomain(artifactType);
		hasMapProp.addRange(mapType);
		hasMapProp.addLabel("has entry of key/value pair");
		
		// lets create some instances:
		OntIndividual art1 = artifactType.createIndividual(NS+"art1");
		OntIndividual art2 = artifactType.createIndividual(NS+"art2");
		Map<String, RDFNode> map = MapResource.asMapResource(art1, hasMapProp, mapTypeDef);			
		map.put("key2", art2);		
		
		//create a deep copy:
		OntModel copy = OntModelFactory.createModel().add(m);
		OntClass artifactTypeCopy = copy.getOntClass(NS+"artifactType");
		OntObjectProperty.Named hasMapPropCopy = copy.getObjectProperty(NS+"hasMapEntry");
		OntIndividual art1copy = copy.getIndividual(NS+"art1");		
		OntIndividual art3copy = artifactTypeCopy.createIndividual(NS+"art3");
		MapResourceType mapTypeDef2 = new MapResourceType(copy);
		Map<String, RDFNode> mapCopy = MapResource.asMapResource(art1copy, hasMapPropCopy, mapTypeDef2);			
		assertEquals(map.size(), mapCopy.size());
		
		mapCopy.clear();		
		assertNotEquals(map.size(), mapCopy.size());
		
		
		mapCopy.put("key3", art3copy);
		assertNull(map.get("key3"));
		assertNotNull(mapCopy.get("key3"));
		
		assertFalse(m.containsResource(art3copy));
		assertTrue(m.containsResource(art1copy));
		
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		System.out.println("--- original / copy ------------");
		RDFDataMgr.write(System.out, copy, Lang.TURTLE) ;
	}

}

package at.jku.isse.artifacteventstreaming.rdf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.PPEPropertyType;
import at.jku.isse.passiveprocessengine.rdfwrapper.MapWrapper;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;

public class TestRDFInstance {

	static final String LIST_OF_ART = "listOfArt";
	static final String LIST_OF_STRING = "listOfString";
	static final String MAP_OF_ART = "mapOfArt";
	static final String PRIORITY = "priority";
	static final String PARENT = "parent";
	static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static NodeToDomainResolver resolver;
	PPEInstanceType typeBase;
	PPEInstanceType typeChild;
	PPEPropertyType parent;
	PPEPropertyType priority;
	PPEPropertyType mapOfArt;
	PPEPropertyType mapOfString;
	PPEPropertyType listOfArt;
	PPEPropertyType listOfString;
	PPEPropertyType setOfArt;
	PPEPropertyType setOfString;
	
	@BeforeEach
	void setup() {
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		m.setNsPrefix("test", NS);
		resolver = new NodeToDomainResolver(m);
		resolver.getMapEntryBaseType();
		resolver.getListBaseType();
		typeBase = resolver.createNewInstanceType(NS+"artifact");
		typeChild = resolver.createNewInstanceType(NS+"issue", typeBase);
		priority = typeBase.createSinglePropertyType(PRIORITY, BuildInType.INTEGER);
		mapOfArt = typeChild.createMapPropertyType(MAP_OF_ART, BuildInType.STRING, typeBase);
		mapOfString = typeChild.createMapPropertyType("mapOfString", BuildInType.STRING, BuildInType.STRING);
		parent = typeChild.createSinglePropertyType(PARENT, typeBase);		
		listOfArt = typeBase.createListPropertyType(LIST_OF_ART, typeChild);
		listOfString = typeBase.createListPropertyType(LIST_OF_STRING, BuildInType.STRING);
		setOfArt = typeBase.createSetPropertyType("setofArt", typeChild);
		setOfString = typeBase.createSetPropertyType("setOfString", BuildInType.STRING);
	}
	
	@Test
	void createInstance() {
		var inst = resolver.createInstance(NS+"art1", typeBase);
		assertEquals("art1", inst.getName());
	}
	
	@Test
	void createInstanceEnsureSingleInstance() {
		var inst = resolver.createInstance(NS+"art1", typeBase);
		assertEquals("art1", inst.getName());
		
		var inst2 = resolver.createInstance(NS+"art1", typeBase);
		assertEquals(inst, inst2);
	}
	
	@Test
	void listInstances() {
		var art1 = resolver.createInstance(NS+"art1", typeBase);
		var art2 = resolver.createInstance(NS+"art2", typeBase);
		
		var artSet1 = resolver.getAllInstancesOfTypeOrSubtype(typeBase);
		var artSet2 = resolver.getAllInstancesOfTypeOrSubtype(typeChild);
		
		assertEquals(0, artSet2.size());
		assertEquals(2, artSet1.size());
	}
	
	@Test
	void testUpgradeInstances() {
		var art1 = resolver.createInstance(NS+"art1", typeBase);
		var art2 = resolver.createInstance(NS+"art2", typeBase);
		var artSet1 = resolver.getAllInstancesOfTypeOrSubtype(typeBase);
		
		art1.setInstanceType(typeChild);
		art2.setInstanceType(typeChild);
		var artSet2 = resolver.getAllInstancesOfTypeOrSubtype(typeChild);
		
		assertEquals(artSet1, artSet2);
		assertEquals(2, artSet2.size());
	}
	
	@Test
	void findInstance() {
		var art1 = resolver.createInstance(NS+"art1", typeBase);
		var art2 = resolver.createInstance(NS+"art2", typeBase);
		String uri3 = NS+"art3";
		
		assertNotNull(resolver.findInstanceById(art1.getId()).get());
		assertNotNull(resolver.findInstanceById(NS+"art2").get());
		assertTrue(resolver.findInstanceById(uri3).isEmpty());
	}
	
	
	

}

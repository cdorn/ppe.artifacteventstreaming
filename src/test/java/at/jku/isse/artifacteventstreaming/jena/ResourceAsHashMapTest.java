package at.jku.isse.artifacteventstreaming.jena;

import static at.jku.isse.artifacteventstreaming.jena.MapResource.MAP_NS;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;

class ResourceAsHashMapTest {

	public static String NS = "http://at.jku.isse.jena#";
	
	
	@Test
	void testMapAsResource() throws ResourceMismatchException {
		OntModel m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		m.setNsPrefix("isse", NS);
		m.setNsPrefix("map", MAP_NS);
		OntClass artifactType = m.createOntClass(NS+"artifactType");		
		OntClass mapType = MapResource.getMapEntryClass(m);
		
		
		
		OntObjectProperty.Named hasMapProp = m.createObjectProperty(NS+"hasMapEntry");
		hasMapProp.addDomain(artifactType);
		hasMapProp.addRange(mapType);
		hasMapProp.addLabel("has entry of key/value pair");
		
		
		OntDataProperty keyProp = MapResource.getKeyProperty(m);
		OntDataProperty literalValueProp = MapResource.getLiteralValueProperty(m);		
		OntObjectProperty.Named objectValueProp = MapResource.getObjectValueProperty(m);
		
		// lets create some instances:
		OntIndividual art1 = artifactType.createIndividual(NS+"art1");
		OntIndividual art2 = artifactType.createIndividual(NS+"art2");
		OntIndividual art3 = artifactType.createIndividual(NS+"art3");
		
		// Awkward way to fill a map
		art1.addProperty(hasMapProp, 
				mapType.createIndividual().addLiteral(keyProp, "SampleKeyRefToArt2")
											.addProperty(objectValueProp, art2));
		art1.addProperty(hasMapProp, 
				mapType.createIndividual().addLiteral(keyProp, "SampleKeyRefToLiteral")
											.addLiteral(literalValueProp, "LiteralAndObjectsMixed"));
		
		// simple way to manipulate a map
		Map<String, RDFNode> map = MapResource.asMapResource(art1, hasMapProp);			
		map.put("SelfKey2", art2);
		map.put("someother", m.createTypedLiteral("test2"));
		map.put("SampleKeyRefToArt2", art3);
		
		RDFNode art2ref = map.get("SelfKey2");
		RDFNode someother = map.get("someother");
		RDFNode art3ref = map.remove("SampleKeyRefToArt2");		
		System.out.println(map.get("SampleKeyRefToLiteral").asLiteral().getString());
		
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		//Utils.printStream(mapType.declaredProperties());
		
		Reasoner reasoner = ReasonerRegistry.getOWLReasoner();
		reasoner.bindSchema(m);
		InfModel infmodel = ModelFactory.createInfModel(reasoner, m);
		Utils.printValidation(infmodel);
		
		assertEquals(art3ref, art3);
		assertEquals(art2ref, art2);
		
		map.clear();
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		assertEquals(map.size(), 0);
		
		Map<String, RDFNode> newMap = MapResource.asMapResource(art1, hasMapProp);
		assertEquals(newMap.size(), 0);
	}

}

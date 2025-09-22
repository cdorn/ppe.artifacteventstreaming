package at.jku.isse.artifacteventstreaming.schemasupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;

public class TestRDFMapWrapper extends TestSchemaSetup {

	
	@Test
	void useMapObjectPropertyOnInstance() {
		var art1 = typeChild.createIndividual(NS+"art1");
		var art2 = typeChild.createIndividual(NS+"art2");
		var base3 = typeBase.createIndividual(NS+"art3");
		var artMap = new TypedMapWrapper(art1, mapOfArt, typeChild, metaSchema.getMapType());
//		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		artMap.put("key1", art2);
		assertThrows(IllegalArgumentException.class, () ->artMap.put("key3", base3));
		
		var resultMap = new TypedMapWrapper(art1, mapOfArt, typeChild, metaSchema.getMapType());
		var entry1 = ( resultMap.get("key1"));
		assertEquals(art2, entry1);
				
		var entry3 = ( resultMap.get("key3"));
		assertNotEquals(base3, entry3);
		
		assertNull(resultMap.get("unusedkey"));
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}
	
	@Test
	void failOnUseStringMapWithObject() {
		var art1 = typeChild.createIndividual(NS+"art1");
		var art2 = typeChild.createIndividual(NS+"art2");
		var artMap = new TypedMapWrapper(art1, mapOfString, stringRange, metaSchema.getMapType());
		assertThrows(IllegalArgumentException.class, () -> artMap.put("key1", art2));
		
		Map<String, RDFNode> resultMap = new TypedMapWrapper(art1, mapOfString, stringRange, metaSchema.getMapType());
		assertNull(resultMap.get("unusedkey"));
	}
	
	@Test
	void testStreamEntries() {
		var art1 = typeChild.createIndividual(NS+"art1");
		var artMap = new TypedMapWrapper(art1, mapOfString, stringRange, metaSchema.getMapType());
		var values = List.of(m.createLiteral("1"), m.createLiteral("2"), m.createLiteral("3"));
		var keys = List.of("Key1", "Key2", "Key3");
		artMap.put(keys.get(0), values.get(0));
		artMap.put(keys.get(1), values.get(1));
		artMap.put(keys.get(2), values.get(2));				
		
		var artMap2 = new TypedMapWrapper(art1, mapOfString, stringRange, metaSchema.getMapType()); // to recreate map
		assertTrue(artMap2.values().containsAll(values));
		assertTrue(artMap2.keySet().containsAll(keys));
		
		assertEquals(3, artMap2.size());
		assertFalse(artMap2.isEmpty());
		assertTrue(artMap2.containsKey(keys.get(0)));
		assertFalse(artMap2.containsKey("unknownkey"));
		assertTrue(artMap2.containsValue(values.get(0)));
		assertFalse(artMap2.containsValue(m.createLiteral("unknown value")));
		artMap2.entrySet().stream().allMatch(entry -> entry.getKey().length() > 2);
		
		artMap2.clear();
		var artMap3 = new TypedMapWrapper(art1, mapOfString, stringRange, metaSchema.getMapType()); // to recreate map
		assertEquals(0, artMap3.size());
		
	}
	
	@Test
	void setMultipleOnes() {		
			var art1 = typeChild.createIndividual(NS+"art1");
			var rdfMap = new TypedMapWrapper(art1, mapOfString, stringRange, metaSchema.getMapType());
			var values = List.of(m.createLiteral("4"), m.createLiteral("5"), m.createLiteral("6"));
			var keys = List.of("Key1", "Key2", "Key3");
			Map<String, RDFNode> artMap = new HashMap<>();			
			artMap.put(keys.get(0), values.get(0));
			artMap.put(keys.get(1), values.get(1));
			artMap.put(keys.get(2), values.get(2));	
			rdfMap.putAll(artMap);
			
			var artMap2 = new TypedMapWrapper(art1, mapOfString, stringRange, metaSchema.getMapType()); // to recreate map
			assertTrue(artMap2.values().containsAll(values));
			assertTrue(artMap2.keySet().containsAll(keys));
			artMap2.put(keys.get(0), m.createLiteral("7"));
			assertNotNull(artMap2.remove(keys.get(1)));
	
			var artMap3 = new TypedMapWrapper(art1, mapOfString, stringRange, metaSchema.getMapType()); // to recreate map
			assertEquals(m.createLiteral("7"), artMap3.get(keys.get(0)));
			assertNull(artMap3.get(keys.get(1)));
			assertNull(artMap2.remove(keys.get(1)));
	}
	
	@Test
	void setMultipleOnesWithArt() {		
			var art1 = typeChild.createIndividual(NS+"art1");
			var art2 = typeChild.createIndividual(NS+"art2");
			var art3 = typeChild.createIndividual(NS+"art3");
			var art4 = typeChild.createIndividual(NS+"art4");
			var art5 = typeChild.createIndividual(NS+"art5");
			var rdfMap = new TypedMapWrapper(art1, mapOfArt, typeChild, metaSchema.getMapType());
			var values = List.of(art2, art3, art4);
			var keys = List.of("Key1", "Key2", "Key3");
			Map<String, RDFNode> artMap = new HashMap<>();			
			artMap.put(keys.get(0), values.get(0));
			artMap.put(keys.get(1), values.get(1));
			artMap.put(keys.get(2), values.get(2));	
			rdfMap.putAll(artMap);
			
			var artMap2 = new TypedMapWrapper(art1, mapOfArt, typeChild, metaSchema.getMapType()); // to recreate map
			assertTrue(artMap2.values().containsAll(values));
			assertTrue(artMap2.keySet().containsAll(keys));
			artMap2.put(keys.get(0), art5);
			assertNotNull(artMap2.remove(keys.get(1)));
	
			var artMap3 = new TypedMapWrapper(art1, mapOfArt, typeChild, metaSchema.getMapType()); // to recreate map
			assertEquals(art5, artMap3.get(keys.get(0)));
			assertNull(artMap3.get(keys.get(1)));
			assertNull(artMap3.remove(keys.get(1)));
			
			assertTrue(artMap3.containsValue(art5));
	}
	
}
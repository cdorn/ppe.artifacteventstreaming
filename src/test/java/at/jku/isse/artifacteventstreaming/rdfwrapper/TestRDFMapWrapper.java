package at.jku.isse.artifacteventstreaming.rdfwrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.collections.MapWrapper;

public class TestRDFMapWrapper extends TestRDFInstance {

	
	@Test
	void useObjectPropertyOnInstance() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		art1.setSingleProperty(parent.getId(), art2);
		
		var result = art1.getTypedProperty(parent.getId(), RDFInstance.class);
		assertEquals(result, art2);
	}
	
	@Test
	void useMapObjectPropertyOnInstance() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		var base3 = resolver.createInstance(NS+"art3", typeBase);
		var artMap = art1.getTypedProperty(mapOfArt.getId(), MapWrapper.class);
//		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		artMap.put("key1", art2);
		artMap.put("key3", base3);
		
		var resultMap = art1.getTypedProperty(mapOfArt.getId(), Map.class);
		var entry1 = ((RDFInstance)resultMap.get("key1"));
		assertEquals(art2, entry1);
				
		var entry3 = ((RDFInstance)resultMap.get("key3"));
		assertEquals(base3, entry3);
		
		assertNull(resultMap.get("unusedkey"));
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}
	
	@Test
	void failOnUseStringMapWithObject() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		var artMap = art1.getTypedProperty(mapOfString.getId(), MapWrapper.class);
		assertThrows(IllegalArgumentException.class, () -> artMap.put("key1", art2));
		
		Map<String, String> resultMap = art1.getTypedProperty(mapOfString.getId(), Map.class);
		assertNull(resultMap.get("unusedkey"));
	}
	
	@Test
	void testStreamEntries() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var artMap = art1.getTypedProperty(mapOfString.getId(), MapWrapper.class);
		var values = List.of("1", "2", "3");
		var keys = List.of("Key1", "Key2", "Key3");
		artMap.put(keys.get(0), values.get(0));
		artMap.put(keys.get(1), values.get(1));
		artMap.put(keys.get(2), values.get(2));				
		
		var artMap2 = art1.getTypedProperty(mapOfString.getId(), MapWrapper.class); // to recreate map
		assertTrue(artMap2.values().containsAll(values));
		assertTrue(artMap2.keySet().containsAll(keys));
		
		assertEquals(3, artMap2.size());
		assertFalse(artMap2.isEmpty());
		assertTrue(artMap2.containsKey(keys.get(0)));
		assertFalse(artMap2.containsKey("unknownkey"));
		assertTrue(artMap2.containsValue(values.get(0)));
		assertFalse(artMap2.containsValue("unknown value"));
		artMap2.entrySet().stream().allMatch(entry -> entry.getKey().length() > 2);
		
		artMap2.clear();
		var artMap3 = art1.getTypedProperty(mapOfString.getId(), MapWrapper.class); // to recreate map
		assertEquals(0, artMap3.size());
		
	}
	
	
	@Test
	void setInvalidObjectProperty() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		assertThrows(IllegalArgumentException.class, () -> art1.setSingleProperty(LIST_OF_ART, art2));	
	}
	
	@Test
	void setMultipleOnes() {		
			var art1 = resolver.createInstance(NS+"art1", typeChild);
			var rdfMap = art1.getTypedProperty(mapOfString.getId(), MapWrapper.class);
			var values = List.of("4", "5", "6");
			var keys = List.of("Key1", "Key2", "Key3");
			Map<String, String> artMap = new HashMap<>();			
			artMap.put(keys.get(0), values.get(0));
			artMap.put(keys.get(1), values.get(1));
			artMap.put(keys.get(2), values.get(2));	
			rdfMap.putAll(artMap);
			
			var artMap2 = art1.getTypedProperty(mapOfString.getId(), MapWrapper.class); // to recreate map
			assertTrue(artMap2.values().containsAll(values));
			assertTrue(artMap2.keySet().containsAll(keys));
			artMap2.put(keys.get(0), "7");
			assertNotNull(artMap2.remove(keys.get(1)));
	
			var artMap3 = art1.getTypedProperty(mapOfString.getId(), MapWrapper.class); // to recreate map
			assertEquals("7", artMap3.get(keys.get(0)));
			assertNull(artMap3.get(keys.get(1)));
			assertNull(artMap2.remove(keys.get(1)));
	}
	
	@Test
	void setMultipleOnesWithArt() {		
			var art1 = resolver.createInstance(NS+"art1", typeChild);
			var art2 = resolver.createInstance(NS+"art2", typeChild);
			var art3 = resolver.createInstance(NS+"art3", typeChild);
			var art4 = resolver.createInstance(NS+"art4", typeChild);
			var art5 = resolver.createInstance(NS+"art5", typeChild);
			var rdfMap = art1.getTypedProperty(mapOfArt.getId(), MapWrapper.class);
			var values = List.of(art2, art3, art4);
			var keys = List.of("Key1", "Key2", "Key3");
			Map<String, Object> artMap = new HashMap<>();			
			artMap.put(keys.get(0), values.get(0));
			artMap.put(keys.get(1), values.get(1));
			artMap.put(keys.get(2), values.get(2));	
			rdfMap.putAll(artMap);
			
			var artMap2 = art1.getTypedProperty(mapOfArt.getId(), MapWrapper.class); // to recreate map
			assertTrue(artMap2.values().containsAll(values));
			assertTrue(artMap2.keySet().containsAll(keys));
			artMap2.put(keys.get(0), art5);
			assertNotNull(artMap2.remove(keys.get(1)));
	
			var artMap3 = art1.getTypedProperty(mapOfArt.getId(), MapWrapper.class); // to recreate map
			assertEquals(art5, artMap3.get(keys.get(0)));
			assertNull(artMap3.get(keys.get(1)));
			assertNull(artMap3.remove(keys.get(1)));
			
			assertTrue(artMap3.containsValue(art5));
	}
	
	@Test
	void testViaDirectAccess() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		
		art1.put(mapOfArt.getId(), "key", art2);
		var objectMap = art1.getTypedProperty(mapOfArt.getId(), Map.class);
		assertTrue(objectMap.containsValue(art2));
		
		
		art1.put(mapOfString.getId(), "key", "Test");
		var stringMap = art1.getTypedProperty(mapOfString.getId(), Map.class);
		assertTrue(stringMap.containsValue("Test"));
		
	}
}

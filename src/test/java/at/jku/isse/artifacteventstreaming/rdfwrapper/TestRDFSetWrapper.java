package at.jku.isse.artifacteventstreaming.rdfwrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.apache.jena.ontapi.model.OntIndividual;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.collections.SetWrapper;

@TestInstance(Lifecycle.PER_CLASS)
class TestRDFSetWrapper extends TestRDFInstance {

	@Test 
	void testStringSet() {
		List<Object> values = List.of("A", "B", "C");
		testSet(values, setOfString.getId());
	}
	
	@Test
	void testArtSet() {
		var art2 = resolver.createInstance(NS+"artA", typeChild);
		var art3 = resolver.createInstance(NS+"artB", typeChild);
		var art4 = resolver.createInstance(NS+"artC", typeChild);
		List<Object> values = List.of(art2, art3, art4);
		testSet(values, setOfArt.getId());
	}
	
	// assumes list with at least 3 different items
	private void testSet(List<Object> values, String propUri) {
		assertTrue(values.size() > 2);
		
		RDFInstance art1 = (RDFInstance) resolver.createInstance(NS+"art1", typeBase);
		
		var set = art1.getTypedProperty(propUri, SetWrapper.class);
		printProperties(art1.getInstance(), propUri);		
		assertTrue(set.isEmpty());
		assertEquals(0, set.size());			
		set.add(values.get(0));
		set.add(values.get(1));
		set.add(values.get(2));
		
		var set2 = art1.getTypedProperty(propUri, SetWrapper.class); // to force recreate		
		assertEquals(values.size(), set2.size());
		assertFalse(set2.isEmpty());
		assertTrue(set.contains(values.get(1)));					
		assertTrue(set.remove(values.get(1))); // now of size 2
		
		var set3 = art1.getTypedProperty(propUri, SetWrapper.class); // to force recreate		
		assertEquals(values.size()-1, set2.size());
		assertTrue(values.contains(set3.iterator().next()));	
		
		assertTrue(set3.stream().allMatch(item -> values.contains(item)));
							
		assertThrows(RuntimeException.class, () -> set3.spliterator());
		assertEquals(2, set3.toArray().length);
		assertThrows(RuntimeException.class, () -> set3.removeIf(null));
		assertThrows(RuntimeException.class, () -> set3.forEach(null));
		set3.addAll(values);
		
		var set6 = art1.getTypedProperty(propUri, SetWrapper.class); // to force recreate				
		assertTrue(set6.containsAll(values));		
		assertEquals(values.size(), set6.size());							
		assertFalse(set6.addAll(Collections.emptyList()));		
		assertTrue(set6.removeAll(values));
		assertTrue(set6.isEmpty());
		
		var set7 = art1.getTypedProperty(propUri, SetWrapper.class); // to force recreate
		set7.addAll(values);		
		set7.retainAll(List.of(values.get(2)));
		
		var set8 = art1.getTypedProperty(propUri, SetWrapper.class); // to force recreate
		assertTrue(set8.stream().allMatch(el -> el.equals(values.get(2))));		
		set8.clear();
		assertTrue(set8.isEmpty());						
	}

	
	@Test
	void testViaDirectAccess() {
		var art1 = resolver.createInstance(NS+"art1", typeBase);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		
		art1.add(setOfArt.getId(), art2);
		var objectSet = art1.getTypedProperty(setOfArt.getId(), SetWrapper.class);
		assertTrue(objectSet.contains(art2));
		
		
		art1.add(setOfString.getId(), "Test");
		var stringSet = art1.getTypedProperty(setOfString.getId(), SetWrapper.class);
		assertTrue(stringSet.contains("Test"));
		
	}
	
	@Test
	void testCatchWrongType() {
		var art1 = resolver.createInstance(NS+"art1", typeBase);		
		var art2 = resolver.createInstance(NS+"art2", typeBase);
		var art3 = resolver.createInstance(NS+"art3", typeChild);
		var objectSet = art1.getTypedProperty(setOfArt.getId(), SetWrapper.class);
		
		assertThrows(IllegalArgumentException.class, () -> objectSet.add(""));		
		assertThrows(IllegalArgumentException.class, () -> objectSet.add(art2));		
		assertTrue(objectSet.add(art3));
		
		var stringSet = art1.getTypedProperty(setOfString.getId(), SetWrapper.class);
		assertThrows(IllegalArgumentException.class, () -> stringSet.add(art2));
		
	}
	
	private void printProperties(OntIndividual ind, String propURI) {
		var iter = ind.listProperties(ind.getModel().createProperty(propURI));
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}
	
}

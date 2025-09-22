package at.jku.isse.artifacteventstreaming.schemasupport;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
class TestRDFSetWrapper extends TestSchemaSetup {

	@Test 
	void testStringSet() {
		List<RDFNode> values = List.of(m.createLiteral("A"), m.createLiteral("B"), m.createLiteral("C"));
		testSet(values, setOfString.getURI());
	}
	
	@Test
	void testArtSet() {
		var art2 = typeChild.createIndividual(NS+"artA");
		var art3 = typeChild.createIndividual(NS+"artB");
		var art4 = typeChild.createIndividual(NS+"artC");
		List<RDFNode> values = List.of(art2, art3, art4);
		testSet(values, setOfArt.getURI());
	}
	
	// assumes list with at least 3 different items
	private void testSet(List<RDFNode> values, String propUri) {
		assertTrue(values.size() > 2);
		OntIndividual art1 = typeBase.createIndividual(NS+"art1");
		TypedSetWrapper set;
		if (propUri.equals(setOfArt.getURI())) {
			set = new TypedSetWrapper(art1, setOfArt, typeChild);
		} else {
			set = new TypedSetWrapper(art1, setOfString, stringRange);
		}

		// Initially empty
		assertTrue(set.isEmpty());
		assertEquals(0, set.size());

		// Add values
		set.add(values.get(0));
		set.add(values.get(1));
		set.add(values.get(2));

		assertEquals(values.size(), set.size());
		assertFalse(set.isEmpty());
		assertTrue(set.contains(values.get(1)));
		assertTrue(set.remove(values.get(1)));
		assertEquals(values.size() - 1, set.size());
		assertTrue(values.contains(set.iterator().next()));
		assertTrue(set.stream().allMatch(item -> values.contains(item)));
		assertThrows(RuntimeException.class, () -> set.spliterator());
		assertEquals(set.size(), set.toArray().length);
		assertThrows(RuntimeException.class, () -> set.removeIf(null));
		assertThrows(RuntimeException.class, () -> set.forEach(null));
		set.addAll(values);
		assertTrue(set.containsAll(values));
		assertEquals(values.size(), set.size());
		assertFalse(set.addAll(Collections.emptyList()));
		assertTrue(set.removeAll(values));
		assertTrue(set.isEmpty());
		set.addAll(values);
		set.retainAll(List.of(values.get(2)));
		assertTrue(set.stream().allMatch(el -> el.equals(values.get(2))));
		set.clear();
		assertTrue(set.isEmpty());
	}

	
	@Test
	void testViaDirectAccess() {
		var art1 = typeBase.createIndividual(NS+"art1");
		var art2 = typeChild.createIndividual(NS+"art2");
		var objectSet = new TypedSetWrapper(art1, setOfArt, typeChild);
		objectSet.add(art2);
		assertTrue(objectSet.contains(art2));

		var stringSet = new TypedSetWrapper(art1, setOfString, stringRange);
		stringSet.add(m.createLiteral("Test"));
		assertTrue(stringSet.contains(m.createLiteral("Test")));
	}

	@Test
	void testCatchWrongType() {
		var art1 = typeBase.createIndividual(NS+"art1");
		var art2 = typeBase.createIndividual(NS+"art2");
		var art3 = typeChild.createIndividual(NS+"art3");
		var objectSet = new TypedSetWrapper(art1, setOfArt, typeChild);

		assertThrows(IllegalArgumentException.class, () -> objectSet.add(m.createLiteral("")));
		assertThrows(IllegalArgumentException.class, () -> objectSet.add(art2));
		assertTrue(objectSet.add(art3));

		var stringSet = new TypedSetWrapper(art1, setOfString, stringRange);
		assertThrows(IllegalArgumentException.class, () -> stringSet.add(art2));
	}
	
	private void printProperties(OntIndividual ind, Property prop) {
		var iter = ind.listProperties(prop);
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}
	
}
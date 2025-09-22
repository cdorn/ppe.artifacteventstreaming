package at.jku.isse.artifacteventstreaming.schemasupport;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.apache.jena.rdf.model.RDFNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
class TestRDFListWrapper extends TestSchemaSetup {

	@Test 
	void testStringList() {
		List<RDFNode> values = List.of(m.createLiteral("A"), m.createLiteral("B"), m.createLiteral("C"));
		testList(values, listOfString.getURI());
	}
	
	@Test
	void testArtList() {
		var art2 = typeChild.createIndividual(NS+"artA");
		var art3 = typeChild.createIndividual(NS+"artB");
		var art4 = typeChild.createIndividual(NS+"artC");
		List<RDFNode> values = List.of(art2, art3, art4);
		testList(values, listOfArt.getURI());
	}
	
	// assumes list with at least 3 different items
	private void testList(List<RDFNode> values, String listPropUri) {
		assertTrue(values.size() > 2);
		
		var art1 = typeBase.createIndividual(NS+"art1");
		var list = listPropUri.equals(listOfArt.getURI())
			? new TypedListWrapper(art1, listOfArt, typeChild, metaSchema.getListType())
			: new TypedListWrapper(art1, listOfString, stringRange, metaSchema.getListType());
		assertTrue(list.isEmpty());
		assertEquals(0, list.size());			
		list.add(values.get(0));
		list.add(values.get(1));
		list.add(values.get(2));
		
		var list2 = listPropUri.equals(listOfArt.getURI())
			? new TypedListWrapper(art1, listOfArt, typeChild, metaSchema.getListType())
			: new TypedListWrapper(art1, listOfString, stringRange, metaSchema.getListType());
		assertEquals(values.size(), list2.size());
		assertFalse(list2.isEmpty());
		assertTrue(list2.contains(values.get(1)));
		assertEquals(values.get(2), list2.get(2));				
		assertEquals(values.get(1), list2.remove(1)); // now of size 2
		
		var list3 = listPropUri.equals(listOfArt.getURI())
			? new TypedListWrapper(art1, listOfArt, typeChild, metaSchema.getListType())
			: new TypedListWrapper(art1, listOfString, stringRange, metaSchema.getListType());
		assertEquals(1, list3.indexOf(values.get(2))); // former third item is now on pos 2
		assertEquals(values.size()-1, list3.size());
		assertTrue(list3.stream().allMatch(item -> values.contains(item)));
		assertEquals(values.get(0), list3.iterator().next());		
		assertThrows(RuntimeException.class, () -> list3.lastIndexOf(values.get(0)));
		assertThrows(RuntimeException.class, () -> list3.listIterator());
		assertThrows(RuntimeException.class, () -> list3.listIterator(0));
		assertEquals(2, list3.toArray().length);
		list3.addAll(values);
		list3.addAll(0, values);
						
		var list4 = listPropUri.equals(listOfArt.getURI())
			? new TypedListWrapper(art1, listOfArt, typeChild, metaSchema.getListType())
			: new TypedListWrapper(art1, listOfString, stringRange, metaSchema.getListType());
		assertTrue(list4.containsAll(values));
		assertEquals(values, list4.subList(0, values.size()));
		assertEquals(values.size()-1+values.size()+values.size(), list4.size());
		
		assertEquals(values.get(0), list4.set(0, values.get(2))); // override pos 0 with 3rd item
		list4.add(1, values.get(2)); // add pos1 with 3rd item
		
		var list5 = listPropUri.equals(listOfArt.getURI())
			? new TypedListWrapper(art1, listOfArt, typeChild, metaSchema.getListType())
			: new TypedListWrapper(art1, listOfString, stringRange, metaSchema.getListType());
		assertEquals(list5.get(0), list5.get(1));
		
		list5.stream().forEach(el -> System.out.println(el));
		
		assertTrue(list5.remove(values.get(0))); // somewhere down at pos 4
		assertNotNull(list5.remove(6));
		assertNotNull(list5.remove(5));
		assertNotNull(list5.remove(4));
		System.out.println("..");
		list3.stream().forEach(el -> System.out.println(el));
		
		assertFalse(list5.contains(values.get(0)));
		
		var list6 = listPropUri.equals(listOfArt.getURI())
			? new TypedListWrapper(art1, listOfArt, typeChild, metaSchema.getListType())
			: new TypedListWrapper(art1, listOfString, stringRange, metaSchema.getListType());
		assertEquals(values.size()-4+values.size()+values.size(), list6.size());
		assertTrue(list6.removeAll(values));
		assertEquals(values.size()-3+values.size(), list6.size());
		
		list6.retainAll(List.of(values.get(2)));
		assertFalse(list6.addAll(1, Collections.emptyList()));
		
		var list7 = listPropUri.equals(listOfArt.getURI())
			? new TypedListWrapper(art1, listOfArt, typeChild, metaSchema.getListType())
			: new TypedListWrapper(art1, listOfString, stringRange, metaSchema.getListType());
		assertTrue(list7.stream().allMatch(el -> el.equals(values.get(2))));		
		list7.clear();
		assertTrue(list7.isEmpty());						
	}
	
	@Test
	void testCatchWrongType() {
		var art1 = typeBase.createIndividual(NS+"art1");
		var art2 = typeBase.createIndividual(NS+"art2");
		var art3 = typeChild.createIndividual(NS+"art3");
		var objectList = new TypedListWrapper(art1, listOfArt, typeChild, metaSchema.getListType());
		
		assertThrows(IllegalArgumentException.class, () -> objectList.add(m.createLiteral("")));
		assertThrows(IllegalArgumentException.class, () -> objectList.add(0, m.createLiteral("")));
		assertThrows(IllegalArgumentException.class, () -> objectList.add(art2));
		assertThrows(IllegalArgumentException.class, () -> objectList.add(0, art2));
		assertTrue(objectList.add(art3));
		
		var stringList = new TypedListWrapper(art1, listOfString, stringRange, metaSchema.getListType());
		assertThrows(IllegalArgumentException.class, () -> stringList.add(art2));
		assertThrows(IllegalArgumentException.class, () -> stringList.add(0, art2));
	}
	
}
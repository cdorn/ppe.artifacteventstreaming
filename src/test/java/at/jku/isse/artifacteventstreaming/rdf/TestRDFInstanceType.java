package at.jku.isse.artifacteventstreaming.rdf;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.util.List;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.PPEPropertyType;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;

class TestRDFInstanceType {

	public static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static NodeToDomainResolver resolver;
	RDFInstanceType typeBase;
	RDFInstanceType typeChild;
	
	@BeforeEach
	void setup() {
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		resolver = new NodeToDomainResolver(m);
		resolver.getMapEntryBaseType();
		resolver.getListBaseType();
		typeBase = resolver.createNewInstanceType(NS+"artifact");
		typeChild = resolver.createNewInstanceType(NS+"issue", typeBase);
	}

	@Test
	void testGetSuperClass() {
		System.out.println(typeChild.getParentType().getName());
		assertEquals(typeChild.getParentType(), typeBase);
		assertEquals(typeChild.getInstanceType(), BuildInType.METATYPE);
		assertEquals(null, typeBase.getParentType());
	}
	
	@Test
	void testGetSuperClassInPresenceOfRestrictions() {
		typeBase.createSinglePropertyType("priority", BuildInType.INTEGER);
		typeChild.createListPropertyType("listOfArt", typeBase);
		
		List<String> names = typeChild.getPropertyNamesIncludingSuperClasses();
		assertTrue(names.contains("priority"));
		assertTrue(names.contains("listOfArt"));
		
		PPEPropertyType listProp = typeChild.getPropertyType(typeChild.makePropertyURI("listOfArt"));
		assertNotEquals(null, listProp);
		assertEquals(typeBase, listProp.getInstanceType());
		assertTrue(typeChild.hasPropertyType(typeBase.makePropertyURI("priority")));
		PPEPropertyType nonExistantProp = typeChild.getPropertyType(typeBase.makePropertyURI("sdfdsfdsf"));
		assertNull(nonExistantProp);
		
		assertEquals(typeChild.getInstanceType(), BuildInType.METATYPE);
		assertEquals(null, typeBase.getParentType());
		
		
		var prop = resolver.getMapType().addObjectMapProperty(typeBase.getType(), NS+"hasMap", typeChild.getType());
		assertNotEquals(null, prop);
		var propType = typeBase.getPropertyType(NS+"hasMap");
		assertEquals(typeChild, propType.getInstanceType());
	}
	
	@Test
	void testFindType() {
		assertEquals(typeBase, resolver.findAllInstanceTypesByFQN(typeBase.getId()).iterator().next());
		assertEquals(typeChild, resolver.findAllInstanceTypesByFQN(typeChild.getId()).iterator().next());
		assertEquals(2, resolver.getAllNonDeletedInstanceTypes().size());
	}
	
	@Test
	void testRemoveType() {
		assertEquals(typeBase, resolver.findAllInstanceTypesByFQN(typeBase.getId()).iterator().next());
		typeBase.markAsDeleted();
		assertTrue(resolver.findAllInstanceTypesByFQN(typeBase.getId()).isEmpty());
		assertEquals(0, resolver.getAllNonDeletedInstanceTypes().size());
	}
	
	@Test
	void testTypeCheck() {
		assertTrue(typeChild.isOfTypeOrAnySubtype(typeBase));
		assertTrue(typeChild.isOfTypeOrAnySubtype(typeChild));
		assertEquals(1, typeBase.getAllSubtypesRecursively().size());
	}
}

package at.jku.isse.artifacteventstreaming.rdfwrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
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
	void setup() throws URISyntaxException, Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);			
		BranchImpl branch = (BranchImpl) new BranchBuilder(new URI(NS+"repo"), repoDataset, repoModel )	
				.setBranchLocalName("branch1")
				.build();		
		m = branch.getModel();
		var cardUtil = new PropertyCardinalityTypes(m); // this adds list , mapentry and metaclass type
		resolver = new NodeToDomainResolver(branch, null, cardUtil);
		resolver.getMapEntryBaseType();
		resolver.getListBaseType();
		typeBase = resolver.createNewInstanceType(NS+"artifact");
		typeChild = resolver.createNewInstanceType(NS+"issue", typeBase);
	}

	@Test
	void testGetSuperClass() {
		System.out.println(typeChild.getParentType().getName());
		assertEquals(typeChild.getParentType(), typeBase);
		//assertEquals(typeChild.getInstanceType(), BuildInType.METATYPE);
		assertEquals(null, typeBase.getParentType());
	}
	
	@Test
	void testGetSuperClassInPresenceOfRestrictions() {
		typeBase.createSinglePropertyType("priority", BuildInType.INTEGER);
		typeChild.createListPropertyType("listOfArt", typeBase);
		
		List<String> names = typeChild.getPropertyNamesIncludingSuperClasses();
		assertTrue(names.contains("priority"));
		assertTrue(names.contains("listOfArt"));
		
		PPEPropertyType listProp = typeChild.getPropertyType("listOfArt");
		assertNotEquals(null, listProp);
		assertEquals(typeBase, listProp.getInstanceType());
		assertTrue(typeChild.hasPropertyType("priority"));
		PPEPropertyType nonExistantProp = typeChild.getPropertyType("sdfdsfdsf");
		assertNull(nonExistantProp);
		
		//assertEquals(resolver.resolveToType(null)e.METATYPE, typeChild.getInstanceType());
		assertEquals(null, typeBase.getParentType());
		
		
		var prop = resolver.getCardinalityUtil().getMapType().addObjectMapProperty(typeBase.getType(), NS+"hasMap", typeChild.getType());
		assertNotEquals(null, prop);
		var propType = typeBase.getPropertyType(NS+"hasMap");
		assertEquals(typeChild, propType.getInstanceType());
	}
	
	@Test
	void testFindType() {
		assertEquals(typeBase, resolver.findAllInstanceTypesByFQN(typeBase.getId()).iterator().next());
		assertEquals(typeChild, resolver.findAllInstanceTypesByFQN(typeChild.getId()).iterator().next());
		var types = resolver.getAllNonDeletedInstanceTypes();
		types.stream().forEach(type -> System.out.println(type.getId()));
		/*
		  http://isse.jku.at/artifactstreaming/rdfwrapper#MetaClass
			http://isse.jku.at/artifactstreaming/rdfwrapper#propertyMetadataEntryType
			http://at.jku.isse.test#issue
			http://at.jku.isse.test#artifact
			http://at.jku.isse.map#EntryType
			http://www.w3.org/1999/02/22-rdf-syntax-ns#Seq

		 * */
		assertEquals(6, types.size());
	}
	
	@Test
	void testRemoveType() {
		assertEquals(typeBase, resolver.findAllInstanceTypesByFQN(typeBase.getId()).iterator().next());
		typeBase.markAsDeleted();
		assertTrue(resolver.findAllInstanceTypesByFQN(typeBase.getId()).isEmpty());
		assertEquals(4, resolver.getAllNonDeletedInstanceTypes().size());
	}
	
	@Test
	void testTypeCheck() {
		assertTrue(typeChild.isOfTypeOrAnySubtype(typeBase));
		assertTrue(typeChild.isOfTypeOrAnySubtype(typeChild));
		assertEquals(1, typeBase.getAllSubtypesRecursively().size());
	}
}

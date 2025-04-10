package at.jku.isse.artifacteventstreaming.rdfwrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
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
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.PrimitiveTypesFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFPropertyType;

class TestRDFInstanceType {

	public static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static NodeToDomainResolver resolver;
	RDFInstanceType typeBase;
	RDFInstanceType typeChild;
	PrimitiveTypesFactory typeFactory;
	
	@BeforeEach
	void setup() throws URISyntaxException, Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);			
		BranchImpl branch = (BranchImpl) new BranchBuilder(new URI(NS+"repo"), repoDataset, repoModel )	
				.setBranchLocalName("branch1")
				.build();		
		m = branch.getModel();
		typeFactory = new PrimitiveTypesFactory(m);
		var metaModel = MetaModelOntology.buildInMemoryOntology(); 
		new RuleSchemaFactory(metaModel); // add rule schema to meta model		
		var cardUtil = new MetaModelSchemaTypes(m, metaModel); // this adds list , mapentry and metaclass type
		resolver = new NodeToDomainResolver(branch, null, cardUtil);
		resolver.getMapEntryBaseType();
		resolver.getListBaseType();
		typeBase = resolver.createNewInstanceType(NS+"artifact");
		var succ = typeBase.createSinglePropertyType("priority", typeFactory.getIntType());
		assertNotNull(succ);		
		typeChild = resolver.createNewInstanceType(NS+"issue", typeBase);		
		succ = typeChild.createListPropertyType("listOfArt", typeBase.getAsPropertyType());
		assertNotNull(succ);
		
		var prop = typeBase.createMapPropertyType(NS+"hasMap", typeChild.getAsPropertyType());
		//var prop = resolver.getCardinalityUtil().getMapType().addObjectMapProperty(typeBase.getType(), NS+"hasMap", typeChild.getType());
		assertNotEquals(null, prop);
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
				
		List<String> names = typeChild.getPropertyNamesIncludingSuperClasses();
		assertTrue(names.contains("priority"));
		assertTrue(names.contains("listOfArt"));
		
		RDFPropertyType listProp = typeChild.getPropertyType("listOfArt");
		assertNotEquals(null, listProp);
		assertEquals(typeBase, listProp.getValueType());
		assertTrue(typeChild.hasPropertyType("priority"));
		RDFPropertyType nonExistantProp = typeChild.getPropertyType("sdfdsfdsf");
		assertNull(nonExistantProp);
		
		//assertEquals(resolver.resolveToType(null)e.METATYPE, typeChild.getInstanceType());
		assertEquals(null, typeBase.getParentType());
		
		var propType = typeBase.getPropertyType(NS+"hasMap");
		assertEquals(typeChild.getAsPropertyType(), propType.getValueType());
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
		 * */
		assertEquals(4, types.size());
	}
	
	@Test
	void testRemoveType() {
		assertEquals(typeBase, resolver.findAllInstanceTypesByFQN(typeBase.getId()).iterator().next());
		typeBase.delete();
		assertTrue(resolver.findAllInstanceTypesByFQN(typeBase.getId()).isEmpty());
		assertEquals(2, resolver.getAllNonDeletedInstanceTypes().size());
	}
	
	@Test
	void testTypeCheck() {
		assertTrue(typeChild.isOfTypeOrAnySubtype(typeBase));
		assertTrue(typeChild.isOfTypeOrAnySubtype(typeChild));
		assertEquals(1, typeBase.getAllSubtypesRecursively().size());
	}
}

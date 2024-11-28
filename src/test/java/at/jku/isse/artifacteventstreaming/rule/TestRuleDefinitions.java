package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.SchemaUtils;

class TestRuleDefinitions {
	
	private static final String LOCALPROP_REF = "ref";
	private static final String LOCALPROP_SUBREF = "subref";

	public static URI baseURI = URI.create("http://at.jku.isse.artifacteventstreaming/test/rules#");
	
	public static RuleSchemaFactory ruleSchemaFactory = new RuleSchemaFactory();
	
	OntModel m;
	OntClass artType;
	OntClass artSubType;
	RuleSchemaProvider factory;
	OntObjectProperty refProp;
	OntObjectProperty subProp;
	OntDataProperty priorityProp;
	OntDataProperty labelProp;
	RDFModelAccess modelAccess;
	
	@BeforeEach
	void setup() {
		//THIS OntSpec is important: as without inference, then subproperties and subclass individuals are not correctly inferred
		// but when using RDFS inference only, then we dont get individual change events due to some deep down graph listener registration problem 
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_RDFS_INF ); // needs deep setting of graph listener
		//m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF ); 		
		m.setNsPrefix("rules", RuleSchemaFactory.uri);
		m.setNsPrefix("test", baseURI.toString());
		artType = m.createOntClass(baseURI+"artType");
		artSubType = m.createOntClass(baseURI+"artSubType");
		artSubType.addSuperClass(artType);
		refProp =m.createObjectProperty(baseURI+LOCALPROP_REF);
		refProp.addRange(artType);
		refProp.addDomain(artType);
		
		subProp = m.createObjectProperty(baseURI+LOCALPROP_SUBREF);
		subProp.addRange(artSubType);
		subProp.addDomain(artSubType);
		
		labelProp = SchemaUtils.createSingleDataPropertyType(baseURI+"label", artType, m.getDatatype(XSD.xstring));
		priorityProp = SchemaUtils.createSingleDataPropertyType(baseURI+"label", artType, m.getDatatype(XSD.xint)); 
		
		factory = new RuleSchemaProvider(m, ruleSchemaFactory);
		modelAccess = new RDFModelAccess(m);
	}
	
	@Test
	void testDefineSimpleRule() throws RuleException {
		
		var ruleDef = factory.createRuleDefinitionBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.isDefined() = true")
				.build();
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}
	
	
	@Test
	void testTypeRule() throws RuleException {
		
		var ruleDef = factory.createRuleDefinitionBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.asType(<"+ artSubType.getURI()+">).subref.size() > 0")
				.build();
		System.out.println(ruleDef.getExpressionError());
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
	}
	
	@Test
	void testTypeLocalNameRule() throws RuleException {
		
		var ruleDef = factory.createRuleDefinitionBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.asType(<"+ artSubType.getLocalName()+">).subref.size() > 0")
				.build();
		System.out.println(ruleDef.getExpressionError());
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
	}
	
	@Test
	void testMissingCastRule() throws RuleException {
		
		var ruleDef = getNonRegisteredBasicRuleDefinition();
		System.out.println(ruleDef.getExpressionError());
		assertTrue(ruleDef.hasExpressionError());
		assertNull(ruleDef.getSyntaxTree());
	}
	
	@Test
	void testUpdateToInvalidRule() throws RuleException {
		var ruleDef = factory.createRuleDefinitionBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.isDefined() = true")
				.build();
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
		ruleDef.setRuleExpression("self.sxx = x");
		assertTrue(ruleDef.hasExpressionError());
		assertNull(ruleDef.getSyntaxTree());
		
		ruleDef.setRuleExpression("self.ref.size() > 0");
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		
	}
	
	@Test
	void testRuleFinding() throws RuleException {
		var repo = new RuleRepository(factory);
		
		var def = getNonRegisteredBasicRuleDefinition();
		String uri = def.getRuleDefinition().getURI();
		assertNull(repo.findRuleDefinitionForResource(def.getRuleDefinition()));
		
		repo.registerRuleDefinition(def);		
		assertEquals(def, repo.findRuleDefinitionForResource(def.getRuleDefinition()));
		
		def.delete();
		var affected = repo.removeRulesAffectedByDeletedRuleDefinition(uri);
		assertEquals(0, affected.size());
		
		var def2 = getNonRegisteredBasicRuleDefinition();		
		assertNull(repo.findRuleDefinitionForResource(def2.getRuleDefinition()));
		
		repo.storeRuleDefinition(def2.getRuleDefinition().as(OntIndividual.class));
		assertNotEquals(def2, repo.findRuleDefinitionForResource(def2.getRuleDefinition())); // because we created a copy here
	}
	
	
	
	private RDFRuleDefinition getNonRegisteredBasicRuleDefinition() throws RuleException {
		return factory.createRuleDefinitionBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.subref.size() > 0")
				.build();
	}

}

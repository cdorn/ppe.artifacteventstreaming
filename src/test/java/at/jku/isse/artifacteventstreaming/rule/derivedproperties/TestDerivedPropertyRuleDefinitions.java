package at.jku.isse.artifacteventstreaming.rule.derivedproperties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.rule.RDFModelAccess;
import at.jku.isse.artifacteventstreaming.rule.RuleException;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.definition.DerivedPropertyRuleDefinition;
import at.jku.isse.artifacteventstreaming.rule.definition.RDFRuleDefinition;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;

class TestDerivedPropertyRuleDefinitions {
	
	private static final String LOCALPROP_REF = "ref";
	private static final String LOCALPROP_SUBREF = "subref";

	public static URI baseURI = URI.create("http://at.jku.isse.artifacteventstreaming/test/derivedpropertyrules#");
	
	
	OntModel m;
	OntClass artType;
	OntClass artSubType;
	RuleSchemaProvider factory;
	OntObjectProperty refProp;
	OntObjectProperty subProp;
	OntObjectProperty refPropDerived;
	OntObjectProperty subPropDerived;
	OntDataProperty numbersProp;
	OntDataProperty numbersDerivedProp;
	OntObjectProperty numbersListProp;
	OntObjectProperty numbersListDerivedProp;
	
	OntDataProperty priorityProp;
	OntDataProperty labelProp;
	OntDataProperty priorityDerivedProp;
	OntDataProperty labelDerivedProp;
	RDFModelAccess modelAccess;
	MetaModelSchemaTypes schemaUtils;
	
	@BeforeEach
	void setup() {
		//THIS OntSpec is important: as without inference, then subproperties and subclass individuals are not correctly inferred
		//m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_RDFS_INF ); // builtin is much faster that this spec!!!!
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF );
		var metaModel = MetaModelOntology.buildInMemoryOntology(); 
		new RuleSchemaFactory(metaModel); // add rule schema to meta model		
		schemaUtils = new MetaModelSchemaTypes(m, metaModel);		 	
		m.setNsPrefix("rules", RuleSchemaFactory.uri);
		m.setNsPrefix("test", baseURI.toString());
		artType = m.createOntClass(baseURI+"artType");
		artSubType = m.createOntClass(baseURI+"artSubType");
		artSubType.addSuperClass(artType);
		refProp =m.createObjectProperty(baseURI+LOCALPROP_REF);
		refProp.addRange(artType);
		refProp.addDomain(artType);
		
		numbersProp =m.createDataProperty(baseURI+"numbers");
		numbersProp.addRange(m.getDatatype(XSD.xint));
		numbersProp.addDomain(artType);
		numbersListProp = schemaUtils.getListType().addLiteralListProperty(artType, baseURI+"numbersList", m.getDatatype(XSD.xint));
		
		
		numbersDerivedProp =m.createDataProperty(baseURI+"numbersDerived");
		numbersDerivedProp.addRange(m.getDatatype(XSD.xint));
		numbersDerivedProp.addDomain(artType);
		
		numbersListDerivedProp = schemaUtils.getListType().addLiteralListProperty(artType, baseURI+"numbersListDerived", m.getDatatype(XSD.xint));
		
		subProp = m.createObjectProperty(baseURI+LOCALPROP_SUBREF);
		subProp.addRange(artSubType);
		subProp.addDomain(artSubType);
		
		refPropDerived =m.createObjectProperty(baseURI+LOCALPROP_REF+"Derived");
		refPropDerived.addRange(artType);
		refPropDerived.addDomain(artType);
		
		subPropDerived = m.createObjectProperty(baseURI+LOCALPROP_SUBREF+"Derived");
		subPropDerived.addRange(artSubType);
		subPropDerived.addDomain(artSubType);
		
		labelProp = schemaUtils.getSingleType().createSingleDataPropertyType(baseURI+"title", artType, m.getDatatype(XSD.xstring));
		priorityProp = schemaUtils.getSingleType().createSingleDataPropertyType(baseURI+"priority", artType, m.getDatatype(XSD.xlong)); 		
			
		labelDerivedProp = schemaUtils.getSingleType().createSingleDataPropertyType(baseURI+"titleShort", artType, m.getDatatype(XSD.xstring));
		priorityDerivedProp = schemaUtils.getSingleType().createSingleDataPropertyType(baseURI+"priorityPercent", artType, m.getDatatype(XSD.xlong)); 		
		
		
		
		factory = new RuleSchemaProvider(m, schemaUtils);
		modelAccess = new RDFModelAccess(m, schemaUtils);
	}
	
	@Test
	void testDefineSimpleRule() throws RuleException {
		
		var ruleDef = factory.createRuleDefinitionBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.title.substring(1,2)")
				.forDerivedProperty(labelDerivedProp)
				.build();
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		assertTrue(ruleDef instanceof DerivedPropertyRuleDefinition);
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}
	
	@Test
	void testIncompatibleDerivedRule() throws RuleException {
		var ruleDef = factory.createRuleDefinitionBuilder()
				.withContextType(artType)
				.withDescription("TestRule")
				.withRuleTitle("TestRuleTitle")
				.withRuleExpression("self.title.substring(1,2)")
				.forDerivedProperty(priorityDerivedProp)
				.build();
		assertTrue(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		assertTrue(ruleDef instanceof DerivedPropertyRuleDefinition);
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}
	
	@Test
	void testIncompatibleSetDerive() throws RuleException {
			var ruleDef = factory.createRuleDefinitionBuilder()
					.withContextType(artSubType)
					.withDescription("Test3Rule")
					.withRuleTitle("Test3RuleTitle")
					.withRuleExpression("self.ref")
					.forDerivedProperty(subPropDerived)
					.build();
		assertTrue(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		assertTrue(ruleDef instanceof DerivedPropertyRuleDefinition);
	}
	
	@Test
	void testCompatibleSetDerive() throws RuleException {
			var ruleDef = factory.createRuleDefinitionBuilder()
					.withContextType(artSubType)
					.withDescription("Test3Rule")
					.withRuleTitle("Test3RuleTitle")
					.withRuleExpression("self.ref")
					.forDerivedProperty(refPropDerived)
					.build();
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		assertTrue(ruleDef instanceof DerivedPropertyRuleDefinition);
	}
}

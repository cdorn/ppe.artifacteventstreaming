package at.jku.isse.artifacteventstreaming.rule;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestRuleRepository {
	
	public static URI baseURI = URI.create("http://at.jku.isse.artifacteventstreaming/test/rules#");
	
	OntModel m;
	OntClass artType;
	RuleFactory factory;
	OntObjectProperty refProp;
	RDFModelAccess modelAccess;
	
	@BeforeEach
	void setup() {
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		m.setNsPrefix("rules", RuleFactory.uri);
		artType = m.createOntClass(baseURI+"artType");
		refProp =m.createObjectProperty(baseURI+"ref");
		refProp.addRange(artType);
		refProp.addDomain(artType);
		factory = new RuleFactory(m);
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

}

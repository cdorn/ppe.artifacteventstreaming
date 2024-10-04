package at.jku.isse.artifacteventstreaming.jena;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntIndividual.Named;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.Test;

class OntologyModelTest {

	public static String NS = "http://at.jku.isse.jena#";
	
	@Test
	void test() {
		OntModel m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		OntClass artifactType = m.createOntClass(NS+"artifact");		
		OntClass otherType = m.createOntClass(NS+"other");
		otherType.addDisjointClass(artifactType);
		
		OntObjectProperty.Named successorProp = m.createObjectProperty(NS+"successor");
		successorProp.addDomain(artifactType);
		successorProp.addRange(artifactType);
		successorProp.addLabel("has successor");
		
		OntDataProperty keyProp = m.createDataProperty(NS+"key");
		keyProp.addDomain(artifactType);
		keyProp.addRange(m.getDatatype(XSD.xstring));
		
		// lets create some instances:
		OntIndividual art1 = artifactType.createIndividual(NS+"art1");
		OntIndividual art2 = artifactType.createIndividual(NS+"art2");
		
		art1.addProperty(keyProp, "TESTKEY-1");
		art2.addProperty(keyProp, "TESTKEY-2");
		
		Named other1 = otherType.createIndividual(NS+"other1");
		art1.addProperty(successorProp, art2);
		art1.addProperty(successorProp, other1);
					
		
		
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		Utils.printStream(artifactType.declaredProperties());
		
		Reasoner reasoner = ReasonerRegistry.getOWLMiniReasoner();
		reasoner.bindSchema(m);
		InfModel infmodel = ModelFactory.createInfModel(reasoner, m);
		Utils.printValidation(infmodel);
	}

}

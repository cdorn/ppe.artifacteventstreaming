package at.jku.isse.artifacteventstreaming.jena;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.Test;

class OntologyModelTest {

	public static String NS = "http://at.jku.isse.jena#";
	
	@Test
	void test() {
		OntModel m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		OntClass artifact = m.createOntClass(NS+"artifact");		
		OntObjectProperty successorProp = m.createObjectProperty(NS+"successor");
		successorProp.addDomain(artifact);
		successorProp.addRange(artifact);
		successorProp.addLabel("has successor");
		
		OntDataProperty keyProp = m.createDataProperty(NS+"key");
		keyProp.addDomain(artifact);
		keyProp.addRange(m.getDatatype(XSD.xstring));
		
		// lets create some instances:
		OntIndividual art1 = artifact.createIndividual(NS+"art1");
		OntIndividual art2 = artifact.createIndividual(NS+"art2");
		
		art1.addProperty(keyProp, "TESTKEY-1");
		art2.addProperty(keyProp, "TESTKEY-2");
		
		//art1.addProperty(successorProp, art2);
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}

}

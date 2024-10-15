package at.jku.isse.artifacteventstreaming.jena;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass.Named;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.Test;

public class RDFSModelTest {

	public static String NS = "http://at.jku.isse.jena#";
		
	@Test
	void testRDFSbasedModel() {
		Model model = ModelFactory.createDefaultModel();
		model.setNsPrefix("owl", OWL.NS);
		InfModel infmodel = ModelFactory.createRDFSModel(model);
		
		//model.register()		
		Resource owlNS = model.createResource("owl:Class");
		
		Resource artType = model.createResource(NS+"artifact");
		artType.addProperty(RDF.type, owlNS);
		Resource otherType = model.createResource(NS+"other");
		otherType.addProperty(RDF.type, owlNS);
		
		Property successorProp = model.createProperty(NS, "successor");
		model.add(successorProp, RDFS.range, artType);
		model.add(successorProp, RDFS.domain, artType);
		
		Property parentProp = model.createProperty(NS, "parent");
		model.add(parentProp, RDFS.range, artType);
		model.add(parentProp, RDFS.domain, otherType);
		
		// now for some instances
		
		Resource art1 = model.createResource(NS+"art1");
		art1.addProperty(RDF.type, artType);
		// duplicate statements are ignored:
		art1.addProperty(RDF.type, artType);
		
		Resource art2 = model.createResource(NS+"art2");
		art2.addProperty(RDF.type, artType);
		
		Resource art3 = model.createResource(NS+"art3");
		art3.addProperty(RDF.type, artType);
		
		Resource art4 = model.createResource(NS+"art4");
		art4.addProperty(RDF.type, artType);
		
		// lets make some links
		art1.addProperty(successorProp, art2);		
		Literal lit = model.createLiteral("NoSuchResource");
		art1.addProperty(successorProp, lit);			
		Resource other1 = model.createResource(NS+"other1"); 
		other1.addProperty(RDF.type, otherType);
		art1.addProperty(successorProp, other1);
		
		// and print graph:
		RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
		Utils.printValidation(infmodel);
		
		Utils.printStatements(infmodel, other1, RDF.type, null);
		
		System.out.println("---------------------------");
		
		//change properties
		model.remove(art1, successorProp, lit);
		art1.addProperty(successorProp, art3);
		art1.addProperty(successorProp, art4);
		art1.addProperty(successorProp, art4);
		// and print graph:
		RDFDataMgr.write(System.out, model, Lang.RDFXML) ;
		Utils.printValidation(infmodel);
		
		OntModel ont = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM);
		ont.add(model);				
		Named ontOther = ont.createOntClass(NS+"other");
		OntDataProperty keyProp = ont.createDataProperty(NS+"key");
		keyProp.addDomain(ontOther);
		keyProp.addRange(ont.getDatatype(XSD.xstring));
		
		Named ontArt = ont.getOntClass(NS+"artifact");
		
		RDFDataMgr.write(System.out, ont, Lang.TURTLE) ;
		
		Utils.printStream(ontOther.declaredProperties());
		//printIterator(ontOther.listProperties(successorProp));
		
	}
	
	
}

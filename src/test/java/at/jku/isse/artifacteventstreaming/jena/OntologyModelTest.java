package at.jku.isse.artifacteventstreaming.jena;

import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntClass.DataMaxCardinality;
import org.apache.jena.ontapi.model.OntClass.ObjectMaxCardinality;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntIndividual.Named;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.rdf.model.Bag;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.ReasonerRegistry;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.schemasupport.ListResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType;
import at.jku.isse.passiveprocessengine.rdf.trialcode.ChangeListener;

class OntologyModelTest {

	public static String NS = "http://at.jku.isse.jena#";
	
	@Test
	void testSimpleModel() {
		OntModel m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		m.register(new ChangeListener());
		Reasoner reasoner = ReasonerRegistry.getOWLReasoner();
		reasoner.bindSchema(m);
		InfModel infmodel = ModelFactory.createInfModel(reasoner, m);
		SingleResourceType singleType = new SingleResourceType(m);
		MapResourceType mapType = new MapResourceType(m, singleType);
		
		OntClass artifactType = m.createOntClass(NS+"artifact");		
		OntClass otherType = m.createOntClass(NS+"other");
		otherType.addDisjointClass(artifactType);
		org.apache.jena.ontapi.model.OntObjectProperty.Named successorProp = m.createObjectProperty(NS+"successor");
		successorProp.addDomain(artifactType);
		successorProp.addRange(artifactType);
		successorProp.addLabel("has successor");
		
		
		ObjectMaxCardinality maxSuccessor = m.createObjectMaxCardinality(successorProp, Integer.MAX_VALUE,  mapType.getMapEntryClass());
		artifactType.addSuperClass(maxSuccessor);
		
		OntDataProperty hintProp = m.createDataProperty(NS+"collectionType");
		hintProp.addRange(m.getDatatype(XSD.xstring));
		
		OntDataProperty keyProp = m.createDataProperty(NS+"artKey");
		keyProp.addDomain(artifactType);
		keyProp.addRange(m.getDatatype(XSD.xstring));
		DataMaxCardinality maxOneKey = m.createDataMaxCardinality(keyProp, 1, null);
		maxOneKey.addProperty(hintProp, "SINGLE");
		artifactType.addSuperClass(maxOneKey);
			
		// lets create some instances:

		OntIndividual art1 = artifactType.createIndividual(NS+"art1");
		OntIndividual art2 = artifactType.createIndividual(NS+"art2");
		OntIndividual art3 = artifactType.createIndividual(NS+"art3");
		OntIndividual art4 = artifactType.createIndividual(NS+"art4");
		OntIndividual art5 = artifactType.createIndividual(NS+"art5");
		
		Literal key1 = m.createTypedLiteral("TESTKEY-1");
		art1.addProperty(keyProp, key1);
		Literal key2 = m.createTypedLiteral("TESTKEY-2");
		art2.addProperty(keyProp, key2);
		
		Named other1 = otherType.createIndividual(NS+"other1");
		Named other2 = otherType.createIndividual(NS+"other2");
		art1.addProperty(successorProp, art2);
		
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		Utils.printStream(artifactType.declaredProperties());

		// lets add a reference/link property
		art1.addProperty(successorProp, art2);
		art1.addProperty(successorProp, "NonExistingArt"); //this is also possible but 
		StmtIterator iter = art1.listProperties(successorProp);
		while (iter.hasNext()) {
		    System.out.println("    " + iter.nextStatement()
		                                    .getObject()
		                                    .toString());
		}
		Utils.printValidation(infmodel);
		
		// clear all successorProp references:
		art1.remove(successorProp, null);
		
		art1.addProperty(successorProp, other1);
		//art1.addProperty(successorProp, other2);
		art1.addProperty(successorProp, art4);
		art1.addProperty(successorProp, art5);
	//	art4.addDifferentFromStatement(art5); has no effect on consistency checking of max cardinality
		Bag successors = m.createBag(); // art1.getURI()+"#successor"
		successors.add(art3).add(art4).add(art5);
		art1.addProperty(successorProp, successors);
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		// and remove from bag again
//		NodeIterator nIter = successors.iterator();
//		while (nIter.hasNext()) {
//			Resource node = nIter.next().asResource();
//			if (node.getURI().equals(art3.getURI())) {
//				nIter.remove();
//				break;
//			}
//		}
//		nIter.close();
		// if we know exactly the index, then we can remove directly via creation of a statement:
		successors.remove(successors.getModel().createStatement(successors, RDF.li(1), art3));
		
		// for removing the complete collection/contains/bag, but without removing the elements from the bag
		//art1.remove(successorProp, successors);
		System.out.println(" //////////// after remove /////////////");
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		
		
		Utils.printValidation(infmodel);
		
		Stream<OntClass.Restriction> restrictions = successorProp.referringRestrictions();
		restrictions
			.filter(ObjectMaxCardinality.class::isInstance)
			.map(ObjectMaxCardinality.class::cast)
			.forEach(rest -> System.out.println(rest.getCardinality()));
	}

	
	@Test
	void testSeq() {
		OntModel m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		m.register(new ChangeListener());
		var singleType = new SingleResourceType(m);
		var listFactory = new ListResourceType(m, singleType);
		
		OntClass artifactType = m.createOntClass(NS+"artifact");
		OntIndividual art1 = artifactType.createIndividual(NS+"art1");
		var successorProp = listFactory.addObjectListProperty(artifactType, NS+"hasSuccessor", artifactType);
		var seq = listFactory.getOrCreateSequenceFor(art1, successorProp);
		
		OntIndividual art2 = artifactType.createIndividual(NS+"art2");
		//Seq seq = m.createSeq(NS+"seq");											
		assertTrue(listFactory.isListCollection(seq.as(OntIndividual.class)));
		
		seq.add(art2);		
		art1.addProperty(successorProp.asProperty(), seq);
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;		
	}
	
	@Test
	void testPropertyName() {
		OntModel m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		OntClass artifactType = m.createOntClass(NS+"artifact");		
		var singleType = new SingleResourceType(m);
		var listFactory = new ListResourceType(m, singleType);
		var successorProp = listFactory.addObjectListProperty(artifactType, NS+"hasSuccessor", artifactType);
		
		artifactType.declaredProperties().forEach(prop -> System.out.println(prop.getLocalName()));
		
		assertTrue(artifactType.declaredProperties()			
			.map(OntProperty::getLocalName)
			.anyMatch(name -> name.equals("hasSuccessor")));
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;		
		
		Reasoner reasoner = ReasonerRegistry.getOWLReasoner();
		reasoner.bindSchema(m);
		InfModel infmodel = ModelFactory.createInfModel(reasoner, m);
		Utils.printValidation(infmodel);
		
	}
}

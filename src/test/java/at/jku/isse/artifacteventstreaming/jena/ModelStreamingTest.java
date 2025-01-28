package at.jku.isse.artifacteventstreaming.jena;

import static org.junit.Assert.assertTrue;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.Test;

import at.jku.isse.passiveprocessengine.rdf.trialcode.ImmediateChangeApplyer;
import at.jku.isse.passiveprocessengine.rdf.trialcode.TransactionalChangeApplyer;

class ModelStreamingTest {

	public static String NS = "http://at.jku.isse.jena#";
	
	
	@Test
	void testStreamFromAtoB()  {
		OntModel sourceModel = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		OntModel targetModel = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		ImmediateChangeApplyer streamer = new ImmediateChangeApplyer(targetModel);
		sourceModel.register(streamer);
		
		sourceModel.setNsPrefix("isse", NS);
		OntClass artifactType = sourceModel.createOntClass(NS+"artifactType");		
		OntClass otherType = sourceModel.createOntClass(NS+"otherType");
		otherType.addDisjointClass(artifactType);
		org.apache.jena.ontapi.model.OntObjectProperty.Named successorProp = sourceModel.createObjectProperty(NS+"successor");
		successorProp.addDomain(artifactType);
		successorProp.addRange(artifactType);
		successorProp.addLabel("has successor");
		
		// lets create some instances:
		OntIndividual art1 = artifactType.createIndividual(NS+"art1");
		OntIndividual art2 = artifactType.createIndividual(NS+"art2");
		OntIndividual art3 = artifactType.createIndividual(NS+"art3");
		art1.addProperty(successorProp, art2);
		art1.removeAll(successorProp);
		art1.addProperty(successorProp, art3);
		
		RDFDataMgr.write(System.out, sourceModel, Lang.TURTLE) ;
		System.out.println("--- original / copy ------------");
		RDFDataMgr.write(System.out, targetModel, Lang.TURTLE) ;
		assertTrue(targetModel.containsAll(sourceModel));
	}

	
	@Test
	void testTransactionalStreamFromAtoB()  {
		OntModel sourceModel = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		OntModel targetModel = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		TransactionalChangeApplyer streamer = new TransactionalChangeApplyer(targetModel);
		sourceModel.register(streamer); // automatically creates a transaction/commit
		
		sourceModel.setNsPrefix("isse", NS);
		OntClass artifactType = sourceModel.createOntClass(NS+"artifactType");		
		OntClass otherType = sourceModel.createOntClass(NS+"otherType");
		otherType.addDisjointClass(artifactType);
		org.apache.jena.ontapi.model.OntObjectProperty.Named successorProp = sourceModel.createObjectProperty(NS+"successor");
		successorProp.addDomain(artifactType);
		successorProp.addRange(artifactType);
		successorProp.addLabel("has successor");
		
		// lets create some instances:
		OntIndividual art1 = artifactType.createIndividual(NS+"art1");
		OntIndividual art2 = artifactType.createIndividual(NS+"art2");
		OntIndividual art3 = artifactType.createIndividual(NS+"art3");
		art1.addProperty(successorProp, art2);
		streamer.commitTransaction("initial commit of types");
		
		art1.removeAll(successorProp);
		var scope = streamer.abortTransaction();
		// here we do a manual undo of changes by applying inverse statements
		sourceModel.remove(scope.getAddedStatements().stream().map(Statement.class::cast).toList());
		sourceModel.add(scope.getRemovedStatements().stream().map(Statement.class::cast).toList());
		
		art1.addProperty(successorProp, art3);
		streamer.commitTransaction("adding some more properties");
		
		RDFDataMgr.write(System.out, sourceModel, Lang.TURTLE) ;
		System.out.println("--- original / copy ------------");
		RDFDataMgr.write(System.out, targetModel, Lang.TURTLE) ;
		assertTrue(targetModel.containsAll(sourceModel));
	}
}

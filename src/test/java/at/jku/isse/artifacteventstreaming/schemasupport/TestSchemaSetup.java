package at.jku.isse.artifacteventstreaming.schemasupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;


public class TestSchemaSetup {

	static final String LIST_OF_ART = "listOfArt";
	static final String LIST_OF_STRING = "listOfString";
	static final String MAP_OF_ART = "mapOfArt";
	static final String PRIORITY = "priority";
	static final String PARENT = "parent";
	static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	
	OntClass typeBase;
	OntClass typeChild;
	OntRelationalProperty parent;
	OntRelationalProperty priority;
	OntObjectProperty mapOfArt;
	OntObjectProperty mapOfString;
	OntRelationalProperty listOfArt;
	OntRelationalProperty listOfString;
	OntRelationalProperty setOfArt;
	OntRelationalProperty setOfString;
	OntDataRange stringRange;
	OntDataRange intRange;
	OntDataRange booleanRange;
	MetaModelSchemaTypes metaSchema;
	
	@BeforeEach
	void setup() throws Exception {		
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);			
		BranchImpl branch = (BranchImpl) new BranchBuilder(new URI(NS+"repo"), repoDataset, repoModel )	
				.setModelReasoner(OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF)
				.setBranchLocalName("branch1")
				.build();		
		m = branch.getModel();	
		m.setNsPrefix("test", NS);
		var metaModel = MetaModelOntology.buildInMemoryOntology(); 
		metaSchema = new MetaModelSchemaTypes(m, metaModel);
		stringRange = m.getDatatype(org.apache.jena.vocabulary.XSD.xstring);
		intRange = m.getDatatype(org.apache.jena.vocabulary.XSD.xint);
		booleanRange = m.getDatatype(org.apache.jena.vocabulary.XSD.xboolean);
		
		
		// Create base and child types as OntClasses
		typeBase = m.createOntClass(NS + "artifact");
		typeChild = m.createOntClass(NS + "issue");
		typeChild.addSuperClass(typeBase);

		// Create properties using metaSchema
		priority = metaSchema.getSingleType().createSingleDataPropertyType(NS + PRIORITY, typeBase, intRange);
		parent = metaSchema.getSingleType().createSingleObjectPropertyType(NS + PARENT, typeChild, typeBase);

		// List and Set properties for String
		listOfString = metaSchema.getListType().addLiteralListProperty(typeBase, LIST_OF_STRING, stringRange);
		setOfString = metaSchema.getSetType().createDataPropertyType(NS + "setOfString", typeBase, stringRange);

		// List and Set properties for Art (child type)
		listOfArt = metaSchema.getListType().addObjectListProperty(typeBase, LIST_OF_ART, typeChild);
		setOfArt = metaSchema.getSetType().createObjectPropertyType(NS + "setofArt", typeBase, typeChild);

		// Map properties for Art and String
		mapOfArt = metaSchema.getMapType().addObjectMapProperty(typeChild, MAP_OF_ART, typeChild);
		mapOfString = metaSchema.getMapType().addLiteralMapProperty(typeChild, "mapOfString", stringRange);
	}
	
	
	

}
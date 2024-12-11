package at.jku.isse.artifacteventstreaming.rdf;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.schemasupport.ListResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType;
import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.CARDINALITIES;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFPropertyType;

class TestRDFPropertyType {

	public static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static NodeToDomainResolver resolver;
	static OntClass artifactType;
	static OntClass otherType;
	static MapResourceType mapFactory;
	static ListResourceType listFactory;
	static SingleResourceType singleFactory;
	
	@BeforeEach
	void setup() throws URISyntaxException, Exception {
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);			
		BranchImpl branch = (BranchImpl) new BranchBuilder(new URI(NS+"repo"), repoDataset, repoModel )	
				.setBranchLocalName("branch1")
				.build();		
		m = branch.getModel();		
		resolver = new NodeToDomainResolver(branch, null);
		resolver.getMapEntryBaseType();
		resolver.getListBaseType();
		mapFactory = resolver.getCardinalityUtil().getMapType();
		listFactory = resolver.getCardinalityUtil().getListType();
		singleFactory = resolver.getCardinalityUtil().getSingleType();
		artifactType = ((RDFInstanceType) resolver.createNewInstanceType(NS+"artifact")).getType();	
		otherType =  ((RDFInstanceType) resolver.createNewInstanceType(NS+"other")).getType();	
		otherType.addProperty(RDFS.label, "other");
	}
	
	@Test
	void testDetectListProperty() {
		String propURI = NS+"hasList";		
		var prop = listFactory.addObjectListProperty(artifactType, propURI, otherType)	;	
		assertNotNull(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.LIST, propType.getCardinality());
		assertEquals("other", propType.getInstanceType().getName());
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}
	
	@Test
	void testDetectMapProperty() {
		String propURI = NS+"hasMap";
		var prop = mapFactory.addObjectMapProperty(artifactType, propURI, otherType);
		assertNotNull(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.MAP, propType.getCardinality());
		assertEquals("other", propType.getInstanceType().getName());
	}

	@Test
	void testDetectSetProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createObjectProperty(propURI);
		prop.addRange(otherType);
		prop.addDomain(artifactType);
		assertNotNull(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SET, propType.getCardinality());
		assertEquals("other", propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectSingleProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createObjectProperty(propURI);
		prop.addRange(otherType);
		prop.addDomain(artifactType);
		var maxOneKey = m.createObjectMaxCardinality(prop, 1, null);
		artifactType.addSuperClass(maxOneKey);
		singleFactory.getSingleObjectProperty().addSubProperty(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		artifactType.superClasses().forEach(superType -> System.out.println(superType));
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals("other", propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectListDataProperty() {
		String propURI = NS+"hasList";		
		var prop = listFactory.addLiteralListProperty(artifactType, propURI, m.getDatatype(XSD.xdouble))	;	
		assertNotNull(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.LIST, propType.getCardinality());
		assertEquals("FLOAT", propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectDataMapProperty() {
		String propURI = NS+"hasMap";
		var prop = mapFactory.addLiteralMapProperty(artifactType, propURI, m.getDatatype(XSD.xstring));
		assertNotNull(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.MAP, propType.getCardinality());
		assertEquals("STRING", propType.getInstanceType().getName());
	}

	@Test
	void testDetectDataSetProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.xstring));
		prop.addDomain(artifactType);
		assertNotNull(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SET, propType.getCardinality());
		assertEquals("STRING", propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectSingleDataProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.xstring));
		prop.addDomain(artifactType);
		var maxOneKey = m.createDataMaxCardinality(prop, 1, null);
		artifactType.addSuperClass(maxOneKey);		
		singleFactory.getSingleLiteralProperty().addSubProperty(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals("STRING", propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectIntegerMapProperty() {
		String propURI = NS+"hasMap";
		var prop = mapFactory.addLiteralMapProperty(artifactType, propURI, m.getDatatype(XSD.xint));
		assertNotNull(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.MAP, propType.getCardinality());
		assertEquals(BuildInType.INTEGER.getName(), propType.getInstanceType().getName());
	}

	@Test
	void testDetectIntegerSetProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.xint));
		prop.addDomain(artifactType);
		assertNotNull(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SET, propType.getCardinality());
		assertEquals(BuildInType.INTEGER.getName(), propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectUnsupportedUnsigLongSetProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.unsignedLong));
		prop.addDomain(artifactType);
		assertNotNull(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SET, propType.getCardinality());
		assertEquals(BuildInType.STRING.getName(), propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectSingleLongProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.xlong));
		prop.addDomain(artifactType);
		var maxOneKey = m.createDataMaxCardinality(prop, 1, null);
		artifactType.addSuperClass(maxOneKey);		
		singleFactory.getSingleLiteralProperty().addSubProperty(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals(BuildInType.INTEGER.getName(), propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectSingleBooleanProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.xboolean));
		prop.addDomain(artifactType);
		var maxOneKey = m.createDataMaxCardinality(prop, 1, null);
		artifactType.addSuperClass(maxOneKey);		
		singleFactory.getSingleLiteralProperty().addSubProperty(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals(BuildInType.BOOLEAN.getName(), propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectSingleFloatProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.xfloat));
		prop.addDomain(artifactType);
		var maxOneKey = m.createDataMaxCardinality(prop, 1, null);
		artifactType.addSuperClass(maxOneKey);
		singleFactory.getSingleLiteralProperty().addSubProperty(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals(BuildInType.FLOAT.getName(), propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectSingleDoubleProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.xdouble));
		prop.addDomain(artifactType);
		var maxOneKey = m.createDataMaxCardinality(prop, 1, null);
		artifactType.addSuperClass(maxOneKey);
		singleFactory.getSingleLiteralProperty().addSubProperty(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals(BuildInType.FLOAT.getName(), propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectSingleShortProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.xshort));
		prop.addDomain(artifactType);
		var maxOneKey = m.createDataMaxCardinality(prop, 1, null);
		artifactType.addSuperClass(maxOneKey);		
		singleFactory.getSingleLiteralProperty().addSubProperty(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals(BuildInType.STRING.getName(), propType.getInstanceType().getName());
	}
	
	@Test
	void testDetectSingleUnsupportedDateProperty() {
		String propURI = NS+"hasSet";
		var prop = m.createDataProperty(propURI);
		prop.addRange(m.getDatatype(XSD.date));
		prop.addDomain(artifactType);
		var maxOneKey = m.createDataMaxCardinality(prop, 1, null);
		artifactType.addSuperClass(maxOneKey);		
		singleFactory.getSingleLiteralProperty().addSubProperty(prop);
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals(BuildInType.STRING.getName(), propType.getInstanceType().getName());
	}
}

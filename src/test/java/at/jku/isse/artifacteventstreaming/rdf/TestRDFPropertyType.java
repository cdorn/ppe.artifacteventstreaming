package at.jku.isse.artifacteventstreaming.rdf;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.passiveprocessengine.core.PPEInstanceType.CARDINALITIES;
import at.jku.isse.passiveprocessengine.rdf.MapResourceType;
import at.jku.isse.passiveprocessengine.rdf.RDFPropertyType;

class TestRDFPropertyType {

	public static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static RDFNodeToDomainResolver resolver;
	static OntClass artifactType;
	static OntClass otherType;
	static MapResourceType mapFactory;
	
	@BeforeEach
	void setup() {
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		resolver = new RDFNodeToDomainResolver(m);
		resolver.getMapEntryBaseType();
		mapFactory = resolver.getMapBase();
		artifactType = m.createOntClass(NS+"artifact");		
		otherType = m.createOntClass(NS+"other");
		otherType.addProperty(RDFS.label, "other");
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
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals("other", propType.getInstanceType().getName());
	}
	
	// TODO: test lists, once they are implemented
	
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
		RDFPropertyType propType = new RDFPropertyType(prop, resolver);
		
		assertEquals(CARDINALITIES.SINGLE, propType.getCardinality());
		assertEquals("STRING", propType.getInstanceType().getName());
	}
}

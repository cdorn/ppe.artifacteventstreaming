package at.jku.isse.artifacteventstreaming.rdf;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;

@TestInstance(Lifecycle.PER_CLASS)
class TestRDFListWrapper {

	private static final String LIST_OF_ART = "listOfArt";
	private static final String LIST_OF_STRING = "listOfString";
	public static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static NodeToDomainResolver resolver;
	PPEInstanceType typeBase;
	
	@BeforeAll
	void setup() {
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		resolver = new NodeToDomainResolver(m);
		resolver.getMapEntryBaseType();
		resolver.getListBaseType();
		typeBase = resolver.createNewInstanceType(NS+"artifact");
		typeBase.createListPropertyType(LIST_OF_ART, typeBase);
		typeBase.createListPropertyType(LIST_OF_STRING, BuildInType.STRING);
		
	}
	
	@BeforeEach
	void reset() {
		resolver.getAllInstancesOfTypeOrSubtype(typeBase).forEach(inst -> inst.markAsDeleted());
	}
	
	@Test
	void testReturnEmptyList() {
		var inst = resolver.createInstance(NS+"art1", typeBase);
		var artList = inst.getTypedProperty(LIST_OF_ART, List.class);
		assert(artList.isEmpty());
		
		
	}

}

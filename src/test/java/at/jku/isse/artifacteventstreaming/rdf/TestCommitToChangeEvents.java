package at.jku.isse.artifacteventstreaming.rdf;

import static at.jku.isse.passiveprocessengine.rdfwrapper.MapResourceType.MAP_NS;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.PPEPropertyType;
import at.jku.isse.passiveprocessengine.rdfwrapper.MapWrapper;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;

public class TestCommitToChangeEvents {

	static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static NodeToDomainResolver resolver;
	PPEInstanceType typeBase;
	PPEInstanceType typeChild;
	PPEPropertyType mapOfArt;
	
	@BeforeEach
	void setup() {
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		m.setNsPrefix("isse", NS);
		m.setNsPrefix("map", MAP_NS);
		resolver = new NodeToDomainResolver(m);
		resolver.getMapEntryBaseType();
//		resolver.getListBaseType();
		typeBase = resolver.createNewInstanceType(NS+"artifact");
		typeChild = resolver.createNewInstanceType(NS+"issue", typeBase);
		mapOfArt = typeChild.createMapPropertyType("mapOfArt", BuildInType.STRING, typeBase);
	}
	
	@Test
	void useMap() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		var artMap = art1.getTypedProperty(mapOfArt.getId(), MapWrapper.class);
		artMap.put("key1", art2);
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
	}
}

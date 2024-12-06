package at.jku.isse.artifacteventstreaming.rdf;

import static at.jku.isse.passiveprocessengine.rdfwrapper.MapResourceType.MAP_NS;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.errorprone.annotations.Var;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;
import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType.PPEPropertyType;
import at.jku.isse.passiveprocessengine.core.ProcessInstanceChangeListener;
import at.jku.isse.passiveprocessengine.core.PropertyChange.Update;
import at.jku.isse.passiveprocessengine.rdfwrapper.CommitChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.MapWrapper;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import lombok.Getter;

class TestCommitToChangeEvents {

	static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static NodeToDomainResolver resolver;
	PPEInstanceType typeBase;
	PPEInstanceType typeChild;
	PPEPropertyType mapOfArt;
	PPEPropertyType listOfString;
	PPEPropertyType setOfBaseArt;
	PPEPropertyType parent;
	StatementAggregator aggr;
	PPEChangeListener listener;
	CommitChangeEventTransformer transformer;
	
	@BeforeEach
	void setup() {
		m = OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM );
		resolver = new NodeToDomainResolver(m);
		aggr = new StatementAggregator();
		listener = new PPEChangeListener();
		transformer = new CommitChangeEventTransformer("Transformer", OntModelFactory.createModel( OntSpecification.OWL2_DL_MEM), resolver);
		transformer.registerWithWorkspace(listener);
		m.register(aggr);
		m.setNsPrefix("isse", NS);
		m.setNsPrefix("map", MAP_NS);
		resolver.getMapEntryBaseType();
		resolver.getListBaseType();
		typeBase = resolver.createNewInstanceType(NS+"artifact");
		typeChild = resolver.createNewInstanceType(NS+"issue", typeBase);
		mapOfArt = typeChild.createMapPropertyType("mapOfArt", BuildInType.STRING, typeBase);
		listOfString = typeChild.createListPropertyType("listOfString", BuildInType.STRING);
		setOfBaseArt = typeChild.createSetPropertyType("setOfBaseArt", typeBase);
		parent = typeBase.createSinglePropertyType("parent", typeBase);
	}
	
	private Commit generateCommit() {
		return new StatementCommitImpl("BranchId", "", "", 0, aggr.retrieveAddedStatements(), aggr.retrieveRemovedStatements());
	}
	
	@Test
	void useMap() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		var artMap = art1.getTypedProperty(mapOfArt.getId(), MapWrapper.class);
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		artMap.put("key1", art2);
		
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(1, listener.getLatestUpdates().size());
		
		artMap.put("key1", art1);
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(2, listener.getLatestUpdates().size()); //removal of old value, add of new values
		
		artMap.remove("key1");
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		assertEquals(1, listener.getLatestUpdates().size()); //removal of existing value
	}
	
	
	@Test
	void useList() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		
		var list = art1.getTypedProperty(listOfString.getId(), List.class);
		list.add("entry1");
		//RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(1, listener.getLatestUpdates().size());
		assertEquals("entry1", listener.getLatestUpdates().get(0).getValue());
		
		list.add(0, "entry2");
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(3, listener.getLatestUpdates().size()); // adding and replacements of existing item to next spot
		assertEquals(Set.of("entry1","entry2"), listener.getLatestUpdates().stream().map(event -> event.getValue()).collect(Collectors.toSet()));
		
		list.clear();
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(2, listener.getLatestUpdates().size()); // removal of both entries
		
	}
	
	@Test
	void useSet() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		var art3 = resolver.createInstance(NS+"art3", typeChild);
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		
		var set = art1.getTypedProperty(setOfBaseArt.getId(), Set.class);
		set.add(art2);
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(1, listener.getLatestUpdates().size());
		assertEquals(art2, listener.getLatestUpdates().get(0).getValue());
		
		set.add(art3);
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(1, listener.getLatestUpdates().size());
		assertEquals(art3, listener.getLatestUpdates().get(0).getValue());
		
		set.clear();
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(2, listener.getLatestUpdates().size()); // removal of both entries
	}
	
	@Test
	void useSingle() {
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		var art3 = resolver.createInstance(NS+"art3", typeChild);
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		
		art1.setSingleProperty(parent.getId(), art2);
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(1, listener.getLatestUpdates().size());
		assertEquals(art2, listener.getLatestUpdates().get(0).getValue());
		

		art1.setSingleProperty(parent.getId(), art3);
		transformer.handleCommit(generateCommit());
		listener.printCurrentUpdates();
		assertEquals(1, listener.getLatestUpdates().size());
		assertEquals(art3, listener.getLatestUpdates().get(0).getValue());
	}
	
	static class PPEChangeListener implements ProcessInstanceChangeListener {

		@Getter
		List<Update> latestUpdates;
		
		@Override
		public void handleUpdates(Collection<Update> arg0) {
			latestUpdates = new ArrayList<>(arg0);
		}
		
		public void printCurrentUpdates() {
			System.out.println("START:");
			latestUpdates.stream().forEach(event -> System.out.println(event));
			System.out.println("END:");
		}
		
	}
}

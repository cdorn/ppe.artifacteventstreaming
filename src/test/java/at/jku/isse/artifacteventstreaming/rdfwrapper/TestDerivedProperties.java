package at.jku.isse.artifacteventstreaming.rdfwrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.ReadWrite;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.rule.RuleException;
import at.jku.isse.artifacteventstreaming.rule.definition.DerivedPropertyRuleDefinition;
import at.jku.isse.passiveprocessengine.rdfwrapper.PrimitiveTypesFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFPropertyType;
import at.jku.isse.passiveprocessengine.rdfwrapper.config.InMemoryEventStreamingSetupFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.CommitChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;

class TestDerivedProperties {

	public static URI repoURI = URI.create("http://testrepos/derivedprop");
	
	static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static RuleEnabledResolver  resolver;
	Branch branch;
	RDFInstanceType typeBase;
	RDFInstanceType typeChild;
	RDFPropertyType mapOfArt;
	RDFPropertyType stringTitle;
	RDFPropertyType listOfString;
	RDFPropertyType setOfBaseArt;
	RDFPropertyType derivedListOfString;
	RDFPropertyType derivedSetOfBaseArt;
	RDFPropertyType derivedListOfBaseArt;
	RDFPropertyType derivedSetOfString;
	RDFPropertyType parent;
	StatementAggregator aggr;
	CommitChangeEventTransformer transformer;
	PrimitiveTypesFactory typeFactory;
	
	@BeforeEach
	void setup() throws Exception {		
		var backend = new InMemoryEventStreamingSetupFactory.FactoryBuilder().withBranchName("backend").withRepoURI(repoURI).build();
		branch = backend.getBranch();
		m = backend.getBranch().getModel();
		resolver = backend.getResolver();
		backend.signalExternalSetupComplete();
		typeFactory = resolver.getMetaschemata().getPrimitiveTypesFactory();
		
		backend.getBranch().getDataset().begin(ReadWrite.WRITE);
		typeBase = resolver.createNewInstanceType(NS+"artifact");
		parent = typeBase.createSinglePropertyType("parent", typeBase.getAsPropertyType());
		stringTitle = typeBase.createSinglePropertyType(NS+"title", typeFactory.getStringType());
		
		typeChild = resolver.createNewInstanceType(NS+"issue", typeBase);
		mapOfArt = typeChild.createMapPropertyType(NS+"mapOfArt", typeBase.getAsPropertyType());
		listOfString = typeChild.createListPropertyType(NS+"listOfString",typeFactory.getStringType());
		setOfBaseArt = typeChild.createSetPropertyType(NS+"setOfBaseArt", typeBase.getAsPropertyType());
		// derived property
		derivedListOfString = typeChild.createListPropertyType(NS+"derivedListOfString",typeFactory.getStringType());
		derivedSetOfBaseArt = typeChild.createSetPropertyType(NS+"derivedSetOfBaseArt", typeBase.getAsPropertyType());
		derivedListOfBaseArt = typeChild.createListPropertyType(NS+"derivedListOfBaseArt",typeFactory.getStringType());
		derivedSetOfString = typeChild.createSetPropertyType(NS+"derivedSetOfString", typeFactory.getStringType());
		backend.getBranch().commitChanges("schema created");
		
	}
	
	@Test
	void testDerivedStringSetProperty() throws Exception {
		branch.getDataset().begin(ReadWrite.WRITE);
		// we manually create rule on RDF level (later at wrapper level)
		var ruleDef = resolver.getRuleRepo().getRuleBuilder()
			.withContextType(typeChild.getType())
			.withDescription("TestDerivedRuleStringSet")
			.withRuleExpression("self.setOfBaseArt->collect(art | art.title).asSet()")
			.forDerivedProperty(derivedSetOfString.getProperty())
			.build();
		assertFalse(ruleDef.hasExpressionError());
		assertNotNull(ruleDef.getSyntaxTree());
		assertTrue(ruleDef instanceof DerivedPropertyRuleDefinition);			
		branch.commitChanges("RuleDef commit");
		
		branch.getDataset().begin(ReadWrite.WRITE);
		var art1 = resolver.createInstance( NS+"Art1", typeChild);
		var art2 = resolver.createInstance( NS+"Art2", typeChild);
		var art3 = resolver.createInstance( NS+"Art3", typeChild);
		art1.setSingleProperty(stringTitle.getId(), "Art1");
		art2.setSingleProperty(stringTitle.getId(), "Art2");
		art3.setSingleProperty(stringTitle.getId(), "Art3");
		art1.add(setOfBaseArt.getId(), art2);
		art1.add(setOfBaseArt.getId(), art3);
		branch.commitChanges("Instances commit");
		
		branch.getDataset().begin(ReadWrite.READ);
		var derivedSet = art1.getTypedProperty(derivedSetOfString.getId(), Set.class);
		assertNotNull(derivedSet);
		assertEquals(2, derivedSet.size());
		assertEquals(Set.of("Art3", "Art2"), derivedSet.stream().map(x -> x).collect(Collectors.toSet()));
	}
	
	@Test
	void testDerivedStringListProperty() throws Exception {
		fail();
	}
}

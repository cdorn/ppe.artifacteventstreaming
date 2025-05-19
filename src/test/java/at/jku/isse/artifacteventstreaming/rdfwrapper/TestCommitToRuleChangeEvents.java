package at.jku.isse.artifacteventstreaming.rdfwrapper;

import static at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType.MAP_NS;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.branch.StatementAggregator;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleException;
import at.jku.isse.artifacteventstreaming.rule.RuleRepositoryInspector;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.rule.evaluation.ActiveRuleTriggerObserver;
import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleEvaluationIterationMetadata;
import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleEvaluationListener;
import at.jku.isse.artifacteventstreaming.rule.evaluation.RuleTriggerObserverFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.PrimitiveTypesFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFPropertyType;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.ChangeListener;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.CommitChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.PropertyChange.Update;
import at.jku.isse.passiveprocessengine.rdfwrapper.metaschema.WrapperMetaModelSchemaTypes;
import at.jku.isse.passiveprocessengine.rdfwrapper.metaschema.WrapperMetaModelSchemaTypes.WrapperMetaModelOntology;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;
import lombok.Getter;

class TestCommitToRuleChangeEvents {

	static String NS = "http://at.jku.isse.test#";
	static OntModel m;
	static NodeToDomainResolver resolver;
	RDFInstanceType typeBase;
	RDFInstanceType typeChild;
	RDFPropertyType mapOfArt;
	RDFPropertyType listOfString;
	RDFPropertyType setOfBaseArt;
	RDFPropertyType parent;
	StatementAggregator aggr;
	PPEChangeListener listener;
	CommitChangeEventTransformer transformer;
	BranchImpl branch;
	ActiveRuleTriggerObserver observer;
	RuleRepositoryInspector inspector;
	PrimitiveTypesFactory typeFactory;
	
	@BeforeEach
	void setup() throws Exception {		
		Dataset repoDataset = DatasetFactory.createTxnMem();
		OntModel repoModel =  OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);			
		branch = (BranchImpl) new BranchBuilder(new URI(NS+"repo"), repoDataset, repoModel )	
				.setBranchLocalName("branch1")
				.setModelReasoner(OntSpecification.OWL2_DL_MEM_RDFS_INF)
				.build();		
		m = branch.getModel();		
		typeFactory = new PrimitiveTypesFactory(m);
		var metaModel = WrapperMetaModelOntology.buildInMemoryOntology(); 
		new RuleSchemaFactory(metaModel); // add rule schema to meta model		
		var cardUtil = new WrapperMetaModelSchemaTypes(m, metaModel);
		var observerFactory = new RuleTriggerObserverFactory(cardUtil);
		observer = observerFactory.buildActiveInstance("RuleTriggeringObserver", m, repoModel);
		observer.registerListener(new TestRuleEvaluationListener());
		var repairService = new RepairService(m, observer.getRepo());
		resolver = new RuleEnabledResolver(branch, repairService, observer.getFactory(), observer.getRepo(), cardUtil);
		inspector = new RuleRepositoryInspector(observer.getFactory());
		//aggr = new StatementAggregator();
		listener = new PPEChangeListener();
		transformer = new CommitChangeEventTransformer("Transformer", repoModel, resolver, observer.getFactory());
		transformer.registerWithBranch(listener);		
		m.setNsPrefix("isse", NS);
		m.setNsPrefix("map", MAP_NS);
		branch.appendBranchInternalCommitService(observer);
		branch.appendBranchInternalCommitService(transformer);
		branch.startCommitHandlers(null);
		branch.getDataset().begin();
		
		resolver.getMapEntryBaseType();
		resolver.getListBaseType();
		typeBase = resolver.createNewInstanceType(NS+"artifact");
		parent = typeBase.createSinglePropertyType("parent", typeBase.getAsPropertyType());
		
		typeChild = resolver.createNewInstanceType(NS+"issue", typeBase);
		mapOfArt = typeChild.createMapPropertyType("mapOfArt", typeBase.getAsPropertyType());		
		listOfString = typeChild.createListPropertyType("listOfString", typeFactory.getStringType());
		setOfBaseArt = typeChild.createSetPropertyType("setOfBaseArt", typeBase.getAsPropertyType());
				
		branch.commitChanges("InitialCommit");
	}
		
	@Test
	void useList() throws BranchConfigurationException, PersistenceException, RuleException {
		branch.getDataset().begin();
		observer.getRepo().getRuleBuilder()
		.withContextType(typeChild.getType())
		.withRuleTitle("TestListUsage")
		.withRuleExpression("self.listOfString.size() > 0")
		.build();		
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		branch.commitChanges("Commit 1");
		listener.printCurrentUpdates();
		listener.latestUpdates.clear();
		
		System.out.println("  ");
		System.out.println("Begin 2  ");
		branch.getDataset().begin();
		var list = art1.getTypedProperty(listOfString.getId(), List.class);
		list.add("entry1");
		//RDFDataMgr.write(System.out, m, Lang.TURTLE) ;
		branch.commitChanges("Commit 2");
		listener.printCurrentUpdates();
		assertEquals(2, listener.getLatestUpdates().size());
		assertEquals(true, listener.getLatestUpdates().stream().filter(update -> update.getName().equals("ruleHasConsistentResult")).findAny().get().getValue());
		assertEquals("entry1", listener.getLatestUpdates().stream().filter(update -> update.getName().equals("listOfString")).findAny().get().getValue());
		listener.latestUpdates.clear();
		
		System.out.println("  ");
		System.out.println("Begin 3  ");
		branch.getDataset().begin();
		list.add(0, "entry2");
		branch.commitChanges("Commit 3");
		listener.printCurrentUpdates();
		assertEquals(3, listener.getLatestUpdates().size()); // adding and replacements of existing item to next spot, no change of rule eval outcome, hence no event		
		assertEquals(true, listener.getLatestUpdates().stream().filter(update -> update.getName().equals("ruleHasConsistentResult")).findAny().isEmpty());
		listener.latestUpdates.clear();
		
		
		System.out.println("  ");
		System.out.println("Begin 4  ");
		branch.getDataset().begin();
		list.clear();
		branch.commitChanges("Commit 4");
		listener.printCurrentUpdates();
		assertEquals(3, listener.getLatestUpdates().size()); // removal of both entries
		assertEquals(false, listener.getLatestUpdates().stream().filter(update -> update.getName().equals("ruleHasConsistentResult")).findAny().get().getValue());
		listener.latestUpdates.clear();
	}
	
	@Test
	void useSet() throws BranchConfigurationException, PersistenceException, RuleException {
		branch.getDataset().begin();
		observer.getRepo().getRuleBuilder()
		.withContextType(typeChild.getType())
		.withRuleTitle("TestSetUsage")
		.withRuleExpression("self.setOfBaseArt.size() > 0")
		.build();
		var art1 = resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		var art3 = resolver.createInstance(NS+"art3", typeChild);
		branch.commitChanges("Commit 1");
		listener.printCurrentUpdates();
		listener.latestUpdates.clear();
		
		branch.getDataset().begin();
		var set = art1.getTypedProperty(setOfBaseArt.getId(), Set.class);
		set.add(art2);
		branch.commitChanges("Commit 2");
		listener.printCurrentUpdates();
		assertEquals(2, listener.getLatestUpdates().size());
		assertEquals(true, listener.getLatestUpdates().stream().filter(update -> update.getName().equals("ruleHasConsistentResult")).findAny().get().getValue());
		listener.latestUpdates.clear();
		
		branch.getDataset().begin();
		set.add(art3);
		branch.commitChanges("Commit 3");
		listener.printCurrentUpdates();
		assertEquals(1, listener.getLatestUpdates().size());
		assertEquals(true, listener.getLatestUpdates().stream().filter(update -> update.getName().equals("ruleHasConsistentResult")).findAny().isEmpty());
		listener.latestUpdates.clear();
		
		branch.getDataset().begin();
		set.clear();
		branch.commitChanges("Commit 4");
		listener.printCurrentUpdates();
		assertEquals(3, listener.getLatestUpdates().size()); // removal of both entries
		assertEquals(false, listener.getLatestUpdates().stream().filter(update -> update.getName().equals("ruleHasConsistentResult")).findAny().get().getValue());
		listener.latestUpdates.clear();
	}
	
	@Test
	void useSingle() throws BranchConfigurationException, PersistenceException, RuleException {
		branch.getDataset().begin();
		observer.getRepo().getRuleBuilder()
		.withContextType(typeChild.getType())
		.withRuleTitle("TestSingleUsage")
		.withRuleExpression("self.parent.isDefined()")
		.build();
		var art1 = (RDFInstance)resolver.createInstance(NS+"art1", typeChild);
		var art2 = resolver.createInstance(NS+"art2", typeChild);
		var art3 = resolver.createInstance(NS+"art3", typeChild);
		branch.commitChanges("Commit 1");
		listener.latestUpdates.clear();
		inspector.getAllScopes().stream().forEach(scope -> inspector.printScope(scope));
		
		System.out.println("  ");
		System.out.println("Begin 2  ");
		branch.getDataset().begin();
		art1.setSingleProperty(parent.getId(), art2);
		branch.commitChanges("Commit 2");
		listener.printCurrentUpdates();
		assertEquals(2, listener.getLatestUpdates().size());
		//assertEquals(true, listener.getLatestUpdates().stream().filter(update -> update.getName().equals("ruleHasConsistentResult")).findAny().get().getValue());
		listener.latestUpdates.clear();

		System.out.println("  ");
		System.out.println("Begin 3  ");
		branch.getDataset().begin();
		art1.setSingleProperty(parent.getId(), art3);
		branch.commitChanges("Commit 3");
		listener.printCurrentUpdates();
		assertEquals(1, listener.getLatestUpdates().size()); // update of the property, no change in evaluation result
		assertEquals(true, listener.getLatestUpdates().stream().filter(update -> update.getName().equals("ruleHasConsistentResult")).findAny().isEmpty());
		listener.latestUpdates.clear();
		
		System.out.println("  ");
		System.out.println("Begin 4  ");
		branch.getDataset().begin();
		art1.setSingleProperty(parent.getId(), null);
		branch.commitChanges("Commit 4");
		listener.printCurrentUpdates();
		assertEquals(2, listener.getLatestUpdates().size()); // removal of the property and change in evaluation result
		listener.latestUpdates.clear();
	}
	
	static class TestRuleEvaluationListener implements RuleEvaluationListener {

		@Override
		public void signalRuleEvaluationFinished(Set<RuleEvaluationIterationMetadata> iterationMetadata) {
			if (iterationMetadata.isEmpty()) return;
			System.out.println("Rules Evaluated:");
			iterationMetadata.forEach(reim -> {
				var name = reim.getRule().getDefinition().getName();
				var ctx = reim.getRule().getContextInstance().getLocalName();
				var result = reim.getRule().isConsistent();
				var isDiff = reim.getHasEvaluationOutcomeChanged() ? "NEW" : "SAME";
				System.out.println(name+" with "+ ctx + " -> "+result+ " "+isDiff);	
			});
		}
		
	}
	
	static class PPEChangeListener implements ChangeListener {

		@Getter
		List<Update> latestUpdates = new ArrayList<>();
		
		@Override
		public void handleUpdates(Collection<Update> arg0) {
			latestUpdates.addAll(arg0);
		}
		
		public void printCurrentUpdates() {
			System.out.println("START:");
			latestUpdates.stream().forEach(System.out::println);
			System.out.println("END:");			
		}
		
	}
}

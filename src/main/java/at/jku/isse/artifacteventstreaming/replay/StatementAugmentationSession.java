package at.jku.isse.artifacteventstreaming.replay;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.api.AES.OPTYPE;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class StatementAugmentationSession {
		
	private final List<Statement> addedStatements;
	private final List<Statement> removedStatements;
	private final PropertyCardinalityTypes schemaUtils;
	private final PerResourceHistoryRepository historyRepo;
	
	private final Set<ContainedStatement> addedAugmentedStatements = new HashSet<>();
	private final Set<ContainedStatement> removedAugmentedStatements = new HashSet<>();
	
	public void process(String commitId, String branchURI, long timestamp) throws PersistenceException {
		 Map<RDFNode, List<StatementWrapper>> opsPerInst = collectPerInstance(addedStatements, removedStatements);
		 opsPerInst.entrySet().stream()
			.forEach(entry -> processPerInstance(entry.getKey(), entry.getValue()));	
		 
		 historyRepo.appendHistory(commitId, branchURI, timestamp, addedAugmentedStatements, removedAugmentedStatements);
	}
	
	private Map<RDFNode, List<StatementWrapper>> collectPerInstance(List<Statement> addedStatements, List<Statement> removedStatements) {
		// we keep the keys (i.e,instances and affected property) in order of their manipulation (.e.g, creating an instance before adding it somewhere else)				
		Map<RDFNode, List<StatementWrapper>> opsPerInst = new LinkedHashMap<>(); 
		// we can't map to instances right now, as the subject of a list manipulation is the listresource and not the individual/owner of the list 
		addedStatements.stream().forEach(stmt -> 
			opsPerInst.computeIfAbsent(stmt.getSubject(), k -> new LinkedList<>()).add(new StatementWrapper(stmt, AES.OPTYPE.ADD)));
		removedStatements.stream().forEach(stmt -> 
			opsPerInst.computeIfAbsent(stmt.getSubject(), k -> new LinkedList<>()).add(new StatementWrapper(stmt, AES.OPTYPE.REMOVE)));
		return opsPerInst;
	}
	
	private void processPerInstance(RDFNode node, List<StatementWrapper> stmts) {
		// ideally we dont need to access schema to do the transformation
				// the branch's statement aggregator guarantees that there are no statements without effect 
				// (i.e., adding and removing a statement cancel each other and thus both statement are removed) 
		// determine class or individual
		if (node.canAs(OntClass.class))	{
			OntClass ontClass = node.as(OntClass.class);
			processTypeEvents(ontClass, stmts);
		} else {
			// Removal of an entry or list, etc results in no more type information, hence need to check for deletion statement amongst the list
			List<Resource> delTypes = getAnyTypeDeletion(stmts).toList();
			if (delTypes.isEmpty() && node.canAs(OntIndividual.class)) {
				processStatementsOfResourcesWithoutTypeDeletions(node, stmts);
			} else if (node.canAs(OntObject.class)){
				processStatementsOfResourcesWithTypeDeletions(node, stmts, delTypes);
			} else {
			//untyped 
				processUntypedSubject(node, stmts);												
			}
		}
	}
	
	protected void processTypeEvents(OntClass node, List<StatementWrapper> stmts) {
		wrapInContainmentStatements(stmts);
	}
	
	private void processStatementsOfResourcesWithoutTypeDeletions(RDFNode node, List<StatementWrapper> stmts) {
		var ontInd = node.as(OntIndividual.class);			
		// instance or map (list/seq/bag is an ont individual)
		if (schemaUtils.getMapType().isMapEntry(ontInd)) {		// a map entry was inserted or updated
			processMapEntry(ontInd, stmts, false);
		} else if (schemaUtils.getListType().isListCollection(ontInd)) { 	// list entry added/removed/reordered
			processListContainer(ontInd, stmts, false);
		} else { 	// --> could also be adding a new list to the owner, or new mapentry to the owner, or removing from the owner
		// else something (e.g., single value) added/removed from individual, might be a set
			processIndividual(ontInd, stmts);
		}
	}
	
	private void processStatementsOfResourcesWithTypeDeletions(RDFNode node, List<StatementWrapper> stmts, List<Resource> delTypes) {
		var ontInd = node.as(OntObject.class);
		// reomved instance or map (list/seq/bag is an ont individual)
		if (schemaUtils.getMapType().wasMapEntry(delTypes)) { 		// a map entry was removed
			processMapEntry(ontInd, stmts, true);
		} else if (schemaUtils.getListType().wasListCollection(delTypes)) { // list removed
			processListContainer(ontInd, stmts, true);
		} else {
			// here we can't filter out removal of deleted containment elements as their type is no longer accessible, 
			// we would have to find them in the overall statement collection
		// else something (e.g., single value) added/removed from individual, might be a set
			processIndividual(ontInd, stmts);
		}
	}
	

	

	
	private Stream<Resource> getAnyTypeDeletion(List<StatementWrapper> wrappers) {
		return wrappers.stream()
			.filter(wrapper -> wrapper.op().equals(AES.OPTYPE.REMOVE))	
			.map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(RDF.type))
			.map(Statement::getResource);
	}
	
	private void processListContainer(OntObject list, List<StatementWrapper> stmts, boolean isDelete) {		
		/*  adding to a list
		 *  removing from a list
		 *  --> here we can check if the elements is of list type
		 */  
		var id = list.isAnon() ? list.getId() : list.getURI();
		// first obtain the owner of the list
		var optOwner = isDelete ? schemaUtils.getFormerListOwner(stmts) : schemaUtils.getCurrentListOwner((OntIndividual) list);
		// should only exist one such resource as we dont share lists across individuals
		if (optOwner.isEmpty()) {
			log.error("Encountered ownerless list "+id);
			// just produce basic wrappers
			wrapInContainmentStatements(stmts);
			return;
		}
		var owner = optOwner.get();
		var commonProps = /*isDelete ? findFormerPropertiesBetween(owner, list) :*/ schemaUtils.getListType().findListReferencePropertiesBetween(owner, list); // list is never removed, just stays empty
		if (commonProps.size() != 1) {
			log.error(String.format("Cannot unambiguously determine list ownership/containment property to use between %s and %s, found %s", owner.getURI(), id, commonProps.size()));
			// just produce basic wrappers
			wrapInContainmentStatements(stmts);
			return;
		}
		var listProp = commonProps.get(0);
		processListContainer(stmts, owner, listProp);
	}	
	
	protected void processListContainer(List<StatementWrapper> stmts, Resource owner, Property listProp) {
		wrapInContainmentStatements(stmts, owner, listProp);
	}
	
	private void processMapEntry(OntObject mapEntry, List<StatementWrapper> stmts, boolean isDeletion) {
		var id = mapEntry.isAnon() ? mapEntry.getId() : mapEntry.getURI();
		// first obtain the owner of the entry
		var optOwner = isDeletion ? getFormerMapEntryOwner(stmts) : getCurrentMapEntryOwner((OntIndividual) mapEntry);
		
		if (optOwner.isEmpty()) {
			log.error("Encountered ownerless mapentry "+id);
			// just produce basic wrappers
			wrapInContainmentStatements(stmts);
			return;
		}
		var owner = optOwner.get();
		// possible cases: hence either  2, or 3+ statements (as any other metadata potentially added)
		// simply adding of a new value without replacement value OR  removal of a value not possible
		// replacement of a value (we cant have ownerreference and value or key changed, as owner reference remains with entry upon creation, no changes upon update)
		// adding of a key + value + ownerreference (i.e., first insert)
		// removal of a key + value + ownerreference (key/value removal)
		
		var commonProps = isDeletion? findFormerPropertiesBetween(owner, mapEntry) :  schemaUtils.getMapType().findMapReferencePropertiesBetween(owner, mapEntry);
		if (commonProps.size() != 1) {
			log.error(String.format("Cannot unambiguously determine map entry property to use between %s and %s, found %s", owner.getURI(), id, commonProps.size()));
			// just produce basic wrappers
			wrapInContainmentStatements(stmts);
			return;
		}
		var prop = commonProps.get(0);
		if (stmts.size() <= 2) {
			handleMapValueChanges(stmts, prop, owner);
		} else { 
			handleMapEntryChange(stmts, prop, owner);
		} 
	}
	
	protected void handleMapValueChanges(List<StatementWrapper> stmts, Property prop, Resource changeSubject) {
		wrapInContainmentStatements(stmts, changeSubject, prop);
	}
	
	protected void handleMapEntryChange(List<StatementWrapper> stmts, Property prop, Resource changeSubject) {
		wrapInContainmentStatements(stmts, changeSubject, prop);
	}
	
	private Optional<Resource> getCurrentMapEntryOwner(OntIndividual mapEntry) {
		return Optional.ofNullable(mapEntry.getPropertyResourceValue(schemaUtils.getMapType().getContainerProperty().asProperty()));
	}
	
	private Optional<Resource> getFormerMapEntryOwner(List<StatementWrapper> stmts) {
		return stmts.stream().filter(wrapper -> wrapper.op().equals(AES.OPTYPE.REMOVE))
			.map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(schemaUtils.getMapType().getContainerProperty().asProperty()))
			.map(Statement::getResource)
			.findAny();
	}
	
	private List<Property> findFormerPropertiesBetween(Resource subject, Resource object) {
		return removedStatements.stream()
			.filter(stmt -> stmt.getSubject().equals(subject) && stmt.getObject().equals(object))
			.map(Statement::getPredicate)
			.toList();
	}
	

	protected void processIndividual(OntObject inst, List<StatementWrapper> stmts) {
		wrapInContainmentStatements(stmts);
	}
	
	protected boolean isSingleProperty(Statement stmt) {		
		if (stmt.getObject().isLiteral()) {			
			StmtIterator iter = stmt.getPredicate().listProperties(RDFS.subPropertyOf);
			while (iter.hasNext()) {
				var res = iter.next().getResource();
				if (res.equals(schemaUtils.getSingleType().getSingleLiteralProperty())) {
					return true;
				}
			}
			return false;
			
//			var ontProp = schemaUtils.getSingleType().getSingleLiteralProperty().getModel().getDataProperty(stmt.getPredicate().getURI());
//			if (ontProp == null) return false;
//			return schemaUtils.getSingleType().getSingleLiteralProperty().hasSubProperty(ontProp, false);
		} else {
			StmtIterator iter = stmt.getPredicate().listProperties(RDFS.subPropertyOf);
			while (iter.hasNext()) {
				if (iter.next().getResource().equals(schemaUtils.getSingleType().getSingleObjectProperty())) {
					return true;
				}
			}
			return false;
			
			
//			var ontProp = schemaUtils.getSingleType().getSingleLiteralProperty().getModel().getObjectProperty(stmt.getPredicate().getURI());
//			if (ontProp == null) return false;
//			return schemaUtils.getSingleType().getSingleObjectProperty().hasSubProperty(ontProp, false);
		}		
//		var prop = schemaUtils.getSingleType().getSingleLiteralProperty().getModel().getProperty(stmt.getPredicate().getURI());
//		if (prop.canAs(OntObjectProperty.class)) {
//			var ontProp = prop.as(OntObjectProperty.class);
//			return (stmt.getObject().isLiteral() && schemaUtils.getSingleType().getSingleLiteralProperty().hasSubProperty(ontProp, false)|| 
//					!stmt.getObject().isLiteral() && schemaUtils.getSingleType().getSingleObjectProperty().hasSubProperty(ontProp, false));
//		} else { // if not an ont object property, then cannot be a single property
//			return false;
//		}
	}
	
	protected void processUntypedSubject(RDFNode inst, List<StatementWrapper> stmts) {
		wrapInContainmentStatements(stmts);
	}
	
	protected void wrapInContainmentStatements(List<StatementWrapper> stmts) {
		stmts.stream().forEach(stmt -> { 
			if (stmt.op.equals(OPTYPE.ADD)) {
				addedAugmentedStatements.add(new ContainedStatementImpl(stmt.stmt()));
			} else {
				removedAugmentedStatements.add(new ContainedStatementImpl(stmt.stmt()));
				}
			});
	}
	
	protected void wrapInContainmentStatements(List<StatementWrapper> stmts, Resource container, Property containmentProperty) {
		stmts.stream().forEach(stmt -> { 
			if (stmt.op.equals(OPTYPE.ADD)) {
				addedAugmentedStatements.add(new ContainedStatementImpl(stmt.stmt(), container, containmentProperty));
			} else {
				removedAugmentedStatements.add(new ContainedStatementImpl(stmt.stmt(), container, containmentProperty));
				}
			});
	}

	public static record StatementWrapper(Statement stmt, AES.OPTYPE op) {}
	
}

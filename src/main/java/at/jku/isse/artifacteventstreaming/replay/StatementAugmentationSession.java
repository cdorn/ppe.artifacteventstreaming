package at.jku.isse.artifacteventstreaming.replay;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.function.library.leviathan.log;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
@Slf4j
public class StatementAugmentationSession {
		
	private final List<ContainedStatement> addedStatements;
	private final List<ContainedStatement> removedStatements;
	private final MetaModelSchemaTypes schemaUtils;

	public void process() {
		 Map<Resource, List<StatementWrapper>> opsPerInst = collectPerInstance(addedStatements, removedStatements);
		 opsPerInst.entrySet().stream()
			.forEach(entry -> processPerInstance(entry.getKey(), entry.getValue()));	
	}
	
	private Map<Resource, List<StatementWrapper>> collectPerInstance(List<ContainedStatement> addedStatements, List<ContainedStatement> removedStatements) {
		// we keep the keys (i.e,instances and affected property) in order of their manipulation (.e.g, creating an instance before adding it somewhere else)				
		Map<Resource, List<StatementWrapper>> opsPerInst = new LinkedHashMap<>();
		// we can't map to instances right now, as the subject of a list manipulation is the listresource and not the individual/owner of the list 
		addedStatements.stream().forEach(stmt -> 
			opsPerInst.computeIfAbsent(stmt.getSubject(), k -> new LinkedList<>()).add(new StatementWrapper(stmt, AES.OPTYPE.ADD)));
		removedStatements.stream().forEach(stmt -> 
			opsPerInst.computeIfAbsent(stmt.getSubject(), k -> new LinkedList<>()).add(new StatementWrapper(stmt, AES.OPTYPE.REMOVE)));
		return opsPerInst;
	}
	
	private void processPerInstance(Resource node, List<StatementWrapper> stmts) {
		// ideally we dont need to access schema to do the transformation
		// the branch's statement aggregator guarantees that there are no statements without effect
		// (i.e., adding and removing a statement cancel each other and thus both statement are removed)

		// Class/Schema level changes:
		// e.g., a mapentry/listcollection subtype added/removed (together with its owning property)
		// or a 'regular' new ontclass created/removed
		// at that level: there are not list or map containment statements, only at OntIndiviual level, done below
		//if (node.canAs(OntClass.class))	{
		//	OntClass ontClass = node.as(OntClass.class);
		//	processTypeEvents(ontClass, stmts);
		//} else {
		// OntIndividual level changes:
			// Removal of an entry or list, etc results in no more type information, hence need to check for deletion statement amongst the list
			// we also need to know whether an individual was deleted, or just its properties altered
			List<Resource> delTypes = getAnyTypeDeletion(stmts).toList();
			if (delTypes.isEmpty() ) {
				processStatementsOfResourcesWithoutTypeDeletions(node, stmts);
			} else {
				processStatementsOfResourcesWithTypeDeletions(node, stmts, delTypes);
			}
		//}
	}

	/**
	 *
	 * @param resource could be a :
	 *             <li>    mapentry that was changed (or added) </li>
	 *             <li>   listEntry added/relocated, </li>
	 *             <li> basic individual that has property added/removed </li>
	 *             <li> owningResource that has list resource added/removed (special case of above) </li>
	 *   we focus specifically on items 1 and 2, as these need augmentation with owner information
	 * @param stmts
	 */
	private void processStatementsOfResourcesWithoutTypeDeletions(Resource resource, List<StatementWrapper> stmts) {
		// instance or map (list/seq/bag is an ont individual)
		if (schemaUtils.getMapType().isMapEntry(resource)) {		// a map entry was inserted or updated
			processMapEntry(resource, stmts, false);
		} else if (schemaUtils.getListType().isListCollection(resource)) { 	// list entry added/removed/reordered
			processListContainer(resource, stmts, false);
		} else { 	// --> could also be adding a new list to the owner, or new mapentry to the owner, or removing from the owner
		// else something (e.g., single value) added/removed from individual, might be a set
			processIndividual(resource, stmts);
		}
	}
	
	private void processStatementsOfResourcesWithTypeDeletions(Resource resource, List<StatementWrapper> stmts, List<Resource> delTypes) {
		// removed instance or map (list/seq/bag is an ont individual)
		if (schemaUtils.getMapType().wasMapEntryBasedOnURI(resource)) { 		// a map entry was removed
			processMapEntry(resource, stmts, true);
		} else if (schemaUtils.getListType().wasListCollectionBasedOnURI(resource)) { // list removed
			processListContainer(resource, stmts, true);
		} else {
			// here we can't filter out removal of deleted containment elements as their type is no longer accessible, 
			// we would have to find them in the overall statement collection
		// else something (e.g., single value) added/removed from individual, might be a set
			processIndividual(resource, stmts);
		}
	}

	
	private Stream<Resource> getAnyTypeDeletion(List<StatementWrapper> wrappers) {
		return wrappers.stream()
			.filter(wrapper -> wrapper.op().equals(AES.OPTYPE.REMOVE))
			.map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(RDF.type))
			.map(Statement::getResource);
	}
	
	private void processListContainer(Resource list, List<StatementWrapper> stmts, boolean isDelete) {
		/*  adding to a list
		 *  removing from a list
		 *  --> here we can check if the elements is of list type
		 */  
		var id = list.isAnon() ? list.getId() : list.getURI();
		// first obtain the owner of the list
		var optOwner = isDelete ? schemaUtils.getListType().getFormerListOwner(stmts) : schemaUtils.getListType().getCurrentListOwner(list);
		// should only exist one such resource as we dont share lists across individuals
		if (optOwner.isEmpty()) {
            log.error("Encountered ownerless list {}", id);
			// just produce basic wrappers
			wrapInContainmentStatements(stmts);
			return;
		}
		var owner = optOwner.get();
		var commonProps = isDelete ? findFormerPropertiesBetween(owner, list) : schemaUtils.getListType().findListReferencePropertiesBetween(owner, list); // list is never removed, just stays empty, except when individual removed
		if (commonProps.size() != 1) {
			log.error("Cannot unambiguously determine list ownership/containment property to use between {} and {}, found {}", owner.getURI(), id, commonProps.size());
			// just produce basic wrappers
			wrapInContainmentStatements(stmts);
			return;
		}
		var listProp = commonProps.getFirst();
		processListContainer(stmts, owner, listProp);
	}	
	
	protected void processListContainer(List<StatementWrapper> stmts, Resource owner, Property listProp) {
		wrapInContainmentStatements(stmts, owner, listProp);
	}
	
	private void processMapEntry(Resource mapEntry, List<StatementWrapper> stmts, boolean isDeletion) {
		var id = mapEntry.isAnon() ? mapEntry.getId() : mapEntry.getURI();
		// first obtain the owner of the entry
		var optOwner = isDeletion ? getFormerMapEntryOwner(stmts) : getCurrentMapEntryOwner(mapEntry);
		
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
			log.error("Cannot unambiguously determine map entry property to use between {} and {}, found {}", owner.getURI(), id, commonProps.stream().map(Property::getURI).collect(Collectors.joining(", ")));
			// just produce basic wrappers
			wrapInContainmentStatements(stmts);
			return;
		}
		var prop = commonProps.getFirst();
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
	
	private Optional<Resource> getCurrentMapEntryOwner(Resource mapEntry) {
		return Optional.ofNullable(mapEntry.getPropertyResourceValue(schemaUtils.getMapType().getMapOwnedByProperty().asProperty()));
	}
	
	private Optional<Resource> getFormerMapEntryOwner(List<StatementWrapper> stmts) {
		return stmts.stream().filter(wrapper -> wrapper.op().equals(AES.OPTYPE.REMOVE))
			.map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(schemaUtils.getMapType().getMapOwnedByProperty().asProperty()))
			.map(Statement::getResource)
			.findAny();
	}
	
	private List<Property> findFormerPropertiesBetween(Resource subject, Resource object) {
		return removedStatements.stream()
			.filter(stmt -> stmt.getSubject().equals(subject) && stmt.getObject().equals(object))
			.map(Statement::getPredicate)
			.toList();
	}
	
	// delegate methods in case someone wants to subclass this, and needs more control over what happens.
	protected void processIndividual(Resource resource, List<StatementWrapper> stmts) {
		wrapInContainmentStatements(stmts);
	}
	

	protected void wrapInContainmentStatements(List<StatementWrapper> stmts) {
		// NoOp as we directly update the statement
	}
	
	protected void wrapInContainmentStatements(List<StatementWrapper> stmts, Resource container, Property containmentProperty) {
		stmts.stream().forEach(stmt -> stmt.stmt().augmentWithContainment(container, containmentProperty));
	}

    public record StatementWrapper(ContainedStatement stmt, AES.OPTYPE op) {
    }
	
}

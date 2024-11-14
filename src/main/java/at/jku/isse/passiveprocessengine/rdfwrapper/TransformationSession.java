package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.passiveprocessengine.core.PropertyChange;
import at.jku.isse.passiveprocessengine.core.PropertyChange.Update;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class TransformationSession {
	
	private final Commit commit;
	private final NodeToDomainResolver resolver;
	
	
	protected List<Update> process() {
		 Map<RDFNode, List<StatementWrapper>> opsPerInst = collectPerInstance(commit);
		 return opsPerInst.entrySet().stream()
			.flatMap(entry -> dispatchType(entry.getKey(), entry.getValue()))
			.toList();	
	}
	
	private Map<RDFNode, List<StatementWrapper>> collectPerInstance(Commit commit) {
		// we keep the keys (i.e,instances and affected property) in order of their manipulation (.e.g, creating an instance before adding it somewhere else)				
		Map<RDFNode, List<StatementWrapper>> opsPerInst = new LinkedHashMap<>(); 
		// we can't map to instances right now, as the subject of a list manipulation is the listresource and not the individual/owner of the list 
		commit.getAddedStatements().stream().forEach(stmt -> {
			opsPerInst.computeIfAbsent(stmt.getSubject(), k -> new LinkedList<>()).add(new StatementWrapper(stmt, OP.ADD));
		});
		commit.getRemovedStatements().stream().forEach(stmt -> {
			opsPerInst.computeIfAbsent(stmt.getSubject(), k -> new LinkedList<>()).add(new StatementWrapper(stmt, OP.REMOVE));
		});
		return opsPerInst;
	}
	
	private Stream<Update> dispatchType(RDFNode node, List<StatementWrapper> stmts) {
		// ideally we dont need to access schema to do the transformation
				// the branch's statement aggregator guarantees that there are no statements without effect 
				// (i.e., adding and removing a statement cancel each other and thus both statement are removed) 
		// determine class or individual
		if (node.canAs(OntClass.class))	{
			//FIXME: handle instance types / ontclass events
			OntClass ontClass = node.as(OntClass.class);
			log.debug("Ignoring statements about OntClass resource "+ontClass.getURI());
			return Stream.empty();
			
		} else {
			// Removal of an entry or list, etc results in no more type information, hence need to check for deletion statement amongst the list
			List<Resource> delTypes = getAnyTypeDeletion(stmts).toList();
			if (delTypes.isEmpty() && node.canAs(OntIndividual.class)) {
				var ontInd = node.as(OntIndividual.class);			
				// instance or map (list/seq/bag is an ont individual)
				if (resolver.getMapBase().isMapEntry(ontInd)) {		// a map entry was inserted or updated
					return processMapEntry(ontInd, stmts, false);
				} else if (resolver.getListBase().isListContainer(ontInd)) { 	// list entry added/removed/reordered
					return processListContainer(ontInd, stmts, false);
				} else { 	// --> could also be adding a new list to the owner, or new mapentry to the owner, or removing from the owner
					stmts = filterOutListOrMapOwnershipAdditions(stmts);
				// else something (e.g., single value) added/removed from individual, might be a set
					return processIndividual(ontInd, stmts);
				}
			} else if (node.canAs(OntObject.class)){
				var ontInd = node.as(OntObject.class);
				// reomved instance or map (list/seq/bag is an ont individual)
				if (resolver.getMapBase().wasMapEntry(delTypes)) { 		// a map entry was removed
					return processMapEntry(ontInd, stmts, true);
				} else if (resolver.getListBase().wasListContainer(delTypes)) { // list removed
					return processListContainer(ontInd, stmts, true);
				} else {
					// here we can't filter out removal of deleted containment elements as their type is no longer accessible, 
					// we would have to find them in the overall statement collection
				// else something (e.g., single value) added/removed from individual, might be a set
					return processIndividual(ontInd, stmts);
				}
			} else {
			//untyped 
				return processUntypedSubject(node, stmts);												
			}
		}
	}
	
	private List<StatementWrapper> filterOutListOrMapOwnershipAdditions(List<StatementWrapper> stmts) {
		return stmts.stream().filter(wrapper -> {
			var object = wrapper.stmt().getObject();
			return !isMapEntryOrListContainer(object);
		}).toList();
	}
	
	private boolean isMapEntryOrListContainer(RDFNode obj) {		
		if (obj.isLiteral()) return false;
		if (obj.canAs(OntIndividual.class)) { // perhaps a mapEntry or list
			var ind = obj.as(OntIndividual.class);
			return resolver.getMapBase().isMapEntry(ind) || resolver.getListBase().isListContainer(ind);
		} // Seq is an OntInd
		return false;
	}
	
	private Stream<Resource> getAnyTypeDeletion(List<StatementWrapper> wrappers) {
		return wrappers.stream()
			.filter(wrapper -> wrapper.op().equals(OP.REMOVE))	
			.map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(RDF.type))
			.map(Statement::getResource);
	}
	
	private Stream<Update> processListContainer(OntObject list, List<StatementWrapper> stmts, boolean isDelete) {		
		/*  adding to a list
		 *  removing from a list
		 *  --> here we can check if the elements is of list type
		 */  
		var id = list.isAnon() ? list.getId() : list.getURI();
		// first obtain the owner of the list
		var optOwner = isDelete ? getFormerListOwner(stmts) : getCurrentListyOwner((OntIndividual) list);
		// should only exist one such resource as we dont share lists across individuals
		if (optOwner.isEmpty()) {
			log.error("Encountered ownerless list "+id);
			return Stream.empty();
		}
		var owner = optOwner.get();
		RDFInstance changeSubject = (RDFInstance) resolver.resolveToRDFElement(owner);
		var commonProps = /*isDelete ? findFormerPropertiesBetween(owner, list) :*/ findCurrentPropertiesBetween(owner, list); // list is never removed, just stays empty
		if (commonProps.size() != 1) {
			log.error(String.format("Cannot unambiguously determine list ownership/containment property to use between %s and %s, found %s", owner.getURI(), id, commonProps.size()));
			return Stream.empty();
		}
		var listProp = commonProps.get(0);
		return stmts.stream()
				.filter(wrapper -> wrapper.stmt().getPredicate().getOrdinal() > 0) // filter out any non-'li' properties
				.map(wrapper -> {
			var value = resolver.convertFromRDF(wrapper.stmt().getObject());
			if (wrapper.op().equals(OP.ADD)) {
				return new PropertyChange.Add(listProp.getLocalName(), changeSubject, value);
			} else {
				return new PropertyChange.Remove(listProp.getLocalName(), changeSubject, value);
			}										
		});
	}	
	
	private Stream<Update> processMapEntry(OntObject mapEntry, List<StatementWrapper> stmts, boolean isDeletion) {
		var id = mapEntry.isAnon() ? mapEntry.getId() : mapEntry.getURI();
		// first obtain the owner of the entry
		var optOwner = isDeletion ? getFormerMapEntryOwner(stmts) : getCurrentMapEntryOwner((OntIndividual) mapEntry);
		
		if (optOwner.isEmpty()) {
			log.error("Encountered ownerless mapentry "+id);
			return Stream.empty();
		}
		var owner = optOwner.get();
		// possible cases: hence either  2, or 3+ statements (as any other metadata potentially added)
		// simply adding of a new value without replacement value OR  removal of a value not possible
		// replacement of a value (we cant have ownerreference and value or key changed, as owner reference remains with entry upon creation, no changes upon update)
		// adding of a key + value + ownerreference (i.e., first insert)
		// removal of a key + value + ownerreference (key/value removal)
		RDFInstance changeSubject = (RDFInstance) resolver.resolveToRDFElement(owner);
		var commonProps = isDeletion? findFormerPropertiesBetween(owner, mapEntry) : findCurrentPropertiesBetween(owner, (OntIndividual) mapEntry);
		if (commonProps.size() != 1) {
			log.error(String.format("Cannot unambiguously determine map entry property to use between %s and %s, found %s", owner.getURI(), id, commonProps.size()));
			return Stream.empty();
		}
		var prop = commonProps.get(0);
		if (stmts.size() <= 2) {
			return handleMapValueChanges(stmts, prop, changeSubject);
		} else { 
			return handleMapEntryChange(stmts, prop, changeSubject);
		} 
	}
	
	private Stream<Update> handleMapValueChanges(List<StatementWrapper> stmts, Property prop, RDFInstance changeSubject) {
		return stmts.stream()
				.filter(wrapper -> isMapValueProperty(wrapper.stmt().getPredicate()))
				.map(wrapper -> {
						var value = resolver.convertFromRDF(wrapper.stmt().getObject());
						if (wrapper.op().equals(OP.ADD)) {
							return new PropertyChange.Add(prop.getLocalName(), changeSubject, value);
						} else {
							return new PropertyChange.Remove(prop.getLocalName(), changeSubject, value);
						}
				});
	}
	
	
	private boolean isMapValueProperty(Property property) {
		if (property.canAs(OntProperty.class)) { 
			var prop = property.as(OntProperty.class);
			return resolver.getMapBase().getLiteralValueProperty().hasSubProperty(prop, true) 
					|| resolver.getMapBase().getObjectValueProperty().hasSubProperty(prop, true);
		}
		else return false;
	}
	
	private Stream<Update> handleMapEntryChange(List<StatementWrapper> stmts, Property prop, RDFInstance changeSubject) {
		var keyOpt = findFirstStatementAboutProperty(stmts, resolver.getMapBase().getKeyProperty().asProperty());
		var litValueOpt = findFirstStatementAboutProperty(stmts, resolver.getMapBase().getLiteralValueProperty().asProperty());
		var objValueOpt = findFirstStatementAboutProperty(stmts, resolver.getMapBase().getObjectValueProperty().asProperty());
		// we are not interested in back reference
		if (keyOpt.isEmpty() || (litValueOpt.isEmpty() && objValueOpt.isEmpty())) {
			log.error("MapEntry change has inconsistent statements, cannot generate change event");
			return Stream.empty();
		}
		var value = resolver.convertFromRDF(litValueOpt.orElse(objValueOpt.get()).getObject());
		if (stmts.get(0).op().equals(OP.ADD)) { // all statements have to have the same OP, otherwise inconsistent
			return Stream.of(new PropertyChange.Add(prop.getLocalName(), changeSubject, value));
		} else {
			return Stream.of(new PropertyChange.Remove(prop.getLocalName(), changeSubject, value));
		}
	}
	
	private Optional<Resource> getCurrentMapEntryOwner(OntIndividual mapEntry) {
		return Optional.ofNullable(mapEntry.getPropertyResourceValue(resolver.getMapBase().getContainerProperty().asProperty()));
	}
	
	private Optional<Resource> getFormerMapEntryOwner(List<StatementWrapper> stmts) {
		return stmts.stream().filter(wrapper -> wrapper.op().equals(OP.REMOVE))
			.map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(resolver.getMapBase().getContainerProperty().asProperty()))
			.map(Statement::getResource)
			.findAny();
	}
	
	private Optional<Resource> getCurrentListyOwner(OntIndividual list) {
		return Optional.ofNullable(list.getPropertyResourceValue(resolver.getListBase().getContainerProperty().asProperty()));
	}
	
	private Optional<Resource> getFormerListOwner(List<StatementWrapper> stmts) {
		return stmts.stream().filter(wrapper -> wrapper.op().equals(OP.REMOVE))
			.map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(resolver.getListBase().getContainerProperty().asProperty()))
			.map(Statement::getResource)
			.findAny();
	}

	
	private List<Property> findCurrentPropertiesBetween(Resource subject, OntObject object) {
		List<Property> props = new ArrayList<>();
		var iter = subject.getModel().listStatements(subject, null, object);
		while (iter.hasNext()) {
			props.add(iter.next().getPredicate());
		}
		return props;
	}
	
	private List<Property> findFormerPropertiesBetween(Resource subject, Resource object) {
		return commit.getRemovedStatements().stream()
			.filter(stmt -> stmt.getSubject().equals(subject) && stmt.getObject().equals(object))
			.map(Statement::getPredicate)
			.toList();
	}
	
	private Optional<Statement> findFirstStatementAboutProperty(List<StatementWrapper> stmts, Property prop) {
		return stmts.stream().map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(prop))
			.findAny();
	}
	
	private Stream<Update> processIndividual(OntObject inst, List<StatementWrapper> stmts) {
		/* based on statements alone we cant distinguish between adding/removing from a set from changes to an individual/single property: 
		 * here we need to access schema information, either from abstraction layer RDFPropertyType, or replicate whats done inside the RDFPropertyType (less efficient)
		 */
		RDFInstance changeSubject = (RDFInstance) resolver.resolveToRDFElement(inst);
		if (changeSubject == null) { //typically for schema information
			return Stream.empty();
		}
		// something (e.g., or single value) added/removed from individual
		return stmts.stream().map(wrapper -> {
			//FIXME for now we treat all as a set
			var value = resolver.convertFromRDF(wrapper.stmt().getObject());
			if (value == null) return null;
			var prop = wrapper.stmt().getPredicate();
			if (wrapper.op().equals(OP.ADD)) {
				return (Update)new PropertyChange.Add(prop.getLocalName(), changeSubject, value);
			} else {
				return (Update)new PropertyChange.Remove(prop.getLocalName(), changeSubject, value);
			}
		}).filter(Objects::nonNull);
	}
	
	private Stream<Update> processUntypedSubject(RDFNode inst, List<StatementWrapper> stmts) {
		stmts.stream().forEach(wrapper -> {
		// inserting a list element may lead to a lot of list changes as all the indexes are updates (i.e., removed and added+1)
			var prop = wrapper.stmt().getPredicate();
			int ordinal = prop.getOrdinal();
			if (ordinal != 0) { // then a 'li', we have a list, 
			// now get the parent/owner of the list
			
		}
		});
		return Stream.empty();
	}
	
	
	

	private enum OP {ADD, REMOVE}
	private record StatementWrapper(Statement stmt, OP op) {}
	
}

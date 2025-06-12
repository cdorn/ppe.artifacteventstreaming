package at.jku.isse.passiveprocessengine.rdfwrapper.events;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.AES.OPTYPE;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.replay.StatementAugmentationSession;
import at.jku.isse.artifacteventstreaming.rule.RuleRepositoryInspector;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFElement;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.events.PropertyChange.Update;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Christoph Mayr-Dorn
 *
 * This class transforms into highlevel events,
 * to properly make use of contained statements for history replaying 
 * it is crucial that each overridden method also calls its super implementation.
 */
@Slf4j
public class TransformationSession extends StatementAugmentationSession {
		
	private final NodeToDomainResolver resolver;
	final RuleRepositoryInspector inspector;
	@Getter private final List<Update> updates = new LinkedList<>();

	public TransformationSession(List<ContainedStatement> addedStatements, List<ContainedStatement> removedStatements,
			NodeToDomainResolver resolver, RuleSchemaProvider ruleSchema) {
		super(addedStatements, removedStatements, resolver.getMetaschemata());
		this.resolver = resolver;
		this.inspector = new RuleRepositoryInspector(ruleSchema);
	}
	
	@Override
	protected void processTypeEvents(OntClass node, List<StatementWrapper> stmts) {
		//FIXME: handle instance types / ontclass events
		super.processTypeEvents(node, stmts);
	}
	
	@Override
	protected void processListContainer(List<StatementWrapper> stmts, Resource owner, Property listProp) {
		super.processListContainer(stmts, owner, listProp);
		/*  adding to a list
		 *  removing from a list
		 *  --> here we can check if the elements is of list type
		 */  
		RDFInstance changeSubject = (RDFInstance) resolver.resolveToRDFElement(owner);
		stmts.stream()
				.filter(wrapper -> wrapper.getStmt().getPredicate().getOrdinal() > 0) // filter out any non-'li' properties
				.forEach(wrapper -> {
			var value = resolver.convertFromRDF(wrapper.getStmt().getObject());
			if (wrapper.getOp().equals(AES.OPTYPE.ADD)) {
				updates.add(new PropertyChange.Add(URI.create(listProp.getURI()), changeSubject, value));
			} else {
				updates.add(new PropertyChange.Remove(URI.create(listProp.getURI()), changeSubject, value));
			}										
		});
	}	
	
	@Override
	protected void handleMapValueChanges(List<StatementWrapper> stmts, Property prop, Resource owner) {
		super.handleMapValueChanges(stmts, prop, owner);
		RDFElement changeElement = resolver.resolveToRDFElement(owner);
		if (changeElement instanceof RDFInstance changeSubject) {
			updates.addAll(stmts.stream()
				.filter(wrapper -> isMapValueProperty(wrapper.getStmt().getPredicate()))
				.map(wrapper -> {
						var value = resolver.convertFromRDF(wrapper.getStmt().getObject());
						if (wrapper.getOp().equals(AES.OPTYPE.ADD)) {
							return new PropertyChange.Add(URI.create(prop.getURI()), changeSubject, value);
						} else {
							return new PropertyChange.Remove(URI.create(prop.getURI()), changeSubject, value);
						}
				})
				.toList());
		} // currently abstraction layer cannot handle changes to RDFInstanceTypes
	}
	
	private boolean isMapValueProperty(Property property) {
		if (property.canAs(OntProperty.class)) { 
			var prop = property.as(OntProperty.class);
			return resolver.getMetaschemata().getMapType().getLiteralValueProperty().hasSubProperty(prop, true) 
					|| resolver.getMetaschemata().getMapType().getObjectValueProperty().hasSubProperty(prop, true);
		}
		else return false;
	}

	@Override
	protected void handleMapEntryChange(List<StatementWrapper> stmts, Property prop, Resource owner) {
		super.handleMapEntryChange(stmts, prop, owner);
		var changeSubject = resolver.resolveToRDFElement(owner);
		
		var keyOpt = findFirstStatementAboutProperty(stmts, resolver.getMetaschemata().getMapType().getKeyProperty().asProperty());
		var litValueOpt = findFirstStatementAboutProperty(stmts, resolver.getMetaschemata().getMapType().getLiteralValueProperty().asProperty());
		var objValueOpt = findFirstStatementAboutProperty(stmts, resolver.getMetaschemata().getMapType().getObjectValueProperty().asProperty());
		// we are not interested in back reference
		if (keyOpt.isEmpty() || (litValueOpt.isEmpty() && objValueOpt.isEmpty())) {
			log.error("MapEntry change has inconsistent statements, cannot generate change event");
			return;
		}
		Object value;
		if (litValueOpt.isPresent()) {
			value = resolver.convertFromRDF(litValueOpt.get().getObject());
		} else {
			value = resolver.convertFromRDF(objValueOpt.get().getObject());
		}
		if (stmts.get(0).getOp().equals(AES.OPTYPE.ADD)) { // all statements have to have the same OP, otherwise inconsistent
			updates.add(new PropertyChange.Add(URI.create(prop.getURI()), changeSubject, value));
		} else {
			updates.add(new PropertyChange.Remove(URI.create(prop.getURI()), changeSubject, value));
		}
	}
	
	private Optional<ContainedStatement> findFirstStatementAboutProperty(List<StatementWrapper> stmts, Property prop) {
		return stmts.stream().map(StatementWrapper::getStmt)
			.filter(stmt -> stmt.getPredicate().equals(prop))
			.findAny();
	}

	@Override
	protected void processIndividual(OntObject inst, List<StatementWrapper> stmts) {
		super.processIndividual(inst, stmts);
		/* based on statements alone we cant distinguish between adding/removing from a set from changes to an individual/single property: 
		 * here we need to access schema information, either from abstraction layer RDFPropertyType, or replicate whats done inside the RDFPropertyType (less efficient)
		 */
		stmts = filterOutListOrMapOwnershipAdditions(stmts);
		var shallowCopy = List.copyOf(stmts);
		
		if (inst.isAnon()) { // e.g., rule eval scopes are anonymous nodes
			processAnonObject(inst, stmts);
		} else {
		RDFInstance changeSubject = (RDFInstance) resolver.resolveToRDFElement(inst);
		if (changeSubject == null) { //typically for schema information
			return;
		}
		// something (e.g., or single value) added/removed from individual
		updates.addAll(stmts.stream().map(wrapper -> {
			var value = resolver.convertFromRDF(wrapper.getStmt().getObject());
			if (value == null) return null;
			var prop = wrapper.getStmt().getPredicate();
			if (isSingleProperty(wrapper.getStmt())) {
				if (wrapper.getOp().equals(AES.OPTYPE.ADD)) {
					return new PropertyChange.Set(URI.create(prop.getURI()), changeSubject, value);
				} else {
					// only if there is no new value added at the same time (null values are not allowed by Jena)
					var optAdd = shallowCopy.stream().filter(copy -> copy.getStmt().getPredicate().equals(prop) && copy.getOp().equals(OPTYPE.ADD)).findAny();
					if (optAdd.isEmpty()) {
						return new PropertyChange.Set(URI.create(prop.getURI()), changeSubject, value);
					} else
						return null;
				}
			} else {
				if (wrapper.getOp().equals(AES.OPTYPE.ADD)) {
					return (Update)new PropertyChange.Add(URI.create(prop.getURI()), changeSubject, value);
				} else {
					return (Update)new PropertyChange.Remove(URI.create(prop.getURI()), changeSubject, value);
				}
			}
		}).filter(Objects::nonNull)
		.toList());
		}
	}
	
	private List<StatementWrapper> filterOutListOrMapOwnershipAdditions(List<StatementWrapper> stmts) {
		return stmts.stream().filter(wrapper -> {
			var object = wrapper.getStmt().getObject();
			return !isMapEntryOrListContainer(object);
		}).toList();
	}
	
	private void processAnonObject(OntObject inst, List<StatementWrapper> stmts) {
		//FIXME: ugly hack to provide scope information to process rule change listener
		updates.addAll(stmts.stream()
				.filter(wrapper -> wrapper.getStmt().getPredicate().getURI().equals(RuleSchemaFactory.usingPropertyURI)) // we do this only for the usingProperty predicate of a scope, nothing else, its a hack!
				.map(wrapper -> {
					var propName = wrapper.getStmt().getPredicate().getURI();
					var scope = wrapper.getStmt().getSubject();
					var element = inspector.getElementFromScope(scope); // from scope object to owner instance of that scope
					if (element != null) {
						var subject = resolver.convertFromRDF(element);
						if (subject instanceof RDFElement instSubject) {
							if (wrapper.getOp().equals(AES.OPTYPE.ADD)) {
								return new PropertyChange.Add(URI.create(propName), instSubject, wrapper.getStmt().getResource().getURI());
							} else {
								return new PropertyChange.Remove(URI.create(propName), instSubject, wrapper.getStmt().getResource().getURI());
							}
						}
					}
					return null;
				})
				.filter(Objects::nonNull)
				.toList());
	}
	
	private boolean isMapEntryOrListContainer(RDFNode obj) {		
		if (obj.isLiteral()) return false;
		if (obj.canAs(OntIndividual.class)) { // perhaps a mapEntry or list
			var ind = obj.as(OntIndividual.class);
			return resolver.getMetaschemata().getMapType().isMapEntry(ind) || resolver.getMetaschemata().getListType().isListCollection(ind);
		} // Seq is an OntInd
		return false;
	}
	
	@Override
	protected void processUntypedSubject(RDFNode inst, List<StatementWrapper> stmts) {
		super.processUntypedSubject(inst, stmts);
//		stmts.stream().forEach(wrapper -> {
//		// inserting a list element may lead to a lot of list changes as all the indexes are updates (i.e., removed and added+1)
//			var prop = wrapper.stmt().getPredicate();
//			int ordinal = prop.getOrdinal();
//			if (ordinal != 0) { // then a 'li', we have a list, 
//			// now get the parent/owner of the list
//			
//		}
//		});
		stmts.stream().forEach(wrapper -> {
			var localName = wrapper.getStmt().getPredicate().getURI();
			localName.length();
		});
		return; //TODO: for now we ignore untyped changes
	}
	

	
}

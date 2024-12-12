package at.jku.isse.passiveprocessengine.rdfwrapper;

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
import org.apache.jena.rdf.model.Statement;
import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.replay.PerResourceHistoryRepository;
import at.jku.isse.artifacteventstreaming.replay.StatementAugmentationSession;
import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PropertyChange;
import at.jku.isse.passiveprocessengine.core.PropertyChange.Update;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransformationSession extends StatementAugmentationSession {
		
	private final NodeToDomainResolver resolver;
	@Getter private final List<Update> updates = new LinkedList<>();

	public TransformationSession(List<Statement> addedStatements, List<Statement> removedStatements,
			NodeToDomainResolver resolver, PerResourceHistoryRepository historyRepo) {
		super(addedStatements, removedStatements, resolver.getCardinalityUtil(), historyRepo);
		this.resolver = resolver;
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
				.filter(wrapper -> wrapper.stmt().getPredicate().getOrdinal() > 0) // filter out any non-'li' properties
				.forEach(wrapper -> {
			var value = resolver.convertFromRDF(wrapper.stmt().getObject());
			if (wrapper.op().equals(AES.OPTYPE.ADD)) {
				updates.add(new PropertyChange.Add(listProp.getLocalName(), changeSubject, value));
			} else {
				updates.add(new PropertyChange.Remove(listProp.getLocalName(), changeSubject, value));
			}										
		});
	}	
	
	@Override
	protected void handleMapValueChanges(List<StatementWrapper> stmts, Property prop, Resource owner) {
		super.handleMapValueChanges(stmts, prop, owner);
		RDFInstance changeSubject = (RDFInstance) resolver.resolveToRDFElement(owner);
		
		updates.addAll(stmts.stream()
				.filter(wrapper -> isMapValueProperty(wrapper.stmt().getPredicate()))
				.map(wrapper -> {
						var value = resolver.convertFromRDF(wrapper.stmt().getObject());
						if (wrapper.op().equals(AES.OPTYPE.ADD)) {
							return new PropertyChange.Add(prop.getLocalName(), changeSubject, value);
						} else {
							return new PropertyChange.Remove(prop.getLocalName(), changeSubject, value);
						}
				})
				.toList());
	}
	
	private boolean isMapValueProperty(Property property) {
		if (property.canAs(OntProperty.class)) { 
			var prop = property.as(OntProperty.class);
			return resolver.getCardinalityUtil().getMapType().getLiteralValueProperty().hasSubProperty(prop, true) 
					|| resolver.getCardinalityUtil().getMapType().getObjectValueProperty().hasSubProperty(prop, true);
		}
		else return false;
	}

	@Override
	protected void handleMapEntryChange(List<StatementWrapper> stmts, Property prop, Resource owner) {
		super.handleMapEntryChange(stmts, prop, owner);
		PPEInstance changeSubject = (PPEInstance) resolver.resolveToRDFElement(owner);
		
		var keyOpt = findFirstStatementAboutProperty(stmts, resolver.getCardinalityUtil().getMapType().getKeyProperty().asProperty());
		var litValueOpt = findFirstStatementAboutProperty(stmts, resolver.getCardinalityUtil().getMapType().getLiteralValueProperty().asProperty());
		var objValueOpt = findFirstStatementAboutProperty(stmts, resolver.getCardinalityUtil().getMapType().getObjectValueProperty().asProperty());
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
		if (stmts.get(0).op().equals(AES.OPTYPE.ADD)) { // all statements have to have the same OP, otherwise inconsistent
			updates.add(new PropertyChange.Add(prop.getLocalName(), changeSubject, value));
		} else {
			updates.add(new PropertyChange.Remove(prop.getLocalName(), changeSubject, value));
		}
	}
	
	private Optional<Statement> findFirstStatementAboutProperty(List<StatementWrapper> stmts, Property prop) {
		return stmts.stream().map(StatementWrapper::stmt)
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
		
		RDFInstance changeSubject = (RDFInstance) resolver.resolveToRDFElement(inst);
		if (changeSubject == null) { //typically for schema information
			return;
		}
		// something (e.g., or single value) added/removed from individual
		updates.addAll(stmts.stream().map(wrapper -> {
var localName = wrapper.stmt().getPredicate().getLocalName();
			var value = resolver.convertFromRDF(wrapper.stmt().getObject());
			if (value == null) return null;
			var prop = wrapper.stmt().getPredicate();
			if (isSingleProperty(wrapper.stmt())) {
				if (wrapper.op().equals(AES.OPTYPE.ADD)) {
					return new PropertyChange.Set(prop.getLocalName(), changeSubject, value);
				} else {
					return null; // we can never unset a property as Apache Jena disallows null values, hence there is always a new value added, 
								//unless the resource is removed, but we dont have instance delete events for now
				}
			} else {
				if (wrapper.op().equals(AES.OPTYPE.ADD)) {
					return (Update)new PropertyChange.Add(prop.getLocalName(), changeSubject, value);
				} else {
					return (Update)new PropertyChange.Remove(prop.getLocalName(), changeSubject, value);
				}
			}
		}).filter(Objects::nonNull)
		.toList());
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
			return resolver.getCardinalityUtil().getMapType().isMapEntry(ind) || resolver.getCardinalityUtil().getListType().isListContainer(ind);
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
			var localName = wrapper.stmt().getPredicate().getLocalName();
			localName.length();
		});
		return; //TODO: for now we ignore untyped changes
	}
	

	
}

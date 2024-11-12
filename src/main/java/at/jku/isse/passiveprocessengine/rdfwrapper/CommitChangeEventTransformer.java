package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Seq;
import org.apache.jena.rdf.model.Statement;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.passiveprocessengine.core.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.ProcessInstanceChangeListener;
import at.jku.isse.passiveprocessengine.core.PropertyChange;
import at.jku.isse.passiveprocessengine.core.PropertyChange.Update;

public class CommitChangeEventTransformer extends AbstractHandlerBase implements ChangeEventTransformer, CommitHandler {

	private final NodeToDomainResolver resolver;
	private ProcessInstanceChangeListener eventSink;
	
	public CommitChangeEventTransformer(String serviceName, OntModel repoModel, NodeToDomainResolver resolver) {
		super(serviceName, repoModel);
		this.resolver = resolver;
		//TODO: find a way to restore/retrieve eventSink upon restart
	}	
	
	@Override
	protected String getServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+this.getClass().getSimpleName();
	}
	
	@Override
	public void registerWithWorkspace(ProcessInstanceChangeListener eventSink) {
		this.eventSink = eventSink;
		
	}

	@Override
	public void handleCommit(Commit commit) {
		List<Update> operations = new LinkedList<>();
		// ideally we dont need to access schema to do the transformation
		// the branch's statement aggregator guarantees that there are no statements without effect 
		// (i.e., adding and removing a statement cancel each other and thus both statement are removed) 
		/* following cases exist: 
		 *
		 * setting a single property: must always be combined with removing the former value (but if there is no former then none
		 * --> per individual there must always be exactly two statements, one add one remove, if there are more, then this is a set
		 * --> but if there is no former value (i.e., set for the first time, then it just looks like set)
		 * 
		 *  --> could also be adding a new list to the owner, or new map to the owner, or removing from the owner
		 *  
		 * 	adding to a set 
		 *  removing from a set
		 *  --> this looks like a single property in case we are only removing a single element and add another single element in that order
		 *  
		 *  adding to a list
		 *  removing from a list
		 *  --> here we can check if the property is a li property
		 *  
		 *  
		 *  adding to a map
		 *  removing from a map
		 *  --> here we can check if the property is a subproperty of the mapKeyProperty
		 *  --> but key and value are different statements!!! and subject to a entry element
		 *  --> if key is set the new entry, if just value is set then an update
		 */
		Map<RDFNode, List<StatementWrapper>> opsPerInst = collectPerInstance(commit);
		opsPerInst.entrySet().stream()
			.forEach(entry -> dispatchType(entry.getKey(), entry.getValue()));		
		eventSink.handleUpdates(operations);
	}

	
	private void dispatchType(RDFNode inst, List<StatementWrapper> stmts) {
		// determine class or individual
		if (inst.canAs(OntClass.class))	{
			OntClass ontClass = inst.as(OntClass.class);
			
			
			
		} else if (inst.canAs(OntIndividual.class)) {
			OntIndividual ontInd = inst.as(OntIndividual.class);			
			// instance or map (list/seq/bag is not an ont individual?!)
			if (resolver.getMapBase().isMapEntry(ontInd)) {
				// a map entry was inserted or updated
				processMapEntry(ontInd, stmts);
			} else if (resolver.getListBase().isListContainer(ontInd)) {
				// list entry added/removed/reordered
				processListContainer(ontInd, stmts);
			} else {
			// else something (e.g., single value) added/removed from individual
				processIndividual(ontInd, stmts);
			}
		} else {
			//untyped 
			processUntypedSubject(inst, stmts);												
		}
		
		
	}
	
	private void processIndividual(OntIndividual inst, List<StatementWrapper> stmts) {
		// something (e.g., seqContainer, mapContainer, or single value) added/removed from individual
		stmts.stream().forEach(wrapper -> {
			// here we manage maps and lists per parent/owner, thus if value is a list or map --> ignore as statements of actual value processed separately 
			
		});
		
	}
	
	public boolean isObjectAMapEntryOrListContainer(Resource obj) {		
		if (obj.isLiteral()) return false;
		if (obj.canAs(OntIndividual.class)) { // perhaps a mapEntry
			var ind = obj.as(OntIndividual.class);
			return resolver.getMapBase().isMapEntry(ind) || resolver.getListBase().isListContainer(ind);
		} // check if Seq is an OntInd
		
		return false;
	}
	
	private void processListContainer(OntIndividual list, List<StatementWrapper> stmts) {		
		// obtain the owner of the entry: assumes we always assign the list to the owner before adding to the list
		Optional<OntClass> optSeqType = getSeqSubClass(list);
		if (optSeqType.isEmpty()) { // generic list
		
		} else {
			var type = optSeqType.get();
			Property listProp = null; //TODO resolve
			
			
			var iter = type.getModel().listResourcesWithProperty(listProp, list);
			// should only exist one such resource as we dont share lists across individuals
			if (iter.hasNext()) {
				var owner = iter.next().as(OntIndividual.class);
				RDFInstanceType changeSubject = (RDFInstanceType) resolver.resolveToType(owner);
				stmts.stream().forEach(wrapper -> {
					var res = wrapper.stmt().getResource();
					if (wrapper.op().equals(OP.ADD)) {
						new PropertyChange.Add(listProp.getLocalName(), changeSubject, resourceToValue(res));
					} else {
						new PropertyChange.Remove(listProp.getLocalName(), changeSubject, resourceToValue(res));
					}										
				});
			}
			
			
		}
		
		
		// removal of an entry at pos	
		// adding of a value at pos		
	}	
	
	private Object resourceToValue(Resource res) {
		if (res.isLiteral()) {
			return res.asLiteral().getValue();
		} else {
			return resolver.resolveToInstance(res);
		}
	}
	
	private Optional<OntClass> getSeqSubClass(OntIndividual seqInstance) {
		return seqInstance.classes(true)
			.filter(superClass -> !superClass.equals(resolver.getListBaseType()))
			.findAny();
	}
	
	private void processMapEntry(OntIndividual mapEntry, List<StatementWrapper> stmts) {
		// in any case obtain the owner of the entry
		
		// removal of a key + value
		// removal of a value
		// adding of a key + value
		// adding of a new value
		
	}
	
	private void processUntypedSubject(RDFNode inst, List<StatementWrapper> stmts) {
		stmts.stream().forEach(wrapper -> {
		// inserting a list element may lead to a lot of list changes as all the indexes are updates (i.e., removed and added+1)
			var prop = wrapper.stmt().getPredicate();
			int ordinal = prop.getOrdinal();
			if (ordinal != 0) { // then a 'li', we have a list, 
			// now get the parent/owner of the list
			
		}
		});
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

	private enum OP {ADD, REMOVE}
	private record StatementWrapper(Statement stmt, OP op) {}
	
	

}

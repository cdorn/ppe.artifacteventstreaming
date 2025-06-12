package at.jku.isse.passiveprocessengine.rdfwrapper.crossbranch;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFElement;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Christoph Mayr-Dorn
 * 
 * @summary Ensures that artifacts that are newly introduced/added to this branch show up correctly in the Instance cache,
 * 
 * This handler needs to be put after the commit change applier to have access to predicate properties such as restrictions, super/sub properties, etc.
 * Always use in combination with {@link ElementDeletedCacheUpdater}
 * 
*/
@Slf4j
public class ElementCreatedCacheUpdater extends AbstractHandlerBase {
	
	private final NodeToDomainResolver resolver;
	
	public ElementCreatedCacheUpdater(Branch branch, NodeToDomainResolver resolver) {
		super("ArtifactCreatedCacheUpdaterFor"+branch.getBranchName(), branch.getBranchResource().getModel());
		this.resolver = resolver;
	}

	@Override
	public void handleCommit(Commit commit) {
		// whenever a resource is added the type of OntIndividual, we add it to the cache
		// rule/evaluation changes are handled by the rule repo etc.
		var newOrRemovedResources = new HashSet<String>(); // resources that are added do not need to be reloaded
		commit.getAddedStatements().stream()
			.filter(stmt ->  stmt.getPredicate().equals(RDF.type) &&	stmt.getObject().isResource() )
			.map(stmt -> processIfAboutAddedIndividual(stmt, newOrRemovedResources))
			.forEach(stmt -> processIfAboutAddedOwlClass(stmt, newOrRemovedResources));
		
		// just collecting removed resources, to avoid reloading/schema processing these
		commit.getRemovedStatements().stream()
			.filter(stmt ->  stmt.getPredicate().equals(RDF.type))
			.map(stmt -> processIfAboutRemovedIndividual(stmt, newOrRemovedResources))
			.forEach(stmt -> processIfAboutRemovedOwlClass(stmt, newOrRemovedResources));
		
			
		checkForMapPropertyChanges(commit, newOrRemovedResources);
		// now also check for artifact type changes (property removal/adding/changes)
		processAddedPropertySchemaChanges(commit, newOrRemovedResources);
		
	}

	private ContainedStatement processIfAboutAddedIndividual(ContainedStatement stmt, Set<String> affectedResources) {
		if (stmt.getResource().getURI().equals(OWL2.NamedIndividual.getURI())) {
			resolver.loadInstanceViaUpdate(stmt.getSubject().as(OntIndividual.class));
			affectedResources.add(stmt.getSubject().getURI());
		}
		return stmt;
	}
	
	private ContainedStatement processIfAboutRemovedIndividual(ContainedStatement stmt, Set<String> affectedResources) {
		if (stmt.getResource().getURI().equals(OWL2.NamedIndividual.getURI())) {
			affectedResources.add(stmt.getSubject().getURI());
		}
		return stmt;
	}
	
	private ContainedStatement processIfAboutAddedOwlClass(ContainedStatement stmt, Set<String> affectedResources) {
		if (stmt.getResource().getURI().equals(OWL2.Class.getURI())) {
			resolver.loadTypeViaUpdate(stmt.getSubject().as(OntClass.class));
			affectedResources.add(stmt.getSubject().getURI());
		}
		return stmt;
	}
	
	private ContainedStatement processIfAboutRemovedOwlClass(ContainedStatement stmt, Set<String> affectedResources) {
		if (stmt.getResource().getURI().equals(OWL2.Class.getURI())) {
			affectedResources.add(stmt.getSubject().getURI());
		}
		return stmt;
	}
	
	private void checkForMapPropertyChanges(Commit commit, Set<String> newResourceURIs) {
		// now also check for artifact map property updates (removals and additions)
		// check if its a contained property and if the property is a map entry reference property
		var affectedMapContainers = new HashSet<ContainedStatement>();
		commit.getAddedStatements().stream()
			.filter(stmt -> !newResourceURIs.contains(stmt.getContainerOrSubject().getURI())) // newly added resource can be ignored
			.filter(stmt -> !stmt.getContainmentPropertyOrPredicate().equals(stmt.getPredicate())) // map changes have different predicates than change itself
			.filter(stmt -> stmt.getContainmentPropertyOrPredicate().canAs(OntProperty.class)) // only if of expected property type
			.filter(stmt -> resolver.getMetaschemata().getMapType().isMapContainerReferenceProperty(stmt.getContainmentPropertyOrPredicate().as(OntProperty.class))) // its a map containment property
			.forEach(stmt -> affectedMapContainers.add(stmt)); // added to map
		commit.getRemovedStatements().stream()
		.filter(stmt -> !newResourceURIs.contains(stmt.getContainerOrSubject().getURI())) // completely removed resource does not need updating
		.filter(stmt -> !stmt.getContainmentPropertyOrPredicate().equals(stmt.getPredicate())) // map changes have different predicates than change itself
		.filter(stmt -> stmt.getContainmentPropertyOrPredicate().canAs(OntProperty.class)) // only if of expected property type
		.filter(stmt -> resolver.getMetaschemata().getMapType().isMapContainerReferenceProperty(stmt.getContainmentPropertyOrPredicate().as(OntProperty.class))) // its a map containment property
		.forEach(stmt -> affectedMapContainers.add(stmt)); // removed from map
		
		affectedMapContainers.stream()
			.map(stmt -> new ResourcePropertyTuple(stmt.getContainerOrSubject(), stmt.getContainmentPropertyOrPredicate()))
			.distinct() // to ensure that for each resource and map property we call reloading only once
			.forEach(stmt -> { 
				RDFElement el = resolver.findInstanceById(stmt.resource().getURI()).orElse(null); // instance
				if (el == null) { // or type
					el = resolver.findNonDeletedInstanceTypeByFQN(stmt.resource().getURI()).orElse(null);
				}
				if (el != null) {
					el.reloadMapProperty(el.getInstanceType().getPropertyType(stmt.property().getURI()));
				}
			});
	}
	
	private void processAddedPropertySchemaChanges(Commit commit, HashSet<String> newOrRemovedResourceURIs) {
		// we do not currently support changing of cardinality or domain of a property, (implicit supported only via removal of property within a commit, recreation differently in next commit)
		// also no changes to the type hierarchy supported (only workaround: complete removal of type hierarchy incl instances and recreation incl instances in different commits)
		commit.getAddedStatements().stream()
		.filter(stmt -> !newOrRemovedResourceURIs.contains(stmt.getContainerOrSubject().getURI())) // newly added resource can be ignored
		.filter(stmt -> stmt.getPredicate().equals(RDFS.domain)) // if a resource appears in the domain of a property --> property "added" to that class --> schema change
		.filter(stmt -> stmt.getObject().isResource()) // we are talking about a resource
		.map(stmt -> new ResourcePropertyTuple(stmt.getResource(), stmt.getPredicate()))
		.distinct()
		.forEach(entry -> { 
			var optType = resolver.findNonDeletedInstanceTypeByFQN(entry.resource().getURI());
			if (optType.isPresent()) {
				optType.get().addProperty(entry.property());
			}
		});
		// removed properties are handled in sibling ElementDeletedCacheUpdater
	}
	
	record ResourcePropertyTuple(Resource resource, Property property ) {};

	@Override
	protected String getServiceTypeURI() {
		return getWellknownServiceTypeURI();
	}

	public static String getWellknownServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+ElementCreatedCacheUpdater.class.getSimpleName();
	}
	
	
}

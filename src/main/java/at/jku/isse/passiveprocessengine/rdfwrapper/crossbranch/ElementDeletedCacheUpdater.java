package at.jku.isse.passiveprocessengine.rdfwrapper.crossbranch;

import java.util.HashSet;
import java.util.Set;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.crossbranch.ElementCreatedCacheUpdater.ResourcePropertyTuple;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Christoph Mayr-Dorn
 * 
 * @summary Ensures that artifacts that are newly introduced/added to this branch show up correctly in the Instance cache,
 * 
 * This handler needs to be put after the commit change applier to have access to predicate properties such as restrictions, super/sub properties, etc.
 * Always use in combination with {@link ElementCreatedCacheUpdater}
 * 
*/
@Slf4j
public class ElementDeletedCacheUpdater extends AbstractHandlerBase {
	
	private final NodeToDomainResolver resolver;
	
	public ElementDeletedCacheUpdater(Branch branch, NodeToDomainResolver resolver) {
		super("ArtifactDeletedCacheUpdaterFor"+branch.getBranchName(), branch.getBranchResource().getModel());
		this.resolver = resolver;
	}

	@Override
	public void handleCommit(Commit commit) {
		//TODO: check if there is a resource that is class and instance at the same time, whether it is then correctly handled twice!!
		
		// whenever a resource has the type of OntIndividual removed, we remove it from the cache. same for ontclasses
		// rule/evaluation changes are handled by the rule repo etc.
		var removedResources = new HashSet<String>(); // resources that are removed do not need to be reloaded
		commit.getRemovedStatements().stream()
		.filter(stmt ->  stmt.getPredicate().equals(RDF.type)
				&&	stmt.getObject().isResource() 
		)
		.map(stmt -> processIfAboutIndividual(stmt, removedResources))
		.forEach(stmt -> processIfAboutOwlClass(stmt, removedResources));
		
		processRemovedPropertySchemaChanges(commit, removedResources);
	}

	private ContainedStatement processIfAboutIndividual(ContainedStatement stmt, Set<String> affectedResources) {
		if (stmt.getResource().getURI().equals(OWL2.NamedIndividual.getURI())) {
			resolver.removeInstanceFromIndex(stmt.getContainerOrSubject().getURI());
			affectedResources.add(stmt.getSubject().getURI());
		}
		return stmt;
	}
	
	private ContainedStatement processIfAboutOwlClass(ContainedStatement stmt, Set<String> affectedResources) {
		if (stmt.getResource().getURI().equals(OWL2.Class.getURI())) {
			resolver.removeInstanceTypeFromIndex(stmt.getContainerOrSubject().getURI());
			affectedResources.add(stmt.getSubject().getURI());
		}
		return stmt;
	}
	
	private void processRemovedPropertySchemaChanges(Commit commit, Set<String> removedResourceURIs) {
		// we do not currently support changing of cardinality or domain of a property, (implicit supported only via removal of property within a commit, recreation differently in next commit)
		// also no changes to the type hierarchy supported (only workaround: complete removal of type hierarchy incl instances and recreation incl instances in different commits)
		commit.getRemovedStatements().stream()
		.filter(stmt -> !removedResourceURIs.contains(stmt.getContainerOrSubject().getURI())) // completely removed resources can be ignored
		.filter(stmt -> stmt.getPredicate().equals(RDFS.domain)) // if a resource no longer appears in the domain of a property --> property "removed" from that class --> schema change
		.filter(stmt -> stmt.getObject().isResource()) // we are talking about a resource
		.forEach(stmt -> { 
			var optType = resolver.findNonDeletedInstanceTypeByFQN(stmt.getResource().getURI());
			if (optType.isPresent()) {
				optType.get().removeProperty(stmt.getSubject().getURI());
			}
		});
		// added properties are handled in sibling ElementCreatedCacheUpdater
	}

	@Override
	protected String getServiceTypeURI() {
		return getWellknownServiceTypeURI();
	}

	public static String getWellknownServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+ElementDeletedCacheUpdater.class.getSimpleName();
	}
	
	
}

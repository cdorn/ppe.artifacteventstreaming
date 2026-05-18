package at.jku.isse.artifacteventstreaming.branch.incoming;

import at.jku.isse.artifacteventstreaming.api.*;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Christoph Mayr-Dorn
 * 
 * @summary Ensures that properties that are newly introduced/added to this branch show up correctly in the property cache,
 * Those properties that are new are put into general and potentially also map, list, xor single cache
 * 
 * This handler needs to be put after the commit change applier to have access to predicate properties such as restrictions, super/sub properties, etc.
 * Always use in combination with {@link PropertyDefinitionRemovedCacheUpdater}
 * 
 * Assumptions: 
 * branch-externally added properties do not exist in this branch, (we only update the cache, no update to the model)
*/
@Slf4j
public class PropertyDefinitionAddedCacheUpdater extends AbstractHandlerBase {

	private final CoreBranch branch;
	private final MetaModelSchemaTypes metaschema;
	
	public PropertyDefinitionAddedCacheUpdater(CoreBranch branch, MetaModelSchemaTypes metaschema) {
		super("PropertyDefinitionAddedCacheUpdatedFor"+branch.getBranchName(), branch.getBranchResource().getModel());
		this.branch = branch;
		this.metaschema = metaschema;
	}
	
	public PropertyDefinitionAddedCacheUpdater(String name, CoreBranch branch, MetaModelSchemaTypes metaschema) {
		super(name, branch.getBranchResource().getModel());
		this.branch = branch;
		this.metaschema = metaschema;
	}

	@Override
	public void handleCommit(Commit commit) {
		Set<ContainedStatement> domainAddedStmts = new HashSet<>();
		Set<ContainedStatement> propertiesAddedStmts = new HashSet<>();

		commit.getAddedStatements().stream()
				.forEach(stmt -> {
					// if property has domain stmt
					if (isPropertyOntClassDomainStatement(stmt)) {
						domainAddedStmts.add(stmt);
					} else if (isPropertyDefinition(stmt)) {
						propertiesAddedStmts.add(stmt);
					}
				});

		propertiesAddedStmts.stream()
				.filter(stmt -> !metaschema.getPrimaryPropertyType().getKnownPropertyURIs().contains(stmt.getSubject().getURI()))
				// only for new unknown properties
				.forEach(stmt -> {
					var propertyURI = stmt.getSubject().getURI();
					log.debug("Handling added property {} from commit {} applied to branch {} ", propertyURI, commit.getCommitId(), branch.getBranchId());
					var domains = domainsForProperty(propertyURI, domainAddedStmts);
					metaschema.syncCachesAfterRemotePropertyAdded(propertyURI, stmt.getSubject().getModel(), domains);
		});
	}

	private boolean isPropertyOntClassDomainStatement(ContainedStatement stmt) {
		return stmt.getObject().isResource()
				&& stmt.getPredicate().equals(RDFS.domain)
				&& stmt.getResource().getURI() != null;
	}

	public static boolean isPropertyDefinition(ContainedStatement stmt) {
		String uri = stmt.getResource().getURI();
		return stmt.getPredicate().equals(RDF.type)
				&&	stmt.getObject().isResource()
				&& ( uri.equals(RDF.Nodes.Property.getURI())
					|| uri.equals(OWL2.ObjectProperty.getURI())
					|| uri.equals(OWL2.DatatypeProperty.getURI()));
	}

	private Set<Resource> domainsForProperty(String propertyURI, Set<ContainedStatement> domainStatements) {
		return domainStatements.stream()
				.filter(stmt -> stmt.getSubject().getURI().equals(propertyURI))
				.map(Statement::getResource)
				.collect(Collectors.toSet());
	}

	public static String getWellknownServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+PropertyDefinitionAddedCacheUpdater.class.getSimpleName();
	}
	
	public static ServiceFactory getServiceFactory(MetaModelSchemaTypes metaschema) {
		return (branch, serviceConfigEntryPoint) -> {
			String name;
			Resource labelRes = serviceConfigEntryPoint.getPropertyResourceValue(RDFS.label);
			if (labelRes == null) {
				name = "PropertyDefinitionAddedCacheUpdatedFor"+branch.getBranchName();
			} else {
				name = labelRes.asLiteral().getString();
			}
			return new PropertyDefinitionAddedCacheUpdater(name, branch, metaschema);
		};
		
	}

	@Override
	protected String getServiceTypeURI() {
		return getWellknownServiceTypeURI();
	}
	
}

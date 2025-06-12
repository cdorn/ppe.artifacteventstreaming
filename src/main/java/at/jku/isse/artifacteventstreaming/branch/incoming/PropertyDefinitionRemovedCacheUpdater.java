package at.jku.isse.artifacteventstreaming.branch.incoming;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import lombok.extern.slf4j.Slf4j;

/**
 * @author Christoph Mayr-Dorn
 * 
 * @summary Ensures that properties that are newly introduced/added to this branch show up correctly in the property cache,
 * Those properties that are removed are also removed from general and potentially also map, list, xor single cache
 * 
 * This handler needs to be put before any change applier, as we need to be able to access existing properties for matching, 
 * otherwise: after applying of any removals, we wont have access to property URI etc.
 * Always use in combination with {@link PropertyDefinitionAddedCacheUpdater}
 *  
 * Assumptions: 
 * branch-externally removed properties are no longer needed in this branch (more like a syncing operation) (only update of the cache, no update of the model)
 */
@Slf4j

public class PropertyDefinitionRemovedCacheUpdater extends AbstractHandlerBase {

	private final Branch branch;
	private final MetaModelSchemaTypes metaschema;
	
	public PropertyDefinitionRemovedCacheUpdater(Branch branch, MetaModelSchemaTypes metaschema) {
		super("PropertyDefinitionRemovedCacheUpdatedFor"+branch.getBranchName(), branch.getBranchResource().getModel());
		this.branch = branch;
		this.metaschema = metaschema;
	}
	
	public PropertyDefinitionRemovedCacheUpdater(String name, Branch branch, MetaModelSchemaTypes metaschema) {
		super(name, branch.getBranchResource().getModel());
		this.branch = branch;
		this.metaschema = metaschema;
	}

	@Override
	public void handleCommit(Commit commit) {		
		commit.getRemovedStatements().stream()
		.filter(stmt ->  stmt.getPredicate().equals(RDF.type)
						&&	stmt.getObject().isResource() 
						&& PropertyDefinitionAddedCacheUpdater.isAboutProperty(stmt.getResource().getURI())
				)
		.map(stmt -> stmt.getSubject().getURI())
		.filter(uri -> metaschema.getPrimaryPropertyType().getKnownPropertyURIs().contains(uri))
		.forEach(uri -> handleRemovedProperty(uri, commit)); 	
	}
	
	
	private void handleRemovedProperty(String propertyURI, Commit commit) {
		log.debug(String.format("Handling removed property %s from commit %s applied to branch %s ", propertyURI, commit.getCommitId(), branch.getBranchId()));
		metaschema.removeURIfromCaches(propertyURI);
	}

	public static String getWellknownServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+PropertyDefinitionRemovedCacheUpdater.class.getSimpleName();
	}
	
	public static ServiceFactory getServiceFactory(MetaModelSchemaTypes metaschema) {
		return (branch, serviceConfigEntryPoint) -> {
			String name;
			Resource labelRes = serviceConfigEntryPoint.getPropertyResourceValue(RDFS.label);
			if (labelRes == null) {
				name = "PropertyDefinitionRemovedCacheUpdatedFor"+branch.getBranchName();
			} else {
				name = labelRes.asLiteral().getString();
			}
			return new PropertyDefinitionRemovedCacheUpdater(name, branch, metaschema);
		};
		
	}

	@Override
	protected String getServiceTypeURI() {
		return getWellknownServiceTypeURI();
	}
	
}

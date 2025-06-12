package at.jku.isse.artifacteventstreaming.branch.incoming;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.OWL2;
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

	private final Branch branch;
	private final MetaModelSchemaTypes metaschema;
	
	public PropertyDefinitionAddedCacheUpdater(Branch branch, MetaModelSchemaTypes metaschema) {
		super("PropertyDefinitionAddedCacheUpdatedFor"+branch.getBranchName(), branch.getBranchResource().getModel());
		this.branch = branch;
		this.metaschema = metaschema;
	}
	
	public PropertyDefinitionAddedCacheUpdater(String name, Branch branch, MetaModelSchemaTypes metaschema) {
		super(name, branch.getBranchResource().getModel());
		this.branch = branch;
		this.metaschema = metaschema;
	}

	@Override
	public void handleCommit(Commit commit) {		
		commit.getAddedStatements().stream()
		.filter(stmt ->  stmt.getPredicate().equals(RDF.type)
				&&	stmt.getObject().isResource() 
				&& isAboutProperty(stmt.getResource().getURI())
		)
		.filter(stmt -> !metaschema.getPrimaryPropertyType().getKnownPropertyURIs().contains(stmt.getSubject().getURI()))
		.forEach(stmt -> { 
			var propertyURI = stmt.getSubject().getURI();
			log.debug(String.format("Handling added property %s from commit %s applied to branch %s ", propertyURI, commit.getCommitId(), branch.getBranchId()));
			metaschema.addURItoCaches(propertyURI, stmt.getSubject().getModel());
			
		}); 	
	}

	public static boolean isAboutProperty(String uri) {
		return uri.equals(RDF.Nodes.Property.getURI())
				|| uri.equals(OWL2.ObjectProperty.getURI())
				|| uri.equals(OWL2.DatatypeProperty.getURI());
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

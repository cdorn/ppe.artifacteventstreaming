package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import org.apache.jena.ontapi.model.OntIndividual;

public interface ServiceFactory {

	CommitHandler getCommitHandlerInstanceFor(CoreBranch branch, OntIndividual serviceConfigEntryPoint) throws BranchConfigurationException;
	
	}

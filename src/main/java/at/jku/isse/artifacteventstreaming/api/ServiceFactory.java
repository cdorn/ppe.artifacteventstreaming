package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import org.apache.jena.ontapi.model.OntIndividual;

public interface ServiceFactory {

	public CommitHandler getCommitHandlerInstanceFor(Branch branch, OntIndividual serviceConfigEntryPoint) throws BranchConfigurationException;
	
	}

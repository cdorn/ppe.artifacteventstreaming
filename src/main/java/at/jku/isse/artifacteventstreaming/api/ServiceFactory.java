package at.jku.isse.artifacteventstreaming.api;

import org.apache.jena.ontapi.model.OntIndividual;

import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;

public interface ServiceFactory {

	public CommitHandler getCommitHandlerInstanceFor(Branch branch, OntIndividual serviceConfigEntryPoint) throws BranchConfigurationException;
	
	}

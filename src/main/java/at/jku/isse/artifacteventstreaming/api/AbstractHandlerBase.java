package at.jku.isse.artifacteventstreaming.api;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.vocabulary.RDFS;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class AbstractHandlerBase implements CommitHandler {
	
	protected final String serviceName;
	protected final OntModel repoModel;
	protected OntIndividual config;
	
	protected abstract String getServiceTypeURI();
	
	/**
	 * provides a default configuration resource describing the service type and having a label set to the service name
	 */
	@Override
	public OntIndividual getConfigResource() {
		if (config == null) {
			OntClass.Named handlerConfig = repoModel.getOntClass(AES.commitHandlerConfigType);
			config = handlerConfig.createIndividual(AES.getURI()+this.getClass().getSimpleName()+"#"+serviceName);
			config.addProperty(AES.isConfigForHandlerType, repoModel.createResource(getServiceTypeURI()));
			config.addLabel(serviceName);
		}
		return config;
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName()+" [name=" + serviceName + "]";
	}
	
	public void clearConfig() {
		config.removeProperties();
	}
	
	public void logIncomingCommit(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		var addDiff = commit.getAdditionCount() - indexOfNewAddition;
		var removeDiff = commit.getRemovalCount() - indexOfNewRemoval;		
		log.debug(String.format("%s called for %s with offsets %s (+%s) and %s (+%s) ", serviceName, commit.getCommitMessage(), indexOfNewAddition, addDiff, indexOfNewRemoval, removeDiff));
	}
}

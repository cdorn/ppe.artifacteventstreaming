package at.jku.isse.artifacteventstreaming.api;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

@Slf4j
public abstract class AbstractHandlerBase implements CommitHandler {
	
	protected final String serviceName;
    protected String uri;
	protected final OntModel repoModel;
	protected OntIndividual config;

    protected AbstractHandlerBase(String serviceName, OntModel repoModel) {
        this.serviceName = serviceName;
        this.repoModel = repoModel;
        var ns = AES.getURI().substring(0, AES.getURI().length()-1);
        uri = ns+"/"+this.getClass().getSimpleName()+"#"+serviceName;
    }

    protected abstract String getServiceTypeURI();




	/**
	 * provides a default configuration resource describing the service type and having a label set to the service name
	 */
	@Override
	public OntIndividual getConfigResource() {

        if (config == null) {
			OntClass.Named handlerConfig = repoModel.getOntClass(AES.commitHandlerConfigType);
            config = handlerConfig.createIndividual(uri);
			config.addProperty(AES.isConfigForHandlerType, repoModel.createResource(getServiceTypeURI()));
			config.addLabel(serviceName);
		}
		return config;
	}

    @Override
    public String getURI() {
        return uri;
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
		log.debug("{} called for {} with offsets {} (+{}) and {} (+{}) ", serviceName, commit.getCommitMessage(), indexOfNewAddition, addDiff, indexOfNewRemoval, removeDiff);
	}
}

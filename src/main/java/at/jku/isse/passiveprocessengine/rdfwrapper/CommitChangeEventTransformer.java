package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.ontapi.model.OntModel;
import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.passiveprocessengine.core.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.core.ProcessInstanceChangeListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitChangeEventTransformer extends AbstractHandlerBase implements ChangeEventTransformer, CommitHandler {

	private final NodeToDomainResolver resolver;
	private ProcessInstanceChangeListener eventSink;
	
	public CommitChangeEventTransformer(String serviceName, OntModel repoModel, NodeToDomainResolver resolver) {
		super(serviceName, repoModel);
		this.resolver = resolver;
		//TODO: find a way to restore/retrieve eventSink upon restart
	}	
	
	@Override
	protected String getServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+this.getClass().getSimpleName();
	}
	
	@Override
	public void registerWithWorkspace(ProcessInstanceChangeListener eventSink) {
		this.eventSink = eventSink;
		
	}

	@Override
	public void handleCommit(Commit commit) {
		var session = new TransformationSession(commit, resolver);
		eventSink.handleUpdates(session.process());
	}

}

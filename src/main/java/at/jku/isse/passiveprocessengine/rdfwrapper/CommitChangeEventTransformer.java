package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.replay.CommitContainmentAugmenter;
import at.jku.isse.artifacteventstreaming.replay.PerResourceHistoryRepository;
import at.jku.isse.passiveprocessengine.core.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.core.ProcessInstanceChangeListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitChangeEventTransformer extends CommitContainmentAugmenter implements ChangeEventTransformer, IncrementalCommitHandler {

	ProcessInstanceChangeListener eventSink;
	public CommitChangeEventTransformer(String serviceName, OntModel repoModel, NodeToDomainResolver resolver, PerResourceHistoryRepository historyRepo) {
		super(serviceName, repoModel, resolver, historyRepo, resolver.getCardinalityUtil());
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
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		var addSize = commit.getAddedStatements().size();
		var remSize = commit.getRemovedStatements().size();
		var session = new TransformationSession(commit.getAddedStatements().subList(indexOfNewAddition, addSize)
				, commit.getRemovedStatements().subList(indexOfNewRemoval, remSize), resolver, this.historyRepo);
		try {
			session.process(commit.getCommitId(), commit.getOriginatingBranchId(), commit.getTimeStamp());
			if (eventSink != null) {
				eventSink.handleUpdates(session.getUpdates());
			}
		} catch (PersistenceException e) {
			
			throw new RuntimeException(e.getMessage());
		}
		
	}

}

package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.artifacteventstreaming.replay.CommitContainmentAugmenter;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.passiveprocessengine.core.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.core.ProcessInstanceChangeListener;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommitChangeEventTransformer extends CommitContainmentAugmenter implements ChangeEventTransformer, IncrementalCommitHandler {

	ProcessInstanceChangeListener eventSink;
	NodeToDomainResolver resolver;
	RuleSchemaProvider ruleSchema;
	
	public CommitChangeEventTransformer(String serviceName, OntModel repoModel, NodeToDomainResolver resolver, RuleSchemaProvider ruleSchema) {
		super(serviceName, repoModel, resolver.getCardinalityUtil());
		this.resolver = resolver;
		this.ruleSchema = ruleSchema;
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
		super.logIncomingCommit(commit, indexOfNewAddition, indexOfNewRemoval);
		var addSize = commit.getAddedStatements().size();
		var remSize = commit.getRemovedStatements().size();
		var session = new TransformationSession(
				commit.getAddedStatements().subList(indexOfNewAddition, addSize)
				, commit.getRemovedStatements().subList(indexOfNewRemoval, remSize)
				, resolver
				, this.ruleSchema);
		
			session.process();
			if (eventSink != null) {
				var updates = session.getUpdates();
				log.debug(String.format("%s resulted in %s higherlevel events", commit.getCommitMessage(), updates.size()));
				eventSink.handleUpdates(updates);
			}
		
		
	}

}

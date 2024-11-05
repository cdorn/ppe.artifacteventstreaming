package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import lombok.Getter;

public abstract class CommitLoggingService extends AbstractHandlerBase implements IncrementalCommitHandler {

	public CommitLoggingService(String serviceName, OntModel repoModel) {
		super(serviceName, repoModel);
	}

	public CommitLoggingService(OntModel branchModel) {
		super(UUID.randomUUID().toString(), branchModel);
	}
	
	@Getter final List<Commit> receivedCommits = new LinkedList<>();
	
	@Override
	public void handleCommit(Commit commit) {
		receivedCommits.add(commit);
	}
	
	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {		
		receivedCommits.add(commit);		
	}
}

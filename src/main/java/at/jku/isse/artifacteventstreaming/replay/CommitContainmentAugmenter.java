package at.jku.isse.artifacteventstreaming.replay;

import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.schemasupport.PropertyCardinalityTypes;
import at.jku.isse.passiveprocessengine.core.ChangeEventTransformer;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.TransformationSession;

public class CommitContainmentAugmenter extends AbstractHandlerBase implements IncrementalCommitHandler {

	protected final NodeToDomainResolver resolver;
	protected final PerResourceHistoryRepository historyRepo;
	protected final PropertyCardinalityTypes cardinalityUtils;

	public CommitContainmentAugmenter(String serviceName, OntModel repoModel, NodeToDomainResolver resolver,
			PerResourceHistoryRepository historyRepo, PropertyCardinalityTypes cardinalityUtils) {
		super(serviceName, repoModel);
		this.resolver = resolver;
		this.historyRepo = historyRepo;
		this.cardinalityUtils = cardinalityUtils;
	}

	@Override
	protected String getServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+this.getClass().getSimpleName();
	}

	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		var addSize = commit.getAddedStatements().size();
		var remSize = commit.getRemovedStatements().size();
		var session = new StatementAugmentationSession(commit.getAddedStatements().subList(indexOfNewAddition, addSize)
				, commit.getRemovedStatements().subList(indexOfNewRemoval, remSize), this.cardinalityUtils, this.historyRepo);
		try {
			session.process(commit.getCommitId(), commit.getOriginatingBranchId(), commit.getTimeStamp());
		} catch (PersistenceException e) {
			
			throw new RuntimeException(e.getMessage());
		}
		
	}

}
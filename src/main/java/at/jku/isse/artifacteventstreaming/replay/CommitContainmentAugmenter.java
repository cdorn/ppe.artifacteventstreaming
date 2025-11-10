package at.jku.isse.artifacteventstreaming.replay;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import at.jku.isse.artifacteventstreaming.api.ServiceFactoryRegistry;
import at.jku.isse.artifacteventstreaming.branch.outgoing.CommitToHistoryHandler;
import at.jku.isse.artifacteventstreaming.branch.outgoing.CommitToHistoryHandler.DefaultServiceFactory;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

public class CommitContainmentAugmenter extends AbstractHandlerBase implements IncrementalCommitHandler {

	protected final MetaModelSchemaTypes cardinalityUtils;

	public CommitContainmentAugmenter(@NonNull String serviceName, @NonNull OntModel repoModel,
			@NonNull MetaModelSchemaTypes cardinalityUtils) {
		super(serviceName, repoModel);
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
				, commit.getRemovedStatements().subList(indexOfNewRemoval, remSize), this.cardinalityUtils);
			session.process();
	}
	
	@RequiredArgsConstructor
	public static class DefaultServiceFactory implements ServiceFactory {

		public void registerSelfIn(ServiceFactoryRegistry registry) {
			registry.register(CommitHandler.serviceTypeBaseURI+CommitContainmentAugmenter.class.getSimpleName(), this);
		}
		
		@Override
		public CommitHandler getCommitHandlerInstanceFor(Branch sourceBranch
				, OntIndividual serviceConfigEntryPoint
				) {
			// obtain metamodel schema						
			var service = new CommitContainmentAugmenter(serviceConfigEntryPoint.getLabel(), sourceBranch.getBranchResource().getModel(), sourceBranch.getSchemaUtils());
			return service;
		}
		
	}
}
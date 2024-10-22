package at.jku.isse.artifacteventstreaming.branch.incoming;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchInternalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CompleteCommitMerger implements CommitHandler {

	private final Branch branch;
	
	@Override
	public void handleCommit(Commit commit) {		
		if (!commit.isEmpty()) {
			log.debug(String.format("About to apply commit %s with %s additions and %s removals to branch %s", commit.getCommitId(), commit.getAdditionCount(), commit.getRemovalCount(), branch.getBranchId()));
			branch.getModel().remove(commit.getRemovedStatements()); //first removal, then adding
			branch.getModel().add(commit.getAddedStatements());
			log.debug(String.format("Applied commit %s to branch %s", commit.getCommitId(), branch.getBranchId()));
		}		
	}

	@Override
	public OntIndividual getConfigResource() {
		OntModel repoModel = branch.getBranchResource().getModel();
		OntIndividual config = repoModel.createIndividual(branch.getBranchId()+"#"+this.getClass().getSimpleName());
		config.addProperty(AES.isConfigForServiceType, repoModel.createResource(getServiceTypeURI()));
		// no other config necessary
		return config;
	}

	public static String getServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+CompleteCommitMerger.class.getSimpleName();
	}
	
	public static ServiceFactory getServiceFactory() {
		return new ServiceFactory() {
			@Override
			public CommitHandler getCommitHandlerInstanceFor(Branch branch, OntIndividual serviceConfigEntryPoint) {
				// simple, as we dont have any config to do
				return new CompleteCommitMerger(branch);
			}
		};
	}
	
}

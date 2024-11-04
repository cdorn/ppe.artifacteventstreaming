package at.jku.isse.artifacteventstreaming.branch.outgoing;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class OnDemandCatchUpStreamer implements CommitHandler {

	private final Branch sourceBranch;
	private final Branch destinationBranch;
	
	// reads persisted commits from source branch
	// reads cache entry from desitantionbranch latest merge
	// obtains all commit since last merge
	// appends them to inqueue of destination branch
	// finishes
	
	
	@Override
	public void handleCommit(Commit commit) {
		try {
			destinationBranch.enqueueIncomingCommit(commit);
		} catch (Exception e) {
			log.warn(String.format("Error enqueuing commit %s  from branch %s to branch %s", commit.getCommitId(), sourceBranch.getBranchId(), destinationBranch.getBranchId()));
			//TODO: retry later necessary
		}
	}
	
	@Override
	public OntIndividual getConfigResource() {
		OntModel repoModel = sourceBranch.getBranchResource().getModel();
		OntIndividual config = repoModel.createIndividual(sourceBranch.getBranchId()+"#"+this.getClass().getSimpleName());
		config.addProperty(AES.isConfigForHandlerType, repoModel.createResource(CommitHandler.serviceTypeBaseURI+this.getClass().getSimpleName()));
		config.addProperty(AES.destinationBranch, destinationBranch.getBranchResource().getURI());
		// no other config necessary
		return config;
	}

}

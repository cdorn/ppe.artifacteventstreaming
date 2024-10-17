package at.jku.isse.artifacteventstreaming.rdf;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
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

}

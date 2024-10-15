package at.jku.isse.artifacteventstreaming.rdf;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CompleteCommitMerger implements CommitHandler {

	private final Branch branch;
	private final Dataset dataset;
	
	@Override
	public void handleCommit(Commit commit) {
		log.debug(String.format("About to apply commit %s to branch %s", commit.getCommitId(), branch.getBranchId()));
		if (!commit.getAddedStatements().isEmpty() || !commit.getRemovedStatements().isEmpty()) {
			branch.getModel().remove(commit.getRemovedStatements()); //first removal, then adding
			branch.getModel().add(commit.getAddedStatements());			
		}
		log.debug(String.format("Applied commit %s to branch %s", commit.getCommitId(), branch.getBranchId()));
	}

}

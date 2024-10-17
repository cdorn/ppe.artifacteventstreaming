package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.BranchInternalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.Commit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class AllUndoService implements BranchInternalCommitHandler {
	
	final String serviceName;
	Set<Statement> seenStatements = new HashSet<>();
	
	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		// here we just look at additions
		log.debug(String.format("%s : called with add offset %s and remove offset %s ", serviceName, indexOfNewAddition, indexOfNewRemoval));
		
		List<Statement> removals = commit.getRemovedStatements();
		if (indexOfNewRemoval >= removals.size()) {
			log.debug("no removed statements");

		} else {		
			do {
				Statement stmt = removals.get(indexOfNewRemoval);			
				// here we just always remove an addition, and add a removal to effectively undo any prior change 
				stmt.getModel().add(stmt);			
				indexOfNewRemoval++;
			} while (indexOfNewRemoval < removals.size());
		}
		List<Statement> additions = commit.getAddedStatements();
		if (indexOfNewAddition >= additions.size()) {
			log.debug("no added statements");

		} else {		
			do {
				Statement stmt = additions.get(indexOfNewAddition);			
				// here we just always remove an addition, and add a removal to effectively undo any prior change 
				stmt.getModel().remove(stmt);			
				indexOfNewAddition++;
			} while (indexOfNewAddition < additions.size());
		}
	}

	@Override
	public String toString() {
		return "AllUndoService [serviceName=" + serviceName + "]";
	}
	
	

}

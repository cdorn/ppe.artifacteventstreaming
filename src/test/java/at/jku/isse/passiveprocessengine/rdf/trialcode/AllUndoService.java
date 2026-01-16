package at.jku.isse.passiveprocessengine.rdf.trialcode;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Statement;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class AllUndoService extends AbstractHandlerBase implements IncrementalCommitHandler {
	
	public static final String SERVICE_TYPE_URI = CommitHandler.serviceTypeBaseURI+AllUndoService.class.getSimpleName();

	Set<Statement> seenStatements = new HashSet<>();

    public AllUndoService(String serviceName, OntModel repoModel) {
        super(serviceName, repoModel);
    }


    @Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		// here we just look at additions
		log.debug(String.format("%s : called with add offset %s and remove offset %s ", serviceName, indexOfNewAddition, indexOfNewRemoval));
		
		List<Statement> removals = commit.getRemovedStatements().stream().map(Statement.class::cast).toList();
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
		List<Statement> additions = commit.getAddedStatements().stream().map(Statement.class::cast).toList();
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

    @Override
    protected String getServiceTypeURI() {
        return SERVICE_TYPE_URI;
    }


	
	

}

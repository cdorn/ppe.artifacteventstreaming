package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Statement;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class AllUndoService implements IncrementalCommitHandler {
	
	public static final String SERVICE_TYPE_URI = CommitHandler.serviceTypeBaseURI+AllUndoService.class.getSimpleName();
	
	
	final String serviceName;
	Set<Statement> seenStatements = new HashSet<>();
	final OntModel model;
	
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

	@Override
	public OntIndividual getConfigResource() {
		OntIndividual config =  model.createIndividual(AES.getURI()+this.getClass().getSimpleName());
		config.addProperty(AES.isConfigForServiceType, model.createResource(SERVICE_TYPE_URI));
		return config;
	}
	
	

}

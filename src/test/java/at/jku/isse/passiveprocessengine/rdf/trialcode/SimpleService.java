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
public class SimpleService implements BranchInternalCommitHandler {
	
	final String serviceName;
	final boolean doNegToPos;
	Set<Statement> seenStatements = new HashSet<>();
	
	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		// here we just look at additions
		log.debug(serviceName + ": called with offset "+indexOfNewAddition);
		
		List<Statement> additions = commit.getAddedStatements();
		if (indexOfNewAddition >= additions.size()) {
			log.debug("no added statements");
			return;
		}
		
		do {
			Statement stmt = additions.get(indexOfNewAddition);
			assert(!seenStatements.contains(stmt));
			// here we just always extract the subject and get its label property if available
			Statement prop = stmt.getSubject().getProperty(RDFS.label);
			if (prop != null) {
				int labelAsInt = prop.getInt();
				int oldLabel = labelAsInt;
				Integer newLabel = Integer.valueOf(0);
				if (labelAsInt < 0 && doNegToPos) {
					labelAsInt--;
					newLabel = labelAsInt * -1;
				} else 
				if (labelAsInt > 0 && !doNegToPos) {
					labelAsInt++;
					newLabel = labelAsInt * -1;
				}
				if (newLabel != 0) {
					log.debug(serviceName + ": Label is set from "+oldLabel+ " to "+newLabel);
					prop.changeLiteralObject(newLabel);
				}
			}
			seenStatements.add(stmt);
			indexOfNewAddition++;
		} while (indexOfNewAddition < additions.size());
	}

}

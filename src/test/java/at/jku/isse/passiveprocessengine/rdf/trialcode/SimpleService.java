package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SimpleService extends CommitLoggingService {
	
	final boolean doNegToPos;
	Set<Statement> seenStatements = new HashSet<>();
	int maxIter = 10;
	int currentIter = 0;
	
	public SimpleService(String serviceName, boolean doNegToPos, OntModel branchModel
			) {
		super(serviceName, branchModel);
		this.doNegToPos = doNegToPos;
	}
	
	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		super.handleCommit(commit);
		// here we just look at additions
		log.debug(serviceName + ": called with offset "+indexOfNewAddition);
		
		List<Statement> additions = commit.getAddedStatements().stream().map(Statement.class::cast).toList();
		if (indexOfNewAddition >= additions.size()) {
			log.debug("no added statements");
			return;
		}
		
		if (currentIter >= 10) {
			log.debug(serviceName +" reached maximum iterations, resetting");
			currentIter = 0;
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
		currentIter++;
	}

	@Override
	protected String getServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+this.getClass().getSimpleName();
	}

}

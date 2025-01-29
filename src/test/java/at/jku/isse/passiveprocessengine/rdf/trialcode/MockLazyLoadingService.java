package at.jku.isse.passiveprocessengine.rdf.trialcode;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockLazyLoadingService extends CommitLoggingService {
	
	final boolean doAddOrRemove;
	final Set<Statement> seenStatements = new HashSet<>();
	final int maxIter;
	int currentIter = 0;
	final OntModel model;
	
	public MockLazyLoadingService(String serviceName, boolean doAddOrRemove, OntModel repoModel
			, OntModel branchModel
			, Integer maxIter
			) {
		super(serviceName, repoModel);
		this.doAddOrRemove = doAddOrRemove;
		this.model = branchModel;
		this.maxIter = maxIter;
	}
	
	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		super.handleCommit(commit);
		// here we just look at additions
		log.debug(serviceName + ": called with offsets "+indexOfNewAddition+" "+indexOfNewRemoval);
		
		if (currentIter >= maxIter) {
			log.debug(serviceName +" reached maximum iterations, resetting");
			currentIter = 0;
			return;
		}
		if (doAddOrRemove) {
			// insert lazyloading flag
			log.debug(serviceName+" adding "+currentIter);
			model.addLiteral(createResourceFromId("LAZYLOADINGFLAG"), RDFS.label, currentIter);
		} else { // remove lazyloading flag
			log.debug(serviceName+" removing "+currentIter);
			var stmt = model.createLiteralStatement(createResourceFromId("LAZYLOADINGFLAG"), RDFS.label, currentIter);
			model.remove(stmt);
		}
		currentIter++;
		
	}

	@Override
	protected String getServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+this.getClass().getSimpleName();
	}
	
	private Resource createResourceFromId(String id) {
		if (isValidURL(id)) {
			return model.createResource(id);
		} else {
			return model.createResource(AnonId.create(id));
		}
	}
	
	boolean isValidURL(String url)  {
	    try {
	        new URI(url);	    		    
	        return true;	   
	    } catch (URISyntaxException e) {
	        return false;
	    }
	}
}

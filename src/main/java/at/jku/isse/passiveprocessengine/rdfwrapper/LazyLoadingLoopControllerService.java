package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LazyLoadingLoopControllerService extends AbstractHandlerBase implements IncrementalCommitHandler {
	
	public static final String LOOPFLAG_URI = "http://at.jku.isse.artifacteventstreaming#LazyLoadingLoopFlag";
	private final OntModel model;
	private static final Resource flag = ResourceFactory.createResource(LOOPFLAG_URI);
	
	public LazyLoadingLoopControllerService(String serviceName, OntModel repoModel
			, OntModel branchModel
			) {
		super(serviceName, repoModel);
		this.model = branchModel;

	}
	
	@Override
	public void handleCommit(Commit commit) {
		handleCommitFromOffset(commit, 0, 0);
	}

	@Override
	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval) {
		// here we just need to look at Lazy Loading Flag additions, and remove them again
		var addSize = commit.getAddedStatements().size();
		commit.getAddedStatements().subList(indexOfNewAddition, addSize).stream()
			.filter(stmt -> flag.equals(stmt.getSubject()) && stmt.getPredicate().equals(RDFS.label))
			.forEach(model::remove);				
	}

	@Override
	protected String getServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+this.getClass().getSimpleName();
	}
	
	public static void insertLoopFlag(OntModel model, int loopIterationCounter) {
		model.addLiteral(flag, RDFS.label, loopIterationCounter);
	}
}

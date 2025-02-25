package at.jku.isse.artifacteventstreaming.branch.incoming;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.AbstractHandlerBase;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j

public class CompleteCommitMerger extends AbstractHandlerBase {

	private Branch branch;
	
	public CompleteCommitMerger(Branch branch) {
		super("MergerFor"+branch.getBranchName(), branch.getBranchResource().getModel());
		this.branch = branch;
	}
	
	public CompleteCommitMerger(String name, Branch branch) {
		super(name, branch.getBranchResource().getModel());
		this.branch = branch;
	}

	@Override
	public void handleCommit(Commit commit) {		
		if (!commit.isEmpty()) {
			log.debug(String.format("About to apply commit %s with %s additions and %s removals to branch %s", commit.getCommitId(), commit.getAdditionCount(), commit.getRemovalCount(), branch.getBranchId()));
			branch.getModel().remove(commit.getRemovedStatements().stream().map(Statement.class::cast).toList()); //first removal, then adding
			branch.getModel().add(commit.getAddedStatements().stream().map(Statement.class::cast).toList());
			log.debug(String.format("Applied commit %s to branch %s", commit.getCommitId(), branch.getBranchId()));
		}		
	}

	public static String getWellknownServiceTypeURI() {
		return CommitHandler.serviceTypeBaseURI+CompleteCommitMerger.class.getSimpleName();
	}
	
	public static ServiceFactory getServiceFactory() {
		return (branch, serviceConfigEntryPoint) -> {
			String name;
			Resource labelRes = serviceConfigEntryPoint.getPropertyResourceValue(RDFS.label);
			if (labelRes == null) {
				name = "MergerFor"+branch.getBranchName();
			} else {
				name = labelRes.asLiteral().getString();
			}
			return new CompleteCommitMerger(name, branch);
		};
		
	}

	@Override
	protected String getServiceTypeURI() {
		return getWellknownServiceTypeURI();
	}
	
}

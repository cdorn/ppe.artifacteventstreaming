package at.jku.isse.artifacteventstreaming.rdf.distribution;

import java.net.URI;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchInternalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.ServiceFactory;
import at.jku.isse.artifacteventstreaming.rdf.BranchRepository;
import at.jku.isse.artifacteventstreaming.rdf.CompleteCommitMerger;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class DefaultDirectBranchCommitStreamer implements CommitHandler {

	public static final String SERVICE_TYPE_URI = CommitHandler.serviceTypeBaseURI+DefaultDirectBranchCommitStreamer.class.getSimpleName();
	
	private final Branch sourceBranch;
	private final Branch destinationBranch;
	
	@Override
	public void handleCommit(Commit commit) {
		destinationBranch.enqueueIncomingCommit(commit);
	}

	@Override
	public OntIndividual getConfigResource() {
		OntModel repoModel = sourceBranch.getBranchResource().getModel();
		OntIndividual config = repoModel.createIndividual(sourceBranch.getBranchId()+"#"+this.getClass().getSimpleName());
		config.addProperty(AES.isConfigForServiceType, repoModel.createResource(SERVICE_TYPE_URI));
		config.addProperty(AES.destinationBranch, repoModel.getResource( destinationBranch.getBranchResource().getURI()));
		// no other config necessary
		return config;
	}
	
	
	
	public static ServiceFactory getServiceFactory(BranchRepository branchRepo) {
		if (factory == null) {
			factory = new DefaultServiceFactory(branchRepo);
		}
		return factory;
	}
	
	private static DefaultServiceFactory factory = null;
	
	@RequiredArgsConstructor
	public static class DefaultServiceFactory implements ServiceFactory {
		
		private final BranchRepository branchRepo;
		
		@Override
		public CommitHandler getCommitHandlerInstanceFor(Branch sourceBranch, OntIndividual serviceConfigEntryPoint) throws Exception {
			// simple, as we dont have any config to do
			Resource destBranchRes = serviceConfigEntryPoint.getPropertyResourceValue(AES.destinationBranch);
			Branch destBranch = branchRepo.getOrLoadBranch(URI.create(destBranchRes.getURI()));
			return new DefaultDirectBranchCommitStreamer(sourceBranch, destBranch);
		}
		
	}
}

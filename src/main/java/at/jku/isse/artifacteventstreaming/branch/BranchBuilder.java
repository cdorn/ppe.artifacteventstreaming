package at.jku.isse.artifacteventstreaming.branch;

import java.net.URI;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchInternalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryEventStore;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryStateKeeperFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import lombok.NonNull;

public class BranchBuilder {

	private BranchStateUpdater stateKeeper;
	private URI repositoryURI;
	private String branchName = "main";
	private Dataset branchDataset;
	private final Dataset repoDataset;
	private OntModel repoModel;
	private List<CommitHandler> incomingCommitHandlers = new LinkedList<>();
	private List<BranchInternalCommitHandler> services = new LinkedList<>();
	private Set<CommitHandler> outgoingCommitDistributers = new HashSet<>();
	
	public BranchBuilder(@NonNull URI repositoryURI, @NonNull Dataset repoDataset, @NonNull OntModel repoModel) {
		this.repositoryURI = repositoryURI;
		this.repoDataset = repoDataset;	
		this.repoModel = repoModel;
	}
	
	public BranchBuilder(@NonNull URI repositoryURI, @NonNull Dataset repoDataset) {
		this.repositoryURI = repositoryURI;
		this.repoDataset = repoDataset;	
		this.repoModel = null;
	}
	
	/**
	 * if not used, by default the 'main' branch will be created.
	 */
	public BranchBuilder setBranchLocalName(@NonNull String branchName) {
		if (branchName.length() == 0) {
			throw new RuntimeException("Branchname cannot be empty");
		}
		this.branchName = branchName;
		return this;
	}
	
	/**
	 * if not used, by default a in memory statekeeper will be used.
	 */
	public BranchBuilder setStateKeeper(@NonNull BranchStateUpdater stateKeeper) {
		this.stateKeeper = stateKeeper;
		return this;
	}
	
	/**
	 * if not used, by default a in memory dataset will be created.
	 */
	public BranchBuilder setDataset(@NonNull Dataset dataset) {
		this.branchDataset = dataset;
		return this;
	}

	/**
	 * if not used, no commits will be merged into this branch
	 */
	public BranchBuilder addIncomingCommitHandler(CommitHandler handler) {
		this.incomingCommitHandlers.add(handler);
		return this;
	}
	
	/**
	 * if not used, no services will be invoked for any commits.
	 */
	public BranchBuilder addBranchInternalCommitHandler(BranchInternalCommitHandler service) {
		this.services.add(service);
		return this;
	}
	
	public BranchBuilder addOutgoingCommitDistributer(CommitHandler distributer) {
		this.outgoingCommitDistributers.add(distributer);
		return this;
	}
	
	
	public static String generateBranchURI(URI repositoryURI, String branchName) {
		return Objects.toString(repositoryURI)+"#"+branchName;
	}
	
	public static String getBranchNameFromURI(@NonNull URI branchURI) {
		int pos = branchURI.toString().lastIndexOf("#");
		if (pos < 0) {
			return null;
		} else {
			return branchURI.toString().substring(pos+1);
		}
	}
	
	public Branch build() throws Exception {
		if (branchDataset == null) {
			setDataset(DatasetFactory.createTxnMem());
		}
		String branchURI = generateBranchURI(repositoryURI, branchName);
		OntIndividual branchResource = prepareBranch(branchURI);
		BlockingQueue<Commit> inQueue = new LinkedBlockingQueue<>();
		BlockingQueue<Commit> outQueue = new LinkedBlockingQueue<>();
		// we init the statekeeper before the branch to avoid triggering the services --> not anymore with separate startHandlers method
		// stateKeeper.loadState(model); only upon setting up all services, not the duty of the builder
		branchDataset.begin();
		OntModel model = OntModelFactory.createModel(branchDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		if (stateKeeper == null) {
			stateKeeper = new StateKeeperImpl(URI.create(branchURI), new InMemoryBranchStateCache(), new InMemoryEventStore());
		}
		BranchImpl branch = new BranchImpl(branchDataset, model, branchResource, stateKeeper, inQueue, outQueue);
		addCommitHandlers(branch);
		return branch;
	}
	
	private OntIndividual prepareBranch(String branchURI) {
		Resource branchRes = ResourceFactory.createResource(branchURI);
		Resource repoRes = ResourceFactory.createResource(repositoryURI.toString());
		OntIndividual branchResource = null;
		repoDataset.begin();
		if (repoModel == null)
			repoModel = OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		if (repoModel.contains(branchRes, AES.partOfRepository, repoRes)) {
			branchResource = repoModel.createIndividual(branchURI);		
		} else { // we assume, each branch has its own model, hence we create the core concepts here as well
			addCoreConcepts(repoModel);
			branchResource = buildBranchResource(repositoryURI, repoModel, branchName);
		}
		repoDataset.commit();
		repoDataset.end();
		return branchResource;
	}
	
	private static void addCoreConcepts(OntModel model) {
		OntClass.Named repoType = model.createOntClass(AES.repositoryType);
		OntClass.Named branchType = model.createOntClass(AES.branchType);
		OntObjectProperty.Named partOfRepo = model.createObjectProperty(AES.partOfRepository.getURI());
		partOfRepo.addDomain(branchType);
		partOfRepo.addRange(repoType);
		partOfRepo.addLabel("part of repository");
	}
	
	private static OntIndividual buildBranchResource(URI repositoryURI, OntModel initializedModel, String branchName) {
		OntClass.Named branchType = initializedModel.getOntClass(AES.branchType);
		OntObjectProperty.Named partOfRepo = initializedModel.getObjectProperty(AES.partOfRepository.getURI());
		OntIndividual branch = branchType.createIndividual(generateBranchURI(repositoryURI, branchName));
		Resource repo = ResourceFactory.createResource(repositoryURI.toString());
		branch.addLabel(branchName);
		branch.addProperty(partOfRepo, repo);
		return branch;
	}
	
	
	private void addCommitHandlers(Branch branch) {
		incomingCommitHandlers.stream().forEach(handler -> branch.appendIncomingCommitHandler(handler));
		services.stream().forEach(service -> branch.appendCommitService(service));		
		outgoingCommitDistributers.stream().forEach(service -> branch.appendOutgoingCrossBranchCommitHandler(service));

	}
	
	public static boolean doesDatasetContainBranch(Dataset dataset, URI repositoryURI, String branchName) {
		dataset.begin();
		String branchURI = generateBranchURI(repositoryURI, branchName);
		Resource branchRes = ResourceFactory.createResource(branchURI);
		Resource repoRes = ResourceFactory.createResource(repositoryURI.toString());
		boolean doesContain = dataset.getDefaultModel().contains(branchRes, AES.partOfRepository, repoRes);
		dataset.end();
		return doesContain;
	}
}

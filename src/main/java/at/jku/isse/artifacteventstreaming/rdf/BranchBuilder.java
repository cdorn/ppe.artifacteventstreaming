package at.jku.isse.artifacteventstreaming.rdf;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
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
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.api.BranchInternalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.StateKeeper;
import lombok.NonNull;

public class BranchBuilder {

	private StateKeeper stateKeeper = new InMemoryStateKeeper();
	private URI repositoryURI;
	private String branchName = "main";
	private Dataset dataset;
	private OntModel model;
	private List<CommitHandler> incomingCommitHandlers = new LinkedList<>();
	private List<BranchInternalCommitHandler> services = new LinkedList<>();
	
	public BranchBuilder(@NonNull URI repositoryURI) {
		this.repositoryURI = repositoryURI;
	}
	
	/**
	 * if not used, by default the 'main' branch will be created.
	 */
	public BranchBuilder setBranch(@NonNull String branchName) {
		this.branchName = branchName;
		return this;
	}
	
	/**
	 * if not used, by default a in memory statekeeper will be used.
	 */
	public BranchBuilder setStateKeeper(@NonNull StateKeeper stateKeeper) {
		this.stateKeeper = stateKeeper;
		return this;
	}
	
	/**
	 * if not used, by default a in memory dataset will be created.
	 */
	public BranchBuilder setDataset(@NonNull Dataset dataset) {
		this.dataset = dataset;
		this.model = OntModelFactory.createModel(dataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
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
	
	public static String generateBranchURI(URI repositoryURI, String branchName) {
		return Objects.toString(repositoryURI)+"#"+branchName;
	}
	
	public Branch build() {
		if (dataset == null) {
			setDataset(DatasetFactory.createTxnMem());
		}
		dataset.begin(ReadWrite.WRITE);
		String branchURI = generateBranchURI(repositoryURI, branchName);
		Resource branchRes = ResourceFactory.createResource(branchURI);
		Resource repoRes = ResourceFactory.createResource(repositoryURI.toString());
		OntIndividual branchResource = null;
		if (model.contains(branchRes, AES.partOfRepository, repoRes)) {
			branchResource = model.createIndividual(branchURI);
		} else { // we assume, each branch has its own model, hence we create the core concepts here as well
			addCoreConcepts(model);
			branchResource = buildBranchResource(repositoryURI, model, branchName);
		}
		BlockingQueue<Commit> inQueue = new LinkedBlockingQueue<>();
		BlockingQueue<Commit> outQueue = new LinkedBlockingQueue<>();
		BranchImpl branch = new BranchImpl(dataset, model, branchResource, stateKeeper, inQueue, outQueue);
		incomingCommitHandlers.stream().forEach(handler -> branch.appendIncomingCommitHandler(handler));
		services.stream().forEach(service -> branch.appendCommitService(service));
		model.register(branch);
		dataset.commit();
		dataset.end();
		dataset.begin();
		return branch;
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

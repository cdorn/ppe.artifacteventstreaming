package at.jku.isse.artifacteventstreaming.branch;

import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.jena.ontapi.model.OntDataProperty;
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
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitHandler;
import at.jku.isse.artifacteventstreaming.api.IncrementalCommitHandler;
import at.jku.isse.artifacteventstreaming.api.TimeStampProvider;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryBranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.InMemoryEventStore;
import at.jku.isse.artifacteventstreaming.branch.persistence.StateKeeperImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BranchBuilder {

	private BranchStateUpdater stateKeeper;
	private Resource repositoryRes;
	private URI branchURI;
	
	private String branchName = "main";
	private Dataset branchDataset;
	private final Dataset repoDataset;
	private OntModel repoModel;
	private OntSpecification modelSpec = OntSpecification.OWL2_DL_MEM; // no inference by default
	private List<CommitHandler> incomingCommitHandlers = new LinkedList<>();
	private List<IncrementalCommitHandler> services = new LinkedList<>();
	private Set<CommitHandler> outgoingCommitDistributers = new HashSet<>();
	private TimeStampProvider timeStampProvider;
	
	
	
	public BranchBuilder(@NonNull URI repositoryURI, @NonNull Dataset repoDataset, @NonNull OntModel repoModel) {
		this.repositoryRes = ResourceFactory.createResource(repositoryURI.toString());
		this.repoDataset = repoDataset;	
		this.repoModel = repoModel;
	}
	
	/**
	 * @param repositoryURI the identifier of the repository we are building this branch in
	 * @param repoDataset the dataset underlying this repository that contains all branches and their configuration within this repository, this is NOT where the branch content is stored
	 */
	public BranchBuilder(@NonNull URI repositoryURI, @NonNull Dataset repoDataset) {
		this.repositoryRes = ResourceFactory.createResource(repositoryURI.toString());
		this.repoDataset = repoDataset;	
		this.repoModel = null;
	}
	
	/**
	 * if not used, by default the 'main' branch will be created.
	 */
	public BranchBuilder setBranchLocalName(@NonNull String branchName) {
		if (branchName.isEmpty()) {
			throw new RuntimeException("Branchname cannot be empty");
		}
		this.branchName = branchName;
		return this;
	}
	
	/**
	 * if not used, by default the 'main' branch will be created.
	 */
	public BranchBuilder setBranchURI(@NonNull URI branchURI) {		
		this.branchURI = branchURI;
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

	public BranchBuilder setModelReasoner(@NonNull OntSpecification spec) {
		this.modelSpec = spec;
		return this;
	}
	
	/**
	 * if not used, then default system time is used as timestamp
	 * */
	public BranchBuilder setTimeStampProvider(@NonNull TimeStampProvider timeStampProvider) {
		this.timeStampProvider = timeStampProvider;
		return this;
	}
	
	/**
	 * if not used, no commits will be merged into this branch
	 */
	public BranchBuilder addIncomingCommitMerger(CommitHandler handler) {
		this.incomingCommitHandlers.add(handler);
		return this;
	}
	
	/**
	 * if not used, no services will be invoked for any commits.
	 */
	public BranchBuilder addBranchInternalCommitService(IncrementalCommitHandler service) {
		this.services.add(service);
		return this;
	}
	
	public BranchBuilder addOutgoingCommitDistributer(CommitHandler distributer) {
		this.outgoingCommitDistributers.add(distributer);
		return this;
	}
	
	
	public static URI generateBranchURI(Resource repositoryRes, String branchName) throws URISyntaxException {
		var baseURI = repositoryRes.getNameSpace();
		var localNamePart = repositoryRes.getLocalName() != null ? "/"+repositoryRes.getLocalName() : "";
		return new URI(baseURI.substring(0, baseURI.length()-1)
				+localNamePart+"#"+branchName);
		//return new URI(Objects.toString(repositoryRes)+"::"+branchName);
	}
	
	public static String getBranchNameFromURI(@NonNull URI branchURI) {
//		int pos = branchURI.toString().lastIndexOf("::");
//		if (pos < 0 || pos == branchURI.toString().length()-2) {
//			return null;
//		} else {
//			return branchURI.toString().substring(pos+2);
//		}
		return branchURI.getFragment();
	}
	
	public Branch build() throws Exception {

			
		if (branchDataset == null) {
			setDataset(DatasetFactory.createTxnMem());
		}
		if (branchURI == null) {
			branchURI = generateBranchURI(repositoryRes, branchName);
		}
		OntIndividual branchResource = prepareBranch(branchURI);
		BlockingQueue<Commit> inQueue = new LinkedBlockingQueue<>();
		BlockingQueue<Commit> outQueue = new LinkedBlockingQueue<>();

		OntModel model = OntModelFactory.createModel(branchDataset.getDefaultModel().getGraph(), modelSpec);
		if (stateKeeper == null) {
			stateKeeper = new StateKeeperImpl(branchURI, new InMemoryBranchStateCache(), new InMemoryEventStore());
		}
		if (timeStampProvider == null) {
			timeStampProvider = new SystemTimeStampProvider();
		}
		BranchImpl branch = new BranchImpl(branchDataset, model, branchResource, stateKeeper, inQueue, outQueue, timeStampProvider);
		addCommitHandlers(branch);
		return branch;
	}
	
	private OntIndividual prepareBranch(URI branchURI) {
		Resource branchRes = ResourceFactory.createResource(branchURI.toString());
		OntIndividual branchResource = null;
		repoDataset.begin(ReadWrite.WRITE);
		if (repoModel == null)
			repoModel = OntModelFactory.createModel(repoDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
		if (repoModel.contains(branchRes, AES.partOfRepository, repositoryRes)) {
			branchResource = repoModel.createIndividual(branchURI.toString());		
		} else { // we assume, each branch has its own model, hence we create the core concepts here as well
			addCoreConcepts(repoModel);
			branchResource = buildBranchResource(repositoryRes, repoModel, branchURI);			
		}	
		repoDataset.commit();
		repoDataset.end();
		return branchResource;
	}
	
	private static void addCoreConcepts(OntModel repoModel) {
		OntClass.Named repoType = repoModel.createOntClass(AES.repositoryType);
		OntClass.Named branchType = repoModel.createOntClass(AES.branchType);
		OntObjectProperty.Named partOfRepo = repoModel.createObjectProperty(AES.partOfRepository.getURI());
		partOfRepo.addDomain(branchType);
		partOfRepo.addRange(repoType);
		partOfRepo.addLabel("part of repository");
		
		OntClass.Named handlerConfig = repoModel.createOntClass(AES.commitHandlerConfigType);
		OntDataProperty configForType = repoModel.createDataProperty(AES.isConfigForHandlerType.getURI());
		configForType.addDomain(handlerConfig);
		configForType.addLabel("is configuration for handler of type");
		configForType.addComment("Is used to enable lookup the right handler factory from which to re-create a handler with the configuration described in domain of this property. Config properties are specific for each handler type");
	}
	
	private static OntIndividual buildBranchResource(Resource repo, OntModel initializedModel, URI branchURI) {
		OntClass.Named branchType = initializedModel.getOntClass(AES.branchType);
		OntObjectProperty.Named partOfRepo = initializedModel.getObjectProperty(AES.partOfRepository.getURI());
		OntIndividual branch = branchType.createIndividual(branchURI.toString());
		branch.addLabel(branch.getLocalName());
		branch.addProperty(partOfRepo, repo);
		return branch;
	}
	
	
	private void addCommitHandlers(Branch branch) {
		incomingCommitHandlers.stream().forEach(branch::appendIncomingCommitMerger);
		services.stream().forEach(branch::appendBranchInternalCommitService);		
		outgoingCommitDistributers.stream().forEach(branch::appendOutgoingCommitDistributer);

	}
	
	public static boolean doesDatasetContainBranch(Dataset dataset, @NonNull Resource repositoryRes, @NonNull String branchName) {
		if (dataset == null) return false;
		dataset.begin();
		URI branchURI;
		try {
			branchURI = generateBranchURI(repositoryRes, branchName);
		} catch (URISyntaxException e) {
			return false;
		}
		Resource branchRes = ResourceFactory.createResource(branchURI.toString());
		boolean doesContain = dataset.getDefaultModel().contains(branchRes, AES.partOfRepository, repositoryRes);
		dataset.end();
		return doesContain;
	}
}

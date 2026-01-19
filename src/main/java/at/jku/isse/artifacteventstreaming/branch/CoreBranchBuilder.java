package at.jku.isse.artifacteventstreaming.branch;

import at.jku.isse.artifacteventstreaming.api.*;
import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import io.micrometer.observation.ObservationRegistry;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.*;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.jspecify.annotations.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class CoreBranchBuilder {
    private static final UrlValidator validator = new UrlValidator();
    protected final Resource metadataBranchResource;
    protected final Dataset metadataBranchDataset;
    protected final List<IncrementalCommitHandler> services = new LinkedList<>();
    protected final ObservationRegistry observationRegistry;

    protected URI branchURI;
    protected String branchName = "main";
    protected Dataset branchDataset;
    protected OntModel metadataModel;
    protected OntSpecification modelSpec = OntSpecification.OWL2_DL_MEM; // no inference by default
    protected TimeStampProvider timeStampProvider = new SystemTimeStampProvider(); // default system time
    protected String owner;
    protected MetaModelOntologyProvider metaOntologyProvider;

    public CoreBranchBuilder(@NonNull URI repositoryURI, @Nullable Dataset metadataBranchDataset, @NonNull ObservationRegistry observationRegistry) {
        this.metadataBranchResource = ResourceFactory.createResource(repositoryURI.toString());
        this.metadataBranchDataset = metadataBranchDataset == null ? DatasetFactory.createTxnMem() : metadataBranchDataset;
        this.metadataModel = createMetadataModel(this.metadataBranchDataset);
        this.observationRegistry = observationRegistry;
        //temporary set branchURI to main
        this.branchURI = URI.create(generateNonValidatedBranchURI(metadataBranchResource, branchName));
    }

    protected static OntModel createMetadataModel(@NonNull Dataset metadataBranchDataset) {
        var doTX = !metadataBranchDataset.isInTransaction();
        if (doTX) {
            metadataBranchDataset.begin(ReadWrite.WRITE);
        }
        var model = OntModelFactory.createModel(metadataBranchDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM_BUILTIN_RDFS_INF);
        if (doTX) {
            metadataBranchDataset.commit();
            metadataBranchDataset.end();
        }
        return model;
    }

    protected static String generateNonValidatedBranchURI(Resource repositoryRes, String branchName) {
        var baseURI = repositoryRes.getNameSpace();
        var localNamePart = repositoryRes.getLocalName() != null ? "/" + repositoryRes.getLocalName() : "";
        return baseURI.substring(0, baseURI.length() - 1) + localNamePart + "/" + branchName + "#" + branchName;
    }

    public static URI generateBranchURI(Resource repositoryRes, String branchName) throws BranchConfigurationException {
        var uri = CoreBranchBuilder.generateNonValidatedBranchURI(repositoryRes, branchName);
        if (CoreBranchBuilder.validator.isValid(uri)) return URI.create(uri);
        else throw new BranchConfigurationException("Local branch name results in invalid Branch URI " + uri);
    }

    public static String getBranchNameFromURI(@NonNull URI branchURI) {
        return branchURI.getFragment();
    }

    private static void addCoreConcepts(OntModel metadataModel) {
        OntClass.Named repoType = metadataModel.createOntClass(AES.repositoryType);
        OntClass.Named branchType = metadataModel.createOntClass(AES.branchType);
        OntObjectProperty.Named partOfRepo = metadataModel.createObjectProperty(AES.partOfRepository.getURI());
        partOfRepo.addDomain(branchType);
        partOfRepo.addRange(repoType);
        partOfRepo.addLabel("part of repository");

        OntClass.Named handlerConfig = metadataModel.createOntClass(AES.commitHandlerConfigType);
        OntDataProperty configForType = metadataModel.createDataProperty(AES.isConfigForHandlerType.getURI());
        configForType.addDomain(handlerConfig);
        configForType.addLabel("is configuration for handler of type");
        configForType.addComment("Is used to enable lookup the right handler factory from which to re-create a handler with the configuration described in domain of this property. Config properties are specific for each handler type");
    }

    private static OntIndividual buildBranchResource(Resource metadataRoot, OntModel initializedModel, URI branchURI) {
        OntClass.Named branchType = initializedModel.getOntClass(AES.branchType);
        OntObjectProperty.Named partOfRepo = initializedModel.getObjectProperty(AES.partOfRepository.getURI());
        OntIndividual branch = branchType.createIndividual(branchURI.toString());
        branch.addLabel(branch.getLocalName());
        branch.addProperty(partOfRepo, metadataRoot);
        return branch;
    }

    public static boolean doesDatasetContainBranch(Dataset dataset, @NonNull Resource repositoryRes, @NonNull URI branchURI) {
        if (dataset == null) return false;
        var inTX = dataset.isInTransaction();
        if (!inTX) {
            dataset.begin();
        }

        Resource branchRes = ResourceFactory.createResource(branchURI.toString());
        boolean doesContain = dataset.getDefaultModel().contains(branchRes, AES.partOfRepository, repositoryRes);
        if (!inTX) {
            dataset.end();
        }
        return doesContain;
    }

    /**
     * if not used, by default the 'main' branch will be created. Override branchURI
     */
    public CoreBranchBuilder setBranchLocalName(@NonNull String branchName) throws BranchConfigurationException {
        if (branchName.isEmpty()) {
            throw new BranchConfigurationException("Branchname cannot be empty");
        }
        this.branchName = branchName;
        var uri = CoreBranchBuilder.generateNonValidatedBranchURI(metadataBranchResource, branchName);
        if (CoreBranchBuilder.validator.isValid(uri)) {
            this.branchURI = URI.create(uri);
            return this;
        } else
            throw new BranchConfigurationException("Local branch name results in invalid Branch URI " + uri);
    }

    /**
     * if not used, by default the 'main' branch will be created, overrides branch name
     */
    public CoreBranchBuilder setBranchURI(@NonNull URI branchURI) throws BranchConfigurationException {
        if (branchURI.getFragment() == null) {
            throw new BranchConfigurationException("BranchURI requires a fragment to be used as local unique name, but was: " + branchURI);
        }
        this.branchURI = branchURI;
        return this;
    }

    /**
     * if not used, by default a in memory dataset will be created.
     */
    public CoreBranchBuilder setDataset(@NonNull Dataset dataset) {
        this.branchDataset = dataset;
        return this;
    }

    public CoreBranchBuilder setModelReasoner(@NonNull OntSpecification spec) {
        this.modelSpec = spec;
        return this;
    }

    /**
     * if not used, then default system time is used as timestamp
     *
     */
    public CoreBranchBuilder setTimeStampProvider(@NonNull TimeStampProvider timeStampProvider) {
        this.timeStampProvider = timeStampProvider;
        return this;
    }

    /**
     * if not used, no services will be invoked for any commits.
     */
    public CoreBranchBuilder addBranchInternalCommitService(IncrementalCommitHandler service) {
        this.services.add(service);
        return this;
    }

    /**
     * if not used, no owner is recorded.
     */
    public CoreBranchBuilder addOwner(String ownerId) {
        this.owner = ownerId;
        return this;
    }

    public CoreBranchBuilder setMetaModelOntologyProvider(MetaModelOntologyProvider metaOntologyProvider) {
        this.metaOntologyProvider = metaOntologyProvider;
        return this;
    }

    public CoreBranch build() {
        if (branchDataset == null) {
            setDataset(DatasetFactory.createTxnMem());
        }
        OntIndividual branchResource = prepareBranch(branchURI, owner);

        branchDataset.begin(ReadWrite.WRITE);
        OntModel model = OntModelFactory.createModel(branchDataset.getDefaultModel().getGraph(), modelSpec);

        CoreBranchImpl branch = new CoreBranchImpl(branchDataset, model, branchResource, metadataModel, metadataBranchDataset, timeStampProvider, observationRegistry);
        if (metaOntologyProvider != null) {
            branch.setSchemaUtils(new MetaModelSchemaTypes(model, metaOntologyProvider.getMetaModelOntology()));
        }
        services.stream().forEach(branch::appendBranchInternalCommitService);
        branchDataset.commit();
        branchDataset.end();
        return branch;
    }

    protected OntIndividual prepareBranch(@NonNull URI branchURI, String owner) {
        Resource branchRes = ResourceFactory.createResource(branchURI.toString());
        OntIndividual branchResource = null;

        boolean doTX = !metadataBranchDataset.isInTransaction();
        if (doTX) {
            metadataBranchDataset.begin(ReadWrite.WRITE);
        }
        if (metadataModel == null) {
            metadataModel = OntModelFactory.createModel(metadataBranchDataset.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
        }
        if (metadataModel.contains(branchRes, AES.partOfRepository, metadataBranchResource)) {
            branchResource = metadataModel.createIndividual(branchURI.toString());
        } else { // we assume, each branch has its own model, hence we create the core concepts here as well
            log.debug("Creating new branch resource {} in metadata model", branchURI);
            CoreBranchBuilder.addCoreConcepts(metadataModel);
            branchResource = CoreBranchBuilder.buildBranchResource(metadataBranchResource, metadataModel, branchURI);
            if (owner != null && !owner.isEmpty()) {
                branchResource.addLiteral(AES.ownedBy, owner);
            }
        }
        if (doTX) {
            metadataBranchDataset.commit();
            metadataBranchDataset.end();
        }
        return branchResource;
    }



    public static class UrlValidator {
        public boolean isValid(final String value) {
            if (value == null) {
                return false;
            }
            final URI uri; // ensure value is a valid URI
            try {
                uri = new URI(value);
            } catch (final URISyntaxException e) {
                return false;
            }
            return uri.getFragment() != null;
        }
    }
}

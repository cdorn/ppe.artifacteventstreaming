package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import lombok.NonNull;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.shared.Lock;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Set;

public interface CoreBranch {

    Dataset getBranchMetadataDataset();
    OntModel getBranchMetadataModel();

    OntModel getModel();
    Dataset getDataset();
    OntIndividual getBranchResource();
    String getBranchId();
    String getBranchName();
    String getRepositoryURI();


    MetaModelSchemaTypes getSchemaUtils();
    void setSchemaUtils(MetaModelSchemaTypes utils);



    /**
     * @param commitMsg
     *  * Upon commit transaction, it does the following:
     *  0) creates a commit
     *    // Service augmentation phase
     *    1) it starts the cycle of commit augmentation by forwarding the commit to the first service
     *     service need to ensure they leave the model in a consistent state, resp add only consistent statements to the commit
     *     when no more statements are added (service exceptions are caught and ignored):
     *     // POST service augmentation phase
     *    2) it commits the changes to the backend model (internal transaction)*
     *
     *    @return the newly created/augmented commit
     * @throws PersistenceException
     */
    Commit commitChanges(@NonNull String commitMsg) throws PersistenceException, BranchConfigurationException;

    Commit commitChanges(@NonNull String commitMsg, @NonNull String mergedFromCommitId, @NonNull String mergedFromBranchURI  ) throws PersistenceException, BranchConfigurationException;


    /**
     *  if the underlying model is based on a transactional dataset, use this delegate method to start a read transaction without having to know about the dataset directly
     */
    void startReadTransaction();

    /**
     * wraps up any transaction with dataset end() (can also be used as fallback/finally on write transactions to ensure rollback and lock release
     */
    void completeTransaction(@Nullable Lock lock);

    /**
     * enables to continue a read transaction by acquiring and entering a lock, then promoting to Write Transaction.
     * when promotion succeeds then same semantics as if called startWriteTransaction
     * will throw if current transaction is a fixed read transaction.
     * if no current transaction, returns null
     * @return lock or null if already in a write transaction (or in no transaction)
     */
    Lock promoteToWriteTransaction();

    /**
     *  if the underlying model is based on a transactional dataset, use this delegate method to obtain a lock to ensure noone else is writing to this model/dataset in the meantime
     */
    Lock startWriteTransaction();

    /**
     * ensures no changes (cached or otherwise) are applied/persisted/forwarded
     */
    void abortWriteTransaction();

    /**
     * @param service
     * adds this service to the end of the chain of services that process local changes.
     * If this service is already in the list, then it moves that service to the current end of the chain
     */
    void appendBranchInternalCommitService(@NonNull IncrementalCommitHandler service) ;

    /**
     * @param service
     * removes the service from the chain. When no services remain, any commit is put directly into the out queue.
     */
    void removeBranchInternalCommitService(@NonNull IncrementalCommitHandler service) ;

    List<OntIndividual> getLocalCommitServiceConfig();


    Set<IncrementalCommitHandler> getRegisteredLocalCommitHandlers();

    // stops all incoming handlers, and distributers etc, so the branch can be garbage collected,
    // one deactivated the branch object should not be used any more and all distributers connected to this branch object should be notified and paused as well, resp, updated with a new branch object
    void deactivate();
}

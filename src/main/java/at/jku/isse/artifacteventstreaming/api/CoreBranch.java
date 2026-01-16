package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.api.exceptions.BranchConfigurationException;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import lombok.NonNull;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.shared.Lock;

import java.util.List;

public interface CoreBranch {


    OntModel getModel();
    Dataset getDataset();
    OntIndividual getBranchResource();
    String getBranchId();
    String getBranchName();
    String getRepositoryURI();


    MetaModelSchemaTypes getSchemaUtils();
    void setSchemaUtils(MetaModelSchemaTypes utils);

    // stops all incoming handlers, and distributers etc, so the branch can be garbage collected,
    // one deactivated the branch object should not be used any more and all distributers connected to this branch object should be notified and paused as well, resp, updated with a new branch object
    void deactivate();

    /**
     * @param commitMsg
     *  * Upon commit transaction, it does the following:
     *  0) creates a commit
     *  // PRE service augmentation phase:
     *    1) it commits the transaction to the event db (temporal persistance to survive restart of application, not needed upon persistance of augmented commit)
     *    2) it stores which commit id has been produced in the ehcache
     *    3) it commits the changes to the backend model db / the authoritative source of the model state
     *    // Service augmentation phase
     *    4) it starts the cycle of commit augmentation by forwarding the commit to the first service
     *     service need to ensure they leave the model in a consistent state, resp add only consistent statements to the commit
     *     when no more statements are added (service exceptions are caught and ignored):
     *     // POST service augmentation phase
     *    5) it commits the augmented transaction to the event db (router skips steps 1 to 4 when no services are configured)
     *    6) it store the commit id as being augmented in the ehcache
     *    7) it commits the changes to the backend model (internal transaction)
     *    8) it puts the augmented commit into the branch's outqueue.
     *    Notice: use {@link concludeTransaction( Lock writeLock, String commitMsg)} when having obtained a write lock instead which wraps around this method
     *
     *    @return the newly created/augmented commit
     * @throws BranchConfigurationException
     * @throws PersistenceException
     */
    Commit commitChanges(String commitMsg) throws PersistenceException, BranchConfigurationException;


    /**
     * drops all currently cached changes/statements
     * and aborts the model transaction (undoes the changes)
     */
    void undoNoncommitedChanges();


    /**
     *  if the underlying model is based on a transactional dataset, use this delegate method to start a read transaction without having to know about the dataset directly
     */
    void startReadTransaction();

    void completeReadTransaction();

    /**
     *  if the underlying model is based on a transactional dataset, use this delegate method to obtain a lock to ensure noone else is writing to this model/dataset in the meantime
     */
    Lock startWriteTransaction();

    /**
     *  when having acquired a lock, use this method instead of {@link commitChanges(String commitMsg)} to complete the write transaction and release the lock
     */
    Commit concludeTransaction(Lock writeLock, String commitMsg) throws PersistenceException, BranchConfigurationException;

    void abortWriteTransaction(@NonNull Lock lock) ;

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

    /**
     * @throws BranchConfigurationException when incoming commits are reenqueue but no merge handler is available
     * @throws Exception                    when handling of preliminary commit or any other replaying to get up to date fails
     */
    void startCommitHandlers() throws PersistenceException, BranchConfigurationException;

}

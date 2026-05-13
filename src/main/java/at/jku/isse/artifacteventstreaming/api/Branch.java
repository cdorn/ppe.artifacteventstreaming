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
import java.util.Set;


public interface Branch extends CoreBranch {



	/**
	 * @throws BranchConfigurationException when incoming commits are reenqueue but no merge handler is available
	 * @throws Exception                    when handling of preliminary commit or any other replaying to get up to date fails
	 */
	void startCommitHandlers() throws PersistenceException, BranchConfigurationException;


	BranchStateKeeper getStateKeeper();
	Commit getLastCommit();
	/**
	 * @param commit
	 * routes the commit through any available filters and processors (statements within the commit are not changes, just the decision which ones are applied)
	 * before applying the remaining statements onto this model within a transaction
	 * whenever there is a filter a replacement commit is created, to document that not the original commit was applied
	 * @throws BranchConfigurationException when branch has been deactivated, or no merge handlers are configured
	 * @throws PersistenceException  when persisting in queue fails, then must assume commit has not been processed 
	 */
    void enqueueIncomingCommit(Commit commit) throws PersistenceException, BranchConfigurationException;

	/**
	 * @param handler
	 * adds this to the end of the chain of handlers that process an incoming commit.
	 * If this handler is already in the list, then it moves that handler to the current end of the chain
	 */
    void appendIncomingCommitMerger(CommitHandler handler);
	
	
	List<OntIndividual> getIncomingCommitHandlerConfig();
	
	/**
	 * @param handler
	 * removes the handler from the chain. When no handlers remain, any incoming commits are dropped/ignored.
	 */
    void removeIncomingCommitMerger(CommitHandler handler);
	


	
	void appendOutgoingCommitDistributer(@NonNull CommitHandler crossBranchHandler);
	
	void removeOutgoingCommitDistributer(@NonNull CommitHandler crossBranchHandler);
	
	List<OntIndividual> getOutgoingCommitDistributerConfig();
	
	


	
}

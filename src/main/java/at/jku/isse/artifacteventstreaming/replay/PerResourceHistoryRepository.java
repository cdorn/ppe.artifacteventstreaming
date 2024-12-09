package at.jku.isse.artifacteventstreaming.replay;

import java.util.List;
import java.util.Set;

import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

public interface PerResourceHistoryRepository {

	public void appendHistory(String commitId, String branchURI, long timeStamp, Set<ContainedStatement> addedStatements, Set<ContainedStatement> removedStatements) throws PersistenceException;
	
	public List<ReplayEntry> getHistoryForResource(String uriOrAnonId, String branchURI);
}

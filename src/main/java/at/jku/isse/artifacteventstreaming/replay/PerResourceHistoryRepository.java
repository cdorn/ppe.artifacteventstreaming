package at.jku.isse.artifacteventstreaming.replay;

import java.util.Set;
import java.util.stream.Stream;

import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

public interface PerResourceHistoryRepository {

	//public void appendHistory(String commitId, String branchURI, long timeStamp, Set<ContainedStatement> addedStatements, Set<ContainedStatement> removedStatements) throws PersistenceException;
	
	public Stream<ReplayEntry> getHistoryForResource(String uriOrAnonId, String branchURI) throws PersistenceException;
	
	public Stream<ReplayEntry> getHistoryForResources(Set<String> uriOrAnonIds, String branchURI) throws PersistenceException;
}

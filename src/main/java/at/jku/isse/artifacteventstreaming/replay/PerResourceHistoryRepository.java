package at.jku.isse.artifacteventstreaming.replay;

import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

import java.util.Set;
import java.util.stream.Stream;

public interface PerResourceHistoryRepository {
	
	Stream<ReplayEntry> getHistoryForResource(String uriOrAnonId, String branchURI) throws PersistenceException;
	
	Stream<ReplayEntry> getHistoryForResources(Set<String> uriOrAnonIds, String branchURI) throws PersistenceException;
}

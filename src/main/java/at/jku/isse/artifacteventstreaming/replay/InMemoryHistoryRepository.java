package at.jku.isse.artifacteventstreaming.replay;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.AES.OPTYPE;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

import java.util.*;
import java.util.stream.Stream;

public class InMemoryHistoryRepository implements PerResourceHistoryRepository {

	private final HashMap<String, List<ReplayEntry>> history = new HashMap<>();
	
	public void appendHistory(String commitId, String branchURI, long timeStamp, Set<ContainedStatement> addedStatements,
			Set<ContainedStatement> removedStatements) {
		addedStatements.forEach(stmt -> history.computeIfAbsent(AES.resourceToId(stmt.getContainerOrSubject()), 
				k -> new LinkedList<>()).add(new ReplayEntry(OPTYPE.ADD, stmt, commitId, timeStamp, branchURI)) );
		removedStatements.forEach(stmt -> history.computeIfAbsent(AES.resourceToId(stmt.getContainerOrSubject()), 
				k -> new LinkedList<>()).add(new ReplayEntry(OPTYPE.REMOVE, stmt, commitId, timeStamp, branchURI)) );
	}

	@Override
	public Stream<ReplayEntry> getHistoryForResource(String uriOrAnonId, String branchURI) {
		return history.getOrDefault(uriOrAnonId, Collections.emptyList()).stream()
			.filter(entry -> entry.branchURI().equals(branchURI));
	}

	@Override
	public Stream<ReplayEntry> getHistoryForResources(Set<String> uriOrAnonIds, String branchURI)
			throws PersistenceException {
		return uriOrAnonIds.stream().flatMap(uriOrAnonId -> getHistoryForResource(uriOrAnonId, branchURI));
	}
}

package at.jku.isse.artifacteventstreaming.replay;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.AES.OPTYPE;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;

public class InMemoryHistoryRepository implements PerResourceHistoryRepository {

	private final HashMap<String, List<ReplayEntry>> history = new HashMap<>();
	
	@Override
	public void appendHistory(String commitId, String branchURI, long timeStamp, Set<ContainedStatement> addedStatements,
			Set<ContainedStatement> removedStatements) {
		addedStatements.forEach(stmt -> history.computeIfAbsent(AES.resourceToId(stmt.getContainerOrSubject()), 
				k -> new LinkedList<>()).add(new ReplayEntry(OPTYPE.ADD, stmt, commitId, timeStamp, branchURI)) );
		removedStatements.forEach(stmt -> history.computeIfAbsent(AES.resourceToId(stmt.getContainerOrSubject()), 
				k -> new LinkedList<>()).add(new ReplayEntry(OPTYPE.REMOVE, stmt, commitId, timeStamp, branchURI)) );
	}

	@Override
	public List<ReplayEntry> getHistoryForResource(String uriOrAnonId, String branchURI) {
		return history.getOrDefault(branchURI, Collections.emptyList()).stream()
			.filter(entry -> entry.getBranchURI().equals(branchURI))
			.toList();
	}
}

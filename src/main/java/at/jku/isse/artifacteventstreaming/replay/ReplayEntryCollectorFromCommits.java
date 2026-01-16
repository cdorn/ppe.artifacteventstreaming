package at.jku.isse.artifacteventstreaming.replay;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.Commit;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReplayEntryCollectorFromCommits implements ReplayEntryCollector {

	private final List<Commit> commitsInChronologicalOrder = new LinkedList<>();
	
	
	public void addCommit(Commit commit) {
		if (!commitsInChronologicalOrder.contains(commit)) {
			commitsInChronologicalOrder.add(commit);
		}
	}
	
	@Override
	public List<ReplayEntry> getReplayEntriesInChronologicalOrder(Map<Resource, Set<Property>> replayScope) {
		return commitsInChronologicalOrder.stream()
				.flatMap(commit -> flattenCommit(commit, replayScope))
				.collect(Collectors.toCollection(ArrayList::new));		
	}
	
	private Stream<ReplayEntry> flattenCommit(Commit commit, Map<Resource, Set<Property>> replayScope) {
		return Stream.concat(
				commit.getAddedStatements().stream()
				.filter(stmt -> replayScope.containsKey(stmt.getSubject()))
				.map(stmt -> {
					var props = replayScope.get(stmt.getSubject());
					if (props.contains(stmt.getPredicate())) {
						return new ReplayEntry(AES.OPTYPE.ADD, new ContainedStatementImpl(stmt), commit.getCommitId(), commit.getTimeStamp(), commit.getOriginatingBranchId());
					} else {
						return null;
					}
				})
				.filter(Objects::nonNull) 
			,
				commit.getRemovedStatements().stream()
				.filter(stmt -> replayScope.containsKey(stmt.getSubject()))
				.map(stmt -> {
					var props = replayScope.get(stmt.getSubject());
					if (props.contains(stmt.getPredicate())) {
						return new ReplayEntry(AES.OPTYPE.REMOVE, new ContainedStatementImpl(stmt), commit.getCommitId(), commit.getTimeStamp(), commit.getOriginatingBranchId());
					} else {
						return null;
					}
				})
				.filter(Objects::nonNull)
			);
	}

	@Override
	public List<ReplayEntry> getPartialReplayEntries(long fromTimeStampIncl, Map<Resource, Set<Property>> replayScope) {
		return commitsInChronologicalOrder.stream()
				.filter(commit -> commit.getTimeStamp() >= fromTimeStampIncl)
				.flatMap(commit -> flattenCommit(commit, replayScope))
				.collect(Collectors.toCollection(ArrayList::new));
	}
}

package at.jku.isse.artifacteventstreaming.replay;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.replay.ReplayEntry.OPTYPE;

public class ReplayEntryCollectorFromCommits implements ReplayEntryCollector {

	private List<Commit> commitsInChronologicalOrder = new LinkedList<>();
	
	
	public void addCommit(Commit commit) {
		if (!commitsInChronologicalOrder.contains(commit)) {
			commitsInChronologicalOrder.add(commit);
		}
	}
	
	@Override
	public List<ReplayEntry> getReplayEntriesInChronologicalOrder(Map<Resource, Set<Property>> replayScope) {
		return commitsInChronologicalOrder.stream()
				.flatMap(commit -> flattenCommit(commit, replayScope))
				.toList();		
	}
	
	private Stream<ReplayEntry> flattenCommit(Commit commit, Map<Resource, Set<Property>> replayScope) {
		return Stream.concat(
				commit.getAddedStatements().stream()
				.filter(stmt -> replayScope.keySet().contains(stmt.getSubject()))
				.map(stmt -> {
					var props = replayScope.get(stmt.getSubject());
					if (props.contains(stmt.getPredicate())) {
						return new ReplayEntry(OPTYPE.ADD, stmt, commit.getCommitId(), commit.getTimeStamp());
					} else {
						return null;
					}
				})
				.filter(Objects::nonNull) 
			,
				commit.getRemovedStatements().stream()
				.filter(stmt -> replayScope.keySet().contains(stmt.getSubject()))
				.map(stmt -> {
					var props = replayScope.get(stmt.getSubject());
					if (props.contains(stmt.getPredicate())) {
						return new ReplayEntry(OPTYPE.REMOVE, stmt, commit.getCommitId(), commit.getTimeStamp());
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
				.toList();
	}
}

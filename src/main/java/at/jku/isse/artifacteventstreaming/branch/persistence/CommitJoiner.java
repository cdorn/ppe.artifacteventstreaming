package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.jena.rdf.model.Statement;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.branch.StatementCommitImpl;

public class CommitJoiner {

	private final List<Commit> commitsToJoin = new LinkedList<>();
		
	/**
	 * @param commit the metadata of the first commit added is used to determine the joined commit details  
	 */
	public void addCommit(Commit commit) {
		commitsToJoin.add(commit);
	}
	
	public Commit join() {
		if (commitsToJoin.isEmpty()) {
			return null;
		} if (commitsToJoin.size() == 1) {
			return commitsToJoin.get(0);
		} else {			
			List<ContainedStatement> addedStmts = new LinkedList<>();			
			List<ContainedStatement> removedStmts = new LinkedList<>();
			commitsToJoin.forEach(commit -> {
				addedStmts.addAll(commit.getAddedStatements());
				removedStmts.addAll(commit.getRemovedStatements());
			});
			var commit = commitsToJoin.get(0);
			commitsToJoin.clear(); // to be on the safe side, if anybody tries to reuse this
			return new StatementCommitImpl(commit.getOriginatingBranchId()
					, commit.getCommitId()
					, commit.getPrecedingCommitId()
					, commit.getCommitMessage()
					, commit.getTimeStamp()
					, new LinkedHashSet<>(addedStmts)
					, new LinkedHashSet<>(removedStmts));
		}
		
	}
	
}

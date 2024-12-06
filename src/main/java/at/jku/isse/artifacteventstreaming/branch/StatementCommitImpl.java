package at.jku.isse.artifacteventstreaming.branch;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.jena.rdf.model.Statement;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import at.jku.isse.artifacteventstreaming.api.Commit;
import lombok.Getter;

@JsonIgnoreProperties(value = { "additionCount", "removalCount", "empty" })
public class StatementCommitImpl implements Commit {

	private final Set<Statement> addedStatements = new LinkedHashSet<>();
	private final Set<Statement> removedStatements = new LinkedHashSet<>();
	@Getter
	private final String commitMessage;
	@Getter
	private final String commitId;
	@Getter
	private final String precedingCommitId;
	@Getter
	private final String originatingBranchId;
	@Getter
	private final long timeStamp;
	
	
	public StatementCommitImpl(String branchId, String commitMsg, String precedingCommitId, long timeStamp) {
		this.commitMessage = commitMsg;
		this.originatingBranchId = branchId;
		this.commitId = generateUUID();
		this.precedingCommitId = precedingCommitId;
		this.timeStamp = timeStamp;
	}
	
	public StatementCommitImpl(@JsonProperty("originatingBranchId") String branchId
			, @JsonProperty("commitMessage") String commitMsg
			, @JsonProperty("precedingCommitId") String precedingCommitId
			, @JsonProperty("timeStamp") long timeStamp
			, @JsonProperty("addedStatements") Set<Statement> addedStatements
			, @JsonProperty("removedStatements") Set<Statement> removedStatements
			) {
		this(branchId, commitMsg, precedingCommitId, timeStamp);
		this.addedStatements.addAll(addedStatements);
		this.removedStatements.addAll(removedStatements);
	}
	

	public StatementCommitImpl( String branchId
			,  String mergedCommitId
			, String commitMsg
			, String precedingCommitId
			, long timeStamp
			, Set<Statement> addedStatements
			, Set<Statement> removedStatements
			) {
		this.commitMessage = commitMsg;
		this.originatingBranchId = branchId;
		this.commitId = mergedCommitId;
		this.precedingCommitId = precedingCommitId;
		this.addedStatements.addAll(addedStatements);
		this.removedStatements.addAll(removedStatements);
		this.timeStamp = timeStamp;
	}
	
	private String generateUUID() {
		return UUID.randomUUID().toString();
	}
	
	@Override
	public void appendAddedStatements(Set<Statement> stmts) {
		addedStatements.addAll(stmts);
	}
	
	@Override
	public void appendRemovedStatement(Set<Statement> stmts) {
		removedStatements.addAll(stmts);
	}
	
	@Override
	public List<Statement> getAddedStatements() {
		return addedStatements.stream().collect(Collectors.toList());
	}
	
	@Override
	public List<Statement> getRemovedStatements() {
		return removedStatements.stream().collect(Collectors.toList());
	}

	@Override
	public int getAdditionCount() {
		return addedStatements.size();
	}

	@Override
	public int getRemovalCount() {
		return removedStatements.size();
	}
	
	@Override
	public boolean isEmpty() {
		return addedStatements.isEmpty() && removedStatements.isEmpty();
	}

	public void removeEffectlessStatements(int addOffsetInclusive, int removeOffsetInclusive) {
		// this is very impl specific how we use this, so not part of the general interface.
		// addoff set defines which service-produced added statements might be undoing a pre-existing remove statement and vice versa, 
		// we need not check any other statements before these offsets, as these are part of the preliminary pre-service commit and have been cleaned as the arrive
		List<Statement> allAdded = addedStatements.stream().collect(Collectors.toList());		
		if (addOffsetInclusive < allAdded.size()) {
			for (int i = (allAdded.size()-1) ; i >= addOffsetInclusive ; i--) {
				var stmt = allAdded.get(i);
				if (removedStatements.contains(stmt)) {
					addedStatements.remove(stmt);
					removedStatements.remove(stmt);
				}
			}
		}
		List<Statement> allRemoved = removedStatements.stream().collect(Collectors.toList());
		if (removeOffsetInclusive < allRemoved.size()) {
			for (int i = (allRemoved.size()-1) ; i >= removeOffsetInclusive ; i--) {
				var stmt = allRemoved.get(i);
				if (addedStatements.contains(stmt)) {
					removedStatements.remove(stmt);
					addedStatements.remove(stmt);					
				}
			}
		}	
	}
	
	@Override
	public String toString() {
		return "Commit [msg=" + commitMessage + ", id=" + commitId
				+ ", branch=" + originatingBranchId + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(commitId, originatingBranchId);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StatementCommitImpl other = (StatementCommitImpl) obj;
		return Objects.equals(commitId, other.commitId)
				&& Objects.equals(originatingBranchId, other.originatingBranchId);
	}

	
	
}

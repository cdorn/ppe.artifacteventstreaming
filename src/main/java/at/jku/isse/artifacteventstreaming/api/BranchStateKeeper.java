package at.jku.isse.artifacteventstreaming.api;

import java.util.List;
import java.util.Optional;

public interface BranchStateKeeper {

	boolean hasSeenCommit(Commit commit);
	
	List<Commit> getHistory();
	
	Optional<Commit> getLastCommit();
	
	List<Commit> getCommitsForwardIncludingFrom(String string);
}

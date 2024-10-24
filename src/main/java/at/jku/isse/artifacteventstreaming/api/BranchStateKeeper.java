package at.jku.isse.artifacteventstreaming.api;

import java.util.List;
import java.util.Optional;

public interface BranchStateKeeper {

	public boolean hasSeenCommit(Commit commit);
	
	public List<Commit> getHistory();
	
	public Optional<Commit> getLastCommit();
	
	public List<Commit> getCommitsForwardIncludingFrom(String string);
}

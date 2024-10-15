package at.jku.isse.artifacteventstreaming.api;

import java.util.List;

public interface StateKeeper {

	public void finishedMerge(Commit commit);
	
	public void beforeServices(Commit commit) throws Exception;
	
	public void afterServices(Commit commit) throws Exception;
	
	public boolean hasSeenCommit(Commit commit);
	
	public List<Commit> getHistory();
	
	public Commit getLastCommit();
	
}

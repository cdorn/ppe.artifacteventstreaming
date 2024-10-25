package at.jku.isse.artifacteventstreaming.api;

public interface IncrementalCommitHandler extends CommitHandler {

	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval);
}

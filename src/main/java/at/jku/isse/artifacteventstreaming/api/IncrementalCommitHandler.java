package at.jku.isse.artifacteventstreaming.api;

public interface IncrementalCommitHandler extends CommitHandler {

	void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval);
}

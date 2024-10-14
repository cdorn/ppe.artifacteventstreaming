package at.jku.isse.artifacteventstreaming.api;

public interface BranchInternalCommitHandler extends CommitHandler {

	public void handleCommitFromOffset(Commit commit, int indexOfNewAddition, int indexOfNewRemoval);
}

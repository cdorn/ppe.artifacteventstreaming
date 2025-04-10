package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.util.LinkedList;
import java.util.List;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.CommitDeliveryEvent;
import at.jku.isse.artifacteventstreaming.api.PerBranchEventStore;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

/**
 * @author Christoph Mayr-Dorn
 * use for testing only
 */
public class InMemoryEventStore implements PerBranchEventStore {

	private final List<Commit> allCommits = new LinkedList<>();
	private final List<CommitDeliveryEvent> deliveries = new LinkedList<>();
	
	@Override
	public List<Commit> loadAllCommits() throws PersistenceException {
		return new LinkedList<>(allCommits);
	}

	@Override
	public List<Commit> loadAllIncomingCommitsForBranchFromCommitIdOnward(String fromCommitIdOnwards) throws PersistenceException {
		if (fromCommitIdOnwards == null) {
			return deliveries.stream().map(event -> event.getCommit())
					.toList();
		} else {
			Boolean found = false;
			List<Commit> commits = new LinkedList<>();
			for (CommitDeliveryEvent event : deliveries) {
				if (Boolean.TRUE.equals(!found) && event.getCommitId().equals(fromCommitIdOnwards)) {
					found = true;
				}
				if (Boolean.TRUE.equals(found)) {
					commits.add(event.getCommit());
				}
			}
			return commits;
		}
	}

	@Override
	public void appendCommit(Commit commit) throws PersistenceException {
		allCommits.add(commit);
	}

	@Override
	public void appendCommitDelivery(CommitDeliveryEvent event) throws PersistenceException {
		deliveries.add(event);
	}

}

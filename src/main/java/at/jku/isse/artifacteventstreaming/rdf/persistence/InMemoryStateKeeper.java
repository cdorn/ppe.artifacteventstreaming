package at.jku.isse.artifacteventstreaming.rdf.persistence;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.StateKeeper;
import at.jku.isse.artifacteventstreaming.api.StateKeeperFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemoryStateKeeper implements StateKeeper {
	
	private final Set<Commit> preliminaryCommits = new HashSet<>();
	private final Set<Commit> producedCommits = new LinkedHashSet<>();
	private final Set<String> seenCommitIds = new HashSet<>();
	private Commit lastCommit = null;
	
	@Override
	public void finishedMerge(Commit commit) {
		// remove from inqueue state keeping, not needed here
		log.debug("Finished merge of" +commit.getCommitId());
	}

	@Override
	public void beforeServices(Commit commit) {
		preliminaryCommits.add(commit);
		seenCommitIds.add(commit.getCommitId());
		log.debug("Pre Services: "+commit.getCommitId());
	}

	@Override
	public void afterServices(Commit commit) {
		producedCommits.add(commit);
		preliminaryCommits.remove(commit);
		seenCommitIds.add(commit.getCommitId());
		lastCommit = commit;
		log.debug("Post Services: "+commit.getCommitId());
	}

	@Override
	public boolean hasSeenCommit(Commit commit) {
		return seenCommitIds.contains(commit.getCommitId());
	}

	@Override
	public List<Commit> getHistory() {
		return new LinkedList<>(producedCommits);
	}

	@Override
	public Commit getLastCommit() {
		return lastCommit;
	}

	@Override
	public void loadState(OntModel model)  {
		//noop as no state to load for in memory statekeeper
	}

	public static StateKeeperFactory getStateKeeperFactory() {
		return new StateKeeperFactory() {
			Map<URI, StateKeeper> keepers = new HashMap<>();
			
			@Override
			public StateKeeper getStateKeeperFor(URI branchURI) {
				if (!keepers.containsKey(branchURI)) {
					keepers.put(branchURI, new InMemoryStateKeeper());
					
				} 
				return keepers.get(branchURI);
			}
		};
	}
	
}

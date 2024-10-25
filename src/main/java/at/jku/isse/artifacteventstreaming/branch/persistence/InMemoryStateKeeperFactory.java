package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.StateKeeperFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemoryStateKeeperFactory implements StateKeeperFactory {

	private final InMemoryBranchStateCache cache =  new InMemoryBranchStateCache(); //we reuse cache across branches
	private final Map<URI,  InMemoryEventStore> keepers = new HashMap<>(); // but have per branch eventstore

	@Override
	public BranchStateUpdater createStateKeeperFor(URI branchURI) {
		if (!keepers.containsKey(branchURI)) {
			keepers.put(branchURI,  new InMemoryEventStore());
		} 
		return new StateKeeperImpl(branchURI, cache, keepers.get(branchURI)) ; 
	}

}

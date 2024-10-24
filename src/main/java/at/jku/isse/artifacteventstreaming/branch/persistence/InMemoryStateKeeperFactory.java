package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.BranchStateUpdater;
import at.jku.isse.artifacteventstreaming.api.StateKeeperFactory;
import at.jku.isse.artifacteventstreaming.branch.persistence.EventStoreFactory.EventStoreImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InMemoryStateKeeperFactory implements StateKeeperFactory {

	private final InMemoryBranchStateCache cache =  new InMemoryBranchStateCache(); //we reuse cache across branches
	Map<URI,  InMemoryEventStore> keepers = new HashMap<>(); // but have per branch eventstore

	@Override
	public BranchStateUpdater createStateKeeperFor(URI branchURI) {
		if (!keepers.containsKey(branchURI)) {
			keepers.put(branchURI,  new InMemoryEventStore());
		} 
		return new StateKeeperImpl(branchURI, cache, keepers.get(branchURI)) ; 
	}

}

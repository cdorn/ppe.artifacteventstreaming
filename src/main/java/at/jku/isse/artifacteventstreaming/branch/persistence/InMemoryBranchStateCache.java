package at.jku.isse.artifacteventstreaming.branch.persistence;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

public class InMemoryBranchStateCache implements BranchStateCache {

	private final Map<String, String> cache = Collections.synchronizedMap(new HashMap<>());
	
	@Override
	public void put(String key, String value) throws PersistenceException {
		cache.put(key, value);
	}

	@Override
	public String get(String key) throws PersistenceException {
		return cache.get(key);
	}
	
}
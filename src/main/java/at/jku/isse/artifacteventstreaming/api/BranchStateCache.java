package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

public interface BranchStateCache {

	public void put(String key, String value) throws PersistenceException;
	public String get(String key) throws PersistenceException;
}

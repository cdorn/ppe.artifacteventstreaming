package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;

public interface BranchStateCache {

	void put(String key, String value) throws PersistenceException;
	String get(String key) throws PersistenceException;
}

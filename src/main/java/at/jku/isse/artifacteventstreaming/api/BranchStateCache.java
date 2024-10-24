package at.jku.isse.artifacteventstreaming.api;

public interface BranchStateCache {

	public void put(String key, String value) throws Exception;
	public String get(String key) throws Exception;
}

package at.jku.isse.artifacteventstreaming.api;

import org.rocksdb.RocksDBException;

public interface BranchStateCache {

	public void put(String key, String value) throws Exception;
	public String get(String key) throws Exception;
}

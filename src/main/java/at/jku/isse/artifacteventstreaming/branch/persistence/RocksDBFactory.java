package at.jku.isse.artifacteventstreaming.branch.persistence;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import lombok.RequiredArgsConstructor;

public class RocksDBFactory {

	public static final String DEFAULT_STORAGEPATH = "./branchStatusCache/";
	
	private Options options;
	private RocksDB db;
	private final String path;
	
	public RocksDBFactory(String path)  {
		this.path = path;
		RocksDB.loadLibrary();
		options = new Options().setCreateIfMissing(true) ;
	}
	
	public RocksDBFactory() {
		this(DEFAULT_STORAGEPATH);
	}
	
	private String getStoragePath() {
		return path;
	}

	public BranchStateCache getCache() throws RocksDBException {
		if (db == null) {
			db = RocksDB.open(options, getStoragePath())  ;
		}
		return new RocksDbCache(db);
	}
	
	/**
	 * deletes the persisted cache from disk and invalidates any prior cache instances
	 * @throws RocksDBException 
	 */
	public void clearAndCloseCache() throws RocksDBException {
		closeCache();
		if (options != null) {
			RocksDB.destroyDB(getStoragePath(), options);
			db = null;
		}
		//db = RocksDB.open(options, getStoragePath())  ;
	}
	
	public void closeCache() {
		if (db != null && !db.isClosed()) {
			db.close();
			db = null;
		}
	}
	
	@RequiredArgsConstructor
	private class RocksDbCache implements BranchStateCache {

		private final RocksDB db;
		
		@Override
		public void put(String key, String value) throws PersistenceException {
			try {
				db.put(key.getBytes(), value.getBytes());
			} catch (RocksDBException e) {
				throw new PersistenceException(String.format("Error writing key %s to cache with underlying exception %s", key, e.getMessage()));
			}
		}

		@Override
		public String get(String key) throws PersistenceException {
			byte[] content;
			try {
				content = db.get(key.getBytes());
			} catch (RocksDBException e) {
				throw new PersistenceException(String.format("Error reading key %s to cache with underlying exception %s", key, e.getMessage()));
			}
			if (content != null)
				return new String(content);
			else
				return null;
		}
		
	}
	
}

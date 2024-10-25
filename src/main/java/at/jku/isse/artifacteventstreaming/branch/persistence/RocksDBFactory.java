package at.jku.isse.artifacteventstreaming.branch.persistence;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import lombok.RequiredArgsConstructor;

public class RocksDBFactory {

	private static final String BRANCH_STATE_KEEPER = "branchStateKeeper";
	public static final String DEFAULT_STORAGEPATH = "./branchStatusCache/";
	
	private final Options options;
	private RocksDB db;
	private final String path;
	
	public RocksDBFactory(String path) throws RocksDBException {
		this.path = path;
		RocksDB.loadLibrary();
		options = new Options().setCreateIfMissing(true) ;
		db = RocksDB.open(options, getStoragePath())  ;
	}
	
	public RocksDBFactory() throws RocksDBException {
		this(DEFAULT_STORAGEPATH);
	}
	
	private String getStoragePath() {
		return path;
	}

	public BranchStateCache getCache() {
		return new RocksDbCache(db);
	}
	
	/**
	 * deletes the persisted cache from disk and invalidates any prior cache instances
	 * @throws RocksDBException 
	 */
	public void resetCache() throws RocksDBException {
		if (db != null && !db.isClosed()) {
			db.close();
		}
		RocksDB.destroyDB(getStoragePath(), options);
		db = RocksDB.open(options, getStoragePath())  ;
	}
	
	public void closeCache() {
		if (db != null && !db.isClosed()) {
			db.close();
		}
	}
	
	@RequiredArgsConstructor
	private class RocksDbCache implements BranchStateCache {

		private final RocksDB db;
		
		@Override
		public void put(String key, String value) throws RocksDBException {
			db.put(key.getBytes(), value.getBytes());
		}

		@Override
		public String get(String key) throws RocksDBException {
			return new String(db.get(key.getBytes()));
		}
		
	}
	
}

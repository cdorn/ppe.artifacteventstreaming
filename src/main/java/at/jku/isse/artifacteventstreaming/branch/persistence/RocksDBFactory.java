package at.jku.isse.artifacteventstreaming.branch.persistence;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import lombok.RequiredArgsConstructor;

public class RocksDBFactory {

	private static final String BRANCH_STATE_KEEPER = "branchStateKeeper";
	
	private final Options options;
	private final RocksDB db;
	
	public RocksDBFactory() throws RocksDBException {
		RocksDB.loadLibrary();
		options = new Options().setCreateIfMissing(true) ;
		db = RocksDB.open(options, getStoragePath())  ;

	}
	
	private String getStoragePath() {
		return "./branchStatusCache/";
	}

	public BranchStateCache getCache() {
		return new RocksDbCache(db);
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

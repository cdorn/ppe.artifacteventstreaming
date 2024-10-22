package at.jku.isse.artifacteventstreaming.jena;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.RocksDBFactory;

class BranchCacheTest {

	final String key = "ssfdsdf23f32fsdfdsf";
	
	@Test @Disabled
	void testPrepareCache() throws Exception {
		BranchStateCache cache = new RocksDBFactory("./branchStatusTestCache/").getCache();
		cache.put(key, key);
		var value = cache.get(key);
		assert(value.equals(key));
	}
	
	@Test @Disabled
	void testUponRestart() throws Exception {
		BranchStateCache cache = new RocksDBFactory("./branchStatusTestCache/").getCache();
		var value = cache.get(key);
		assert(value.equals(key));
	}
}

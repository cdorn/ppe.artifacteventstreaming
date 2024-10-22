package at.jku.isse.artifacteventstreaming.jena;

import org.junit.jupiter.api.Test;
import at.jku.isse.artifacteventstreaming.api.BranchStateCache;
import at.jku.isse.artifacteventstreaming.branch.persistence.RocksDBFactory;

class BranchCacheTest {

	final String key = "ssfdsdf23f32fsdfdsf";
	
	@Test 
	void testPrepareCache() throws Exception {
		BranchStateCache cache = new RocksDBFactory().getCache();
		cache.put(key, key);
		var value = cache.get(key);
		assert(value.equals(key));
	}
	
	@Test
	void testUponRestart() throws Exception {
		BranchStateCache cache = new RocksDBFactory().getCache();
		var value = cache.get(key);
		assert(value.equals(key));
	}
}

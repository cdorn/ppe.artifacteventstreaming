package at.jku.isse.artifacteventstreaming.rdf.persistence;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;

public class EHCacheFactory {

	private static final String BRANCH_STATE_KEEPER = "branchStateKeeper";
	private final CacheManager cacheManager;
	
	public EHCacheFactory() {
		cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
				.build();
		cacheManager.init();
	}
	
	public Cache<String, String> getCache() {
		var cache = cacheManager.getCache(BRANCH_STATE_KEEPER, String.class, String.class);
		if (cache == null) {
			cache = cacheManager.createCache(BRANCH_STATE_KEEPER, 
			    CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(10)));
		}
		return cache;
	}
	
}

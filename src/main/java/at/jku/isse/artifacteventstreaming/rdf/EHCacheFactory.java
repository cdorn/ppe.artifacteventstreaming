package at.jku.isse.artifacteventstreaming.rdf;

import org.apache.jena.ext.xerces.util.URI;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;

public class EHCacheFactory {

	private final CacheManager cacheManager;
	
	public EHCacheFactory() {
		cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
				.build();
		cacheManager.init();
	}
	
	public Cache<String, String> getCache() {
		Cache<String, String> cache = cacheManager.createCache("branchStateKeeper", 
			    CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, ResourcePoolsBuilder.heap(10)));
		return cache;
	}
	
}

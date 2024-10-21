package at.jku.isse.artifacteventstreaming.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ServiceFactoryRegistry {

	private Map<String, ServiceFactory> registry = new HashMap<>();
	
	public void register(String serviceTypeURI, ServiceFactory factory) {
		registry.put(serviceTypeURI, factory);
	}
	
	public void unregister(String serviceTypeURI) {
		registry.remove(serviceTypeURI);
	}
	
	public Optional<ServiceFactory> getFactory(String serviceTypeURI) {
		return Optional.ofNullable(registry.get(serviceTypeURI));
	}
}

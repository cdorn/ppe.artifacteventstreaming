package at.jku.isse.artifacteventstreaming.api;

import java.net.URI;

public interface StateKeeperFactory {

	public StateKeeper getStateKeeperFor(URI branchURI) ;
}

package at.jku.isse.artifacteventstreaming.api;

import java.net.URI;

public interface StateKeeperFactory {

	public BranchStateUpdater createStateKeeperFor(URI branchURI) ;
}

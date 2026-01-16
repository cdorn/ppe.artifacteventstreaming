package at.jku.isse.artifacteventstreaming.api;

import java.net.URI;

public interface StateKeeperFactory {

	BranchStateUpdater createStateKeeperFor(URI branchURI) ;
}

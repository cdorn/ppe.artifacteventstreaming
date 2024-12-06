package at.jku.isse.artifacteventstreaming.branch;

import at.jku.isse.artifacteventstreaming.api.TimeStampProvider;

public class SystemTimeStampProvider implements TimeStampProvider {

	@Override
	public long getCurrentTimeStamp() {
		return System.currentTimeMillis();
	}

}

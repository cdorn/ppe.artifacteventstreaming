package at.jku.isse.artifacteventstreaming.replay;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

public interface ReplayEntryCollector {

	List<ReplayEntry> getReplayEntriesInChronologicalOrder(Map<Resource, Set<Property>> replayScope);

	List<ReplayEntry> getPartialReplayEntries(long fromTimeStampIncl, Map<Resource, Set<Property>> replayScope);
	
	
}

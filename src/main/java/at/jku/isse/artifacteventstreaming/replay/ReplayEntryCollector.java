package at.jku.isse.artifacteventstreaming.replay;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

/**
 * @author Christoph Mayr-Dorn
 *
 */
public interface ReplayEntryCollector {

	/**
	 * @param replayScope
	 * @return must return MUTABLE list of replay entries to enable resorting and extension at a later point in time
	 */
	List<ReplayEntry> getReplayEntriesInChronologicalOrder(Map<Resource, Set<Property>> replayScope);

	
	/**
	 * @param fromTimeStampIncl
	 * @param replayScope
	 * @return must return MUTABLE list of replay entries to enable resorting and extension at a later point in time
	 */
	List<ReplayEntry> getPartialReplayEntries(long fromTimeStampIncl, Map<Resource, Set<Property>> replayScope);
	
	
}

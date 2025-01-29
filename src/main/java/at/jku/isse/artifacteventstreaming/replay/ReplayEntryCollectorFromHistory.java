package at.jku.isse.artifacteventstreaming.replay;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.exceptions.PersistenceException;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ReplayEntryCollectorFromHistory implements ReplayEntryCollector {

	private final PerResourceHistoryRepository historyRepo;
	private final String branchURI;
	
	@Override
	public List<ReplayEntry> getReplayEntriesInChronologicalOrder(Map<Resource, Set<Property>> replayScope) {
		return getPartialReplayEntries(-1, replayScope);
	}

	@Override
	public List<ReplayEntry> getPartialReplayEntries(long fromTimeStampIncl, Map<Resource, Set<Property>> replayScope) {
		return replayScope.entrySet().stream()
				.flatMap(entry -> 
					getReplayEntriesOrEmpty(AES.resourceToId(entry.getKey()), branchURI) 
						.filter(replEntry -> entry.getValue().contains(replEntry.getStatement().getContainmentPropertyOrPredicate()) )
				)
				.filter(replEntry -> replEntry.getTimeStamp() >= fromTimeStampIncl)
				.sorted(new ReplayEntry.CompareByTimeStamp())
				.collect(Collectors.toCollection(ArrayList::new));
	}
	
	private Stream<ReplayEntry> getReplayEntriesOrEmpty(String uriOrId, String branchURI) {
		try {
			return historyRepo.getHistoryForResource(uriOrId, branchURI);
		} catch (PersistenceException e) {
			// TODO how to handle this?
			return Stream.empty();
		}
	}
}

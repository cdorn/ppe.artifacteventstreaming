package at.jku.isse.artifacteventstreaming.replay;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ReplaySession {

	private final ReplayEntryCollector collector;
	private final Map<Resource, Set<Property>> replayScope;
	private final Model model;
	
	private int currentNonReplayedEntryPos = 0;
	private List<ReplayEntry> history;
	
	public void revert() {
		history = new LinkedList<>(collector.getReplayEntriesInChronologicalOrder(replayScope)); // history needs to be modifiable for sorting and scope extension
		revert(history);
	}
	
	private void revert(List<ReplayEntry> history) {
		if (history.isEmpty()) return;
		history.sort(new ReplayEntry.CompareByTimeStamp()); // oldest first					
		for (int i = history.size()-1; i>= 0 ; i--) {
			var entry = history.get(i);
			entry.applyBackward(model);
		}
	}
	
	public List<ReplayEntry> playForwardOneTimestamp() {
		if (currentNonReplayedEntryPos >= history.size()) {
			return Collections.emptyList();
		}
		List<ReplayEntry> replayedEntries = new LinkedList<>();
		var nextTimestamp = history.get(currentNonReplayedEntryPos).getTimeStamp();
		while (currentNonReplayedEntryPos < history.size() &&  nextTimestamp == history.get(currentNonReplayedEntryPos).getTimeStamp()) {// as long as changes happened at the same time (as per commit timestamp)
			var entry = history.get(currentNonReplayedEntryPos);
			entry.applyForward(model);
			replayedEntries.add(entry);
			currentNonReplayedEntryPos++;
		} 
		return replayedEntries;
	}
	
	public void addToScope(Map<Resource, Set<Property>> additionalReplayScope) {
		Map<Resource, Set<Property>> filteredReplayScope = new HashMap<>(additionalReplayScope);
		filteredReplayScope.entrySet().stream()
			.forEach(entry -> {
				entry.getValue().removeAll(replayScope.getOrDefault(entry.getKey(), Collections.emptySet())); // remove what we already have
			});
		replayScope.putAll(filteredReplayScope); // add the filtered scope to the replayScope for future checking in this method
		
		var partialHistory = collector.getPartialReplayEntries(history.get(currentNonReplayedEntryPos).getTimeStamp(), filteredReplayScope);
		revert(partialHistory);
		// insert history, there wont be any event earlier that current pos in history (as we asked not to have anything earlier)
		history.addAll(partialHistory);
		history.sort(new ReplayEntry.CompareByTimeStamp()); // resort to oldest first	
	}
}

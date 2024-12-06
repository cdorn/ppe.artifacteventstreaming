package at.jku.isse.artifacteventstreaming.replay;

import java.util.Comparator;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ReplayEntry {

	public enum OPTYPE {ADD, REMOVE}
	
	@Getter private final OPTYPE opType;
	@Getter private final Statement statement;
	@Getter private final String commitId;
	@Getter private final long timeStamp;
	
	public void applyForward(Model model) {
		switch (opType) {
		case ADD:
			model.add(statement);
			break;
		case REMOVE:
			model.remove(statement);
			break;
		default:
			throw new RuntimeException("Unknown opType: "+opType);
		}			
	}
	
	public void applyBackward(Model model) {
		switch (opType) {
		case ADD:
			model.remove(statement);
			break;
		case REMOVE:
			model.add(statement);
			break;
		default:
			throw new RuntimeException("Unknown opType: "+opType);
		}		
	}
	
	public static class CompareByTimeStamp implements Comparator<ReplayEntry> {
		@Override
		public int compare(ReplayEntry o1, ReplayEntry o2) {
			return Long.compare(o1.getTimeStamp(), o2.getTimeStamp());
		}		
	}
}

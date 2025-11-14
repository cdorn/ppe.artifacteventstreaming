package at.jku.isse.artifacteventstreaming.replay;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.jena.rdf.model.Model;

import java.util.Comparator;

@RequiredArgsConstructor
public class ReplayEntry {

	@Getter private final AES.OPTYPE opType;
	@Getter private final ContainedStatement statement;
	@Getter private final String commitId;
	@Getter private final long timeStamp;
	@Getter private final String branchURI;
	
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

	@Override
	public String toString() {
		return "ReplayEntry [" + opType + ", " + statement + ", at=" + timeStamp
				+ ", branchURI=" + branchURI + "]";
	}
	
	
}

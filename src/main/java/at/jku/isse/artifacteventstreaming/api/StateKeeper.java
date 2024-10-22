package at.jku.isse.artifacteventstreaming.api;

import java.io.IOException;
import java.util.List;

import org.apache.jena.ontapi.model.OntModel;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;

public interface StateKeeper {

	public void finishedMerge(Commit commit) throws Exception;
	
	public void beforeServices(Commit commit) throws Exception;
	
	public void afterServices(Commit commit) throws Exception;
	
	public boolean hasSeenCommit(Commit commit);
	
	public List<Commit> getHistory();
	
	public Commit getLastCommit();

	void loadState(OntModel model) throws StreamReadException, DatabindException, IOException;
	
}

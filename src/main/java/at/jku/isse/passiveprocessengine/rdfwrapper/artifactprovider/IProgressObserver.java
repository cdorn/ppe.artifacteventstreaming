package at.jku.isse.passiveprocessengine.rdfwrapper.artifactprovider;

public interface IProgressObserver {	
	public void dispatchNewEntry(ProgressEntry entry);
	public void updatedEntry(ProgressEntry entry); 
}

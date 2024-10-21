package at.jku.isse.artifacteventstreaming.api;

import org.apache.jena.ontapi.model.OntIndividual;

public interface CommitHandler {

	public static final String serviceTypeBaseURI = AES.uri+"ServiceType";
	
	public void handleCommit(Commit commit);
	
	public OntIndividual getConfigResource();
	
}

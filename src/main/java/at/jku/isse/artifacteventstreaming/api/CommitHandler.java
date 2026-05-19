package at.jku.isse.artifacteventstreaming.api;

import org.apache.jena.ontapi.model.OntIndividual;

public interface CommitHandler extends TransactionAware {

	String serviceTypeBaseURI = AES.uri+"ServiceType";
	
	void handleCommit(Commit commit);
	
	OntIndividual getConfigResource();

    String getURI();


}

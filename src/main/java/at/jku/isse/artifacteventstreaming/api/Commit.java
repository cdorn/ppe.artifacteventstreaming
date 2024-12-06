package at.jku.isse.artifacteventstreaming.api;

import java.util.List;
import java.util.Set;

import org.apache.jena.rdf.model.Statement;

public interface Commit {

	String getCommitMessage();

	String getCommitId();

	String getPrecedingCommitId();

	String getOriginatingBranchId();

	void appendAddedStatements(Set<Statement> stmts);

	void appendRemovedStatement(Set<Statement> stmts);

	List<Statement> getAddedStatements();

	List<Statement> getRemovedStatements();
	
	int getAdditionCount();
	
	int getRemovalCount();

	boolean isEmpty();

	void removeEffectlessStatements(int baseAdds, int baseRemoves);

	long getTimeStamp();

}
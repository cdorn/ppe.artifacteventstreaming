package at.jku.isse.artifacteventstreaming.testutils;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Statement;

public class ModelDiff {

	public static void printDiff(OntModel sourceModel, OntModel destModel, boolean verbose) {
		Set<Statement> sourceStmts = new HashSet<>();
		var iterSource = sourceModel.listStatements();
		while(iterSource.hasNext()) {
			sourceStmts.add(iterSource.next());
		}
		Set<Statement> destStmts = new HashSet<>();
		var iterDest = destModel.listStatements();
		while(iterDest.hasNext()) {
			destStmts.add(iterDest.next());
		}
		Set<Statement> missingInSource = destStmts.stream().filter(stmt -> !sourceStmts.contains(stmt)).collect(Collectors.toSet());
		Set<Statement> missingInDest = sourceStmts.stream().filter(stmt -> !destStmts.contains(stmt)).collect(Collectors.toSet());
		
		System.out.println("MISSING IN SOURCE: "+missingInSource.size());	
		if (verbose) {
			missingInSource.stream().forEach(System.out::println);
		}
		System.out.println("MISSING IN DESTINATION: "+missingInDest.size());
		if (verbose) {
			missingInDest.stream().forEach(System.out::println);
		}
	}
	
}

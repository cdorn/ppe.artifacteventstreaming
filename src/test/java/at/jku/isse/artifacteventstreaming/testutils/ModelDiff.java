package at.jku.isse.artifacteventstreaming.testutils;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Statement;

public class ModelDiff {

	public static Entry<Integer, Integer> printDiff(OntModel sourceModel, OntModel destModel, boolean verbose) {		
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
		
		System.out.println(String.format("MISSING IN SOURCE: %s out of %s existing", missingInSource.size(), sourceStmts.size()));	
		if (verbose) {
			missingInSource.stream().forEach(System.out::println);
		}
		System.out.println(String.format("MISSING IN DEST: %s out of %s existing", missingInDest.size(), destStmts.size()));	
		if (verbose) {
			missingInDest.stream().forEach(System.out::println);
		}
		return new AbstractMap.SimpleEntry<Integer,Integer>(missingInSource.size(), missingInDest.size());
	}
	
}

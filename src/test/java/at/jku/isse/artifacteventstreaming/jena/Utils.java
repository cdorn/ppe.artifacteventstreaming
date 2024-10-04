package at.jku.isse.artifacteventstreaming.jena;

import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.reasoner.ValidityReport;

public class Utils {
	public static void printIterator(StmtIterator iter) {
		while (iter.hasNext()) {
			System.out.println(iter.next());
		}
	}
	
	public static void printStatements(Model model, Resource resource, Property property, RDFNode value) {
		StmtIterator iter = model.listStatements(resource, property, value);
		printIterator(iter);
	}
	
	
	public static void printValidation(InfModel infmodel) {
		
		ValidityReport validity = infmodel.validate();
		if (validity.isValid()) {
		    System.out.println("OK");
		} else {
		    System.out.println("Conflicts");
		    for (Iterator i = validity.getReports(); i.hasNext(); ) {
		        System.out.println(" - " + i.next());
		    }
		}
	}

	public static void printStream(Stream<OntProperty> declaredProperties) {
		declaredProperties.forEach(ontProp -> System.out.println(ontProp));
		
	}
}

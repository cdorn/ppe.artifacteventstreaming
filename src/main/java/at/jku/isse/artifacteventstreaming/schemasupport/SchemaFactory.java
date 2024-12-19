package at.jku.isse.artifacteventstreaming.schemasupport;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SchemaFactory {
	
	public OntModel loadOntologyFromFilesystem(String ontologyName) {
		String fullName = "ontologies/"+ontologyName+".ttl" ;
		OntModel model = OntModelFactory.createModel(OntSpecification.OWL2_DL_MEM) ;
		try {
			model.read(fullName);
		} catch(Exception e) {
			log.warn(String.format("Could not read ontology %s from file %s due to %s, returning empty model" , ontologyName, fullName, e.getMessage()));
		}
		return model;
	}
	
	public void writeOntologyToFilesystem(Model model, String ontologyName) {				
		String fullName = "ontologies/"+ontologyName+".ttl" ;		
		try {
			var output = new FileOutputStream(fullName);
			
			RDFDataMgr.write(output, model, Lang.TURTLE);
		} catch (FileNotFoundException e) {
			log.error(String.format("Could not write ontology %s to file %s due to %s" , ontologyName, fullName, e.getMessage()));
		}
		
	}
	
}

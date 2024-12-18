package at.jku.isse.artifacteventstreaming.schemasupport;

import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.Model;

import lombok.Getter;

public class SingleResourceType {
	public static final String SINGLE_NS = "http://at.jku.isse.single#";
	
	public static final String SINGLE_OBJECT_URI = SINGLE_NS+"object";
	public static final String SINGEL_LITERAL_URI = SINGLE_NS+"literal";
	
	private static SingleSchemaFactory factory = new SingleSchemaFactory();
	
	@Getter
	private final OntObjectProperty singleObjectProperty;
	@Getter
	private final OntDataProperty singleLiteralProperty;
	
	public SingleResourceType(OntModel model) {
		factory.addSchemaToModel(model);	
		singleObjectProperty = model.getObjectProperty(SINGLE_OBJECT_URI);
		singleLiteralProperty = model.getDataProperty(SINGEL_LITERAL_URI);
	}
	
	private static class SingleSchemaFactory extends SchemaFactory {
		
		public static final String SINGLEONTOLOGY = "singlevalueontology";
		private final OntModel model;
		
		public SingleSchemaFactory() {
			this.model = loadOntologyFromFilesystem(SINGLEONTOLOGY);			
			initTypes();			
			super.writeOntologyToFilesystemn(model, SINGLEONTOLOGY);
		}				
		
		private void initTypes() {
			var singleObjectProperty = model.getObjectProperty(SINGLE_OBJECT_URI);
			if (singleObjectProperty == null) {
				singleObjectProperty = model.createObjectProperty(SINGLE_OBJECT_URI);
			}
			var singleLiteralProperty = model.getDataProperty(SINGEL_LITERAL_URI);
			if (singleLiteralProperty == null) {
				singleLiteralProperty = model.createDataProperty(SINGEL_LITERAL_URI);
			}
		}

		public void addSchemaToModel(Model modelToAddOntologyTo) {
			modelToAddOntologyTo.add(model);		
		} 
	}
}

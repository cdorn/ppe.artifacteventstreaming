package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;

import lombok.Getter;

public class SingleResourceType {
	public static final String SINGLE_NS = "http://at.jku.isse.single#";
	
	public static final String SINGLE_OBJECT_URI = SINGLE_NS+"#object";
	public static final String SINGEL_LITERAL_URI = SINGLE_NS+"#literal";
	
	@Getter
	private final OntObjectProperty singleObjectProperty;
	@Getter
	private final OntDataProperty singleLiteralProperty;
	
	public SingleResourceType(OntModel model) {
		singleObjectProperty = model.createObjectProperty(SINGLE_OBJECT_URI);
		singleLiteralProperty = model.createDataProperty(SINGEL_LITERAL_URI);
	}
}

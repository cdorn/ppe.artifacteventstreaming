package at.jku.isse.artifacteventstreaming.branch.events;

import java.io.IOException;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.Statement;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;

public class StatementJsonDeserializer extends StdDeserializer<Statement> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final OntModel model;
	
	protected StatementJsonDeserializer(Class<Statement> t, OntModel model) {
		super(t);		
		this.model = model;
	}
	
	@Override
	public Statement deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
		
		ObjectNode node = p.readValueAsTree();
		String subjectURI = node.get(StatementJsonSerializer.SUBJECT).asText();
		String predicatetURI = node.get(StatementJsonSerializer.PREDICATE).asText();
		String value = node.get(StatementJsonSerializer.OBJECT).asText();
		if (node.has(StatementJsonSerializer.DATATYPE) ) { // a literal object
			String datatypeURI = node.get(StatementJsonSerializer.DATATYPE).asText();			
			return model.createStatement(model.createResource(subjectURI), model.createProperty(predicatetURI), model.createTypedLiteral(value, datatypeURI));
		} else { // a resource object			
			return model.createStatement(model.createResource(subjectURI), model.createProperty(predicatetURI), model.createResource(value));
		}
	}

	public static void registerDeserializationModule(ObjectMapper mapper, OntModel model) {
		SimpleModule module = new SimpleModule();
		module.addDeserializer(Statement.class, new StatementJsonDeserializer(Statement.class, model));
		mapper.registerModule(module);
	}

	
}

package at.jku.isse.artifacteventstreaming.branch.events;

import java.io.IOException;

import org.apache.jena.rdf.model.Statement;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.core.JsonGenerator;

public class StatementJsonSerializer extends StdSerializer<Statement> {

	public static final String OBJECT = "object";
	public static final String DATATYPE = "datatype";
	public static final String PREDICATE = "predicate";
	public static final String SUBJECT = "subject";
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected StatementJsonSerializer(Class<Statement> t) {
		super(t);
		
	}

	@Override
	public void serialize(Statement value, JsonGenerator gen, SerializerProvider provider) throws IOException {
		
		gen.writeStartObject();
        gen.writeStringField(SUBJECT, value.getSubject().getURI());		
        gen.writeStringField(PREDICATE, value.getPredicate().getURI());
        if (value.getObject().isLiteral()) {
        	String datatypeURI = value.getObject().asLiteral().getDatatype().getURI();
        	 gen.writeStringField(DATATYPE, datatypeURI);
        	 gen.writeStringField(OBJECT, value.getObject().asLiteral().getLexicalForm());
        } else {
        	gen.writeStringField(OBJECT, value.getObject().asResource().getURI());
        }                		
        gen.writeEndObject();
	}

	public static void registerSerializationModule(ObjectMapper mapper) {
		SimpleModule module = new SimpleModule();
		module.addSerializer(Statement.class, new StatementJsonSerializer(Statement.class));
		mapper.registerModule(module);
	}
	

//	public static void toJson(Commit commit) {
//		ObjectMapper jsonMapper = new JsonMapper().registerModule(null);
//	}



	
	
}

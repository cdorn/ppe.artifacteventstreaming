package at.jku.isse.artifacteventstreaming.branch.serialization;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;

public class StatementJsonSerializer extends StdSerializer<ContainedStatement> {

	public static final String OBJECT = "object";
	public static final String DATATYPE = "datatype";
	public static final String PREDICATE = "predicate";
	public static final String SUBJECT = "subject";
	public static final String CONTAINING_SUBJECT = "containingsubject";
	public static final String CONTAINMENT_PREDICATE = "containmentpredicate";
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	protected StatementJsonSerializer(Class<ContainedStatement> t) {
		super(t);
		
	}

	@Override
	public void serialize(ContainedStatement value, JsonGenerator gen, SerializerProvider provider) throws IOException {
		
		var containmentId = AES.resourceToId(value.getContainerOrSubject());
		var containingProp = value.getContainmentPropertyOrPredicate().getURI();
		var id = AES.resourceToId(value.getSubject());
		var prop = value.getPredicate().getURI();
		gen.writeStartObject();
		if (!id.equals(containmentId)) {			
			gen.writeStringField(CONTAINING_SUBJECT, containmentId);
		}
		gen.writeStringField(SUBJECT, id);		              
		if (!prop.equals(containingProp)) {
			gen.writeStringField(CONTAINMENT_PREDICATE, containingProp);
		}
        gen.writeStringField(PREDICATE, prop);
        if (value.getObject().isLiteral()) {
        	String datatypeURI = value.getObject().asLiteral().getDatatype().getURI();
        	 gen.writeStringField(DATATYPE, datatypeURI);
        	 gen.writeStringField(OBJECT, value.getObject().asLiteral().getLexicalForm());
        } else {
        	gen.writeStringField(OBJECT, AES.resourceToId(value.getObject().asResource()));
        }                		
        gen.writeEndObject();
	}

	public static void registerSerializationModule(ObjectMapper mapper) {
		SimpleModule module = new SimpleModule();
		module.addSerializer(ContainedStatement.class, new StatementJsonSerializer(ContainedStatement.class));
		mapper.registerModule(module);
	}
	
}

package at.jku.isse.artifacteventstreaming.branch.serialization;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;


import org.apache.jena.rdf.model.Resource;

import java.io.IOException;

public class StatementJsonSerializer extends StdSerializer<ContainedStatement> {

	public static final String OBJECT = "object";
	public static final String DATATYPE = "datatype";
	public static final String PREDICATE = "predicate";
	public static final String SUBJECT = "subject";
	public static final String CONTAINING_SUBJECT = "containingsubject";
	public static final String CONTAINMENT_PREDICATE = "containmentpredicate";
	/** Prefix marking an id as blank node label (cannot start a valid URI, as a scheme must begin with a letter). */
	public static final String BLANK_NODE_PREFIX = "_:";
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	protected StatementJsonSerializer(Class<ContainedStatement> t) {
		super(t);
		
	}

	@Override
	public void serialize(ContainedStatement value, JsonGenerator gen, SerializerProvider provider) throws IOException {
		
		var containmentId = markedId(value.getContainerOrSubject());
		var containingProp = value.getContainmentPropertyOrPredicate().getURI();
		var id = markedId(value.getSubject());
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
        	gen.writeStringField(OBJECT, markedId(value.getObject().asResource()));
        }
        gen.writeEndObject();
	}

	static String markedId(Resource res) {
		return res.isAnon() ? BLANK_NODE_PREFIX + res.getId().toString() : AES.resourceToId(res);
	}

	public static void registerSerializationModule(ObjectMapper mapper) {
		SimpleModule module = new SimpleModule();
		module.addSerializer(ContainedStatement.class, new StatementJsonSerializer(ContainedStatement.class));
		mapper.registerModule(module);
	}
	
}

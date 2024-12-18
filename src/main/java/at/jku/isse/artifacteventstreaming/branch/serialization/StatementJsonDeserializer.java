package at.jku.isse.artifacteventstreaming.branch.serialization;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class StatementJsonDeserializer extends StdDeserializer<Statement> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final transient Model model = ModelFactory.createDefaultModel();
	
	protected StatementJsonDeserializer(Class<Statement> t) {
		super(t);			
	}
	
	@Override
	public Statement deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		
		ObjectNode node = p.readValueAsTree();
		String subjectId = node.get(StatementJsonSerializer.SUBJECT).asText();
		
		
		String predicatetURI = node.get(StatementJsonSerializer.PREDICATE).asText();
		String value = node.get(StatementJsonSerializer.OBJECT).asText();
		if (node.has(StatementJsonSerializer.DATATYPE) ) { // a literal object
			String datatypeURI = node.get(StatementJsonSerializer.DATATYPE).asText();			
			return model.createStatement(createResourceFromId(subjectId), model.createProperty(predicatetURI), model.createTypedLiteral(value, datatypeURI));
		} else { // a resource object			
			return model.createStatement(createResourceFromId(subjectId), model.createProperty(predicatetURI), createResourceFromId(value));
		}
	}

	public static void registerDeserializationModule(ObjectMapper mapper) {
		SimpleModule module = new SimpleModule();
		module.addDeserializer(Statement.class, new StatementJsonDeserializer(Statement.class));
		mapper.registerModule(module);
	}

	private Resource createResourceFromId(String id) {
		if (isValidURL(id)) {
			return model.createResource(id);
		} else {
			return model.createResource(AnonId.create(id));
		}
	}
	
	boolean isValidURL(String url)  {
		return url.startsWith("http"); //FIXME: better but performant way to check this.
//	    try {
//	    	 var uri = new URI(url);	    
//		     uri.toURL();
//		     return true;	   
//	    } catch (URISyntaxException | MalformedURLException e) {
//	        return false;
//	    }
	}
	
}

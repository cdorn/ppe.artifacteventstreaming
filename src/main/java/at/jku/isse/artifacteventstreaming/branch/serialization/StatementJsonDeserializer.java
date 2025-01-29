package at.jku.isse.artifacteventstreaming.branch.serialization;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;

import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.replay.ContainedStatementImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatementJsonDeserializer extends StdDeserializer<ContainedStatement> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final transient Model model = ModelFactory.createDefaultModel();
	
	protected StatementJsonDeserializer(Class<ContainedStatement> t) {
		super(t);			
	}
	
	@Override
	public ContainedStatement deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
		
		ObjectNode node = p.readValueAsTree();
		var subjectId = node.get(StatementJsonSerializer.SUBJECT).asText();
		var containmentRes = getResourceFromNodeOrNull(node.get(StatementJsonSerializer.CONTAINING_SUBJECT));
		var containmentPred = getPropertyFromNodeOrNull(node.get(StatementJsonSerializer.CONTAINMENT_PREDICATE));
		var predicatetURI = node.get(StatementJsonSerializer.PREDICATE).asText();
		var value = node.get(StatementJsonSerializer.OBJECT).asText();
		Statement stmt = null;
		if (node.has(StatementJsonSerializer.DATATYPE) ) { // a literal object
			String datatypeURI = node.get(StatementJsonSerializer.DATATYPE).asText();			
			stmt = model.createStatement(createResourceFromId(subjectId), model.createProperty(predicatetURI), model.createTypedLiteral(value, datatypeURI));					
		} else { // a resource object			
			stmt = model.createStatement(createResourceFromId(subjectId), model.createProperty(predicatetURI), createResourceFromId(value));
		}		
		return new ContainedStatementImpl(stmt, containmentRes, containmentPred);		
	}

	private Property getPropertyFromNodeOrNull(JsonNode possiblyNull) {
		if (possiblyNull == null) return null;
		else return model.createProperty(possiblyNull.asText());
	}

	private Resource getResourceFromNodeOrNull(JsonNode possiblyNull) {
		if (possiblyNull == null) return null;
		else return createResourceFromId(possiblyNull.asText());
	}

	private Resource createResourceFromId(String id) {
		if (isValidURL(id)) {
			return model.createResource(id);
		} else {
			return model.createResource(AnonId.create(id));
		}
	}
	
	boolean isValidURL(String url)  {
		if (url == null) { // should never happen
			log.error("Data persistence error: statement deserialization returned null url");
			return false;
		}
		return url.startsWith("http"); //FIXME: better but performant way to check this.
//	    try {
//	    	 var uri = new URI(url);	    
//		     uri.toURL();
//		     return true;	   
//	    } catch (URISyntaxException | MalformedURLException e) {
//	        return false;
//	    }
	}
	
	public static void registerDeserializationModule(ObjectMapper mapper) {
		SimpleModule module = new SimpleModule();
		module.addDeserializer(ContainedStatement.class, new StatementJsonDeserializer(ContainedStatement.class));
		mapper.registerModule(module);
	}
}

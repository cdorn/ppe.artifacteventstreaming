package at.jku.isse.artifacteventstreaming.rdfwrapper;

import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.PrimitiveTypesFactory;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import lombok.Getter;

public class TestSchema {
	
public static final String NS = "http://at.jku.isse/testrepos/testschema#";
	
	public enum CoreProperties {key, state, priority, requirements, config, derivedBugs, parent;
			
		@Override
		public String toString() {
			return NS+name();
		}
		
		public String getURI() {
			return NS+name();
		}
	}
	
	final NodeToDomainResolver resolver;
	final PrimitiveTypesFactory primitives;
	@Getter final RDFInstanceType issueType;
	@Getter final RDFInstanceType taskType;
	
	
	public TestSchema(NodeToDomainResolver resolver) {
		this.resolver = resolver;
		this.primitives = resolver.getMetaschemata().getPrimitiveTypesFactory();
		issueType = resolver.createNewInstanceType(NS+"Issue");
		issueType.createSinglePropertyType(CoreProperties.key.getURI(), primitives.getStringType());
		issueType.createSinglePropertyType(CoreProperties.state.getURI(), primitives.getStringType());
		issueType.createSetPropertyType(CoreProperties.requirements.getURI(), issueType.getAsPropertyType());
		issueType.createMapPropertyType(CoreProperties.config.getURI(), issueType.getAsPropertyType());
		issueType.createSetPropertyType(CoreProperties.derivedBugs.getURI(), issueType.getAsPropertyType());
		
		taskType = resolver.createNewInstanceType(NS+"Task", issueType);
		issueType.createSinglePropertyType(CoreProperties.parent.getURI(), issueType.getAsPropertyType());
	}
	
}

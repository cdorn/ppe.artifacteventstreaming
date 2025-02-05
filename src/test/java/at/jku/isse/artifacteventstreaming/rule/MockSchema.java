package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.vocabulary.XSD;

import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import lombok.Getter;
import lombok.NonNull;

public class MockSchema {

	public static final String TEST_SCHEMA_URI = "http://at.jku.isse.artifacteventstreaming/types#";
	public static enum States { Open, InProgress, Closed, ReadyForReview, Released}
	
	@Getter public OntClass issueType;
	@Getter	public OntDataProperty keyProperty;
	@Getter	public OntDataProperty stateProperty;
	@Getter	public OntDataProperty priorityProperty;
	@Getter public OntObjectProperty requirementsProperty;
	@Getter public OntObjectProperty bugsProperty;
	@Getter public OntObjectProperty upstreamProperty;
	@Getter public OntObjectProperty downstreamProperty;
	@Getter public OntObjectProperty parentProperty;
	@Getter public OntObjectProperty labelProperty;
	@Getter public OntObjectProperty keyValueProperty;
	
	//private final PropertyCardinalityTypes schemaUtils;
	
	public MockSchema(@NonNull OntModel model, @NonNull MetaModelSchemaTypes schemaUtils) {				
		issueType = model.createOntClass(TEST_SCHEMA_URI+"Issue");
		
		keyProperty = schemaUtils.createSingleDataPropertyType(TEST_SCHEMA_URI+"key", issueType, model.getDatatype(XSD.xstring));
		stateProperty = schemaUtils.createSingleDataPropertyType(TEST_SCHEMA_URI+"state", issueType, model.getDatatype(XSD.xstring));
		priorityProperty = schemaUtils.createSingleDataPropertyType(TEST_SCHEMA_URI+"priority", issueType, model.getDatatype(XSD.xint));
		
		requirementsProperty = schemaUtils.createBaseObjectPropertyType(TEST_SCHEMA_URI+"requirements", issueType, issueType);  
		bugsProperty = schemaUtils.createBaseObjectPropertyType(TEST_SCHEMA_URI+"bugs", issueType, issueType);
		upstreamProperty = schemaUtils.createBaseObjectPropertyType(TEST_SCHEMA_URI+"upstream", issueType, issueType);
		downstreamProperty = schemaUtils.createBaseObjectPropertyType(TEST_SCHEMA_URI+"downstream", issueType, issueType);
		
		parentProperty = schemaUtils.createSingleObjectPropertyType(TEST_SCHEMA_URI+"downstream", issueType, issueType);
		labelProperty = schemaUtils.getListType().addLiteralListProperty(issueType, TEST_SCHEMA_URI+"label", model.getDatatype(XSD.xstring));
		keyValueProperty = schemaUtils.getMapType().addLiteralMapProperty(issueType, TEST_SCHEMA_URI+"keyValue", model.getDatatype(XSD.xint));
	}
	
	public OntIndividual createIssue(String name, OntIndividual... reqs) {
		var issue = issueType.createIndividual(TEST_SCHEMA_URI+name);
		issue.addProperty(keyProperty, name);
		issue.addProperty(stateProperty, States.Open.toString());
		
		for (OntIndividual req : reqs) {
			issue.addProperty(requirementsProperty.asProperty(), req);
		}		
		return issue;
	}	
	
	
	public RDFRuleDefinition getRegisteredRuleRequirementsSizeGT1(int counter, RuleRepository repo) throws RuleException {
		return repo.getRuleBuilder()
				.withContextType(issueType)
				.withDescription("TestRuleDescription-"+counter)
				.withRuleTitle("RequirementsSizeGT1-"+counter)
				.withRuleExpression("self.requirements.size() > 1")
				.build();
	}
	
}

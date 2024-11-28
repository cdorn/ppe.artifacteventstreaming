package at.jku.isse.artifacteventstreaming.rule;

import java.util.UUID;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;

import at.jku.isse.designspace.rule.arl.evaluator.ModelAccess;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RuleDefinitionBuilder {

	private final RuleSchemaProvider factory;
	
	private OntClass contextType;
	private String ruleExpression;
	private String ruleURI;
	private String description;
	private String title;
	
	public RuleDefinitionBuilder withContextType(@NonNull OntClass contextType) {
		this.contextType = contextType;
		return this;
	}
	
	public RuleDefinitionBuilder withRuleExpression(@NonNull String ruleExpression) {
		this.ruleExpression = ruleExpression;
		return this;
	}
	
	public RuleDefinitionBuilder withRuleURI(@NonNull String ruleURI) {
		this.ruleURI = ruleURI;
		return this;
	}
	
	public RuleDefinitionBuilder withDescription(@NonNull String description) {
		this.description = description;
		return this;
	}
	
	public RuleDefinitionBuilder withRuleTitle(@NonNull String title) {
		this.title = title;
		return this;
	}
	
	public RDFRuleDefinition build() throws RuleException {
		if (contextType == null)
			throw new RuleException("Rule context type must not remain undefined");
		if (ruleExpression == null)
			throw new RuleException("Rule expression cannot remain empty");
		
		if (ruleURI == null) {
			ruleURI = RuleSchemaFactory.uri+UUID.randomUUID();
		}
		// we create the evaluation type and add as super type the rule definition
		
		// treat this as a type definition 
		var ruleEvalType = factory.getDefinitionType().getModel().createOntClass(ruleURI);
		ruleEvalType.addSuperClass(factory.getResultBaseType());
		// then treat this as a instance of definition
		var ruleDef = factory.getDefinitionType().getModel().createIndividual(ruleURI);
		RDFRuleDefinitionImpl rule = new RDFRuleDefinitionImpl(ruleDef, factory, ruleExpression, contextType);
		if (description != null)
			rule.setDescription(description);
		if (title != null)
			rule.setName(title);
		
		return rule;
	}
}

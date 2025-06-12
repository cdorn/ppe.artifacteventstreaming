package at.jku.isse.artifacteventstreaming.rule.definition;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;

import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import lombok.NonNull;

public class RuleDefinitionFactory {

	public static RDFRuleDefinition rebuildRDFRuleDefinitionImpl(@NonNull OntObject ruleDefinition
								, @NonNull RuleSchemaProvider factory) {
		if (ruleDefinition.canAs(OntIndividual.class)) {
			var indiv = ruleDefinition.as(OntIndividual.class);
			if (indiv.hasOntClass(factory.getDerivedPropertyRuleType(), true)) {
				var def = new DerivedPropertyRuleDefinition(ruleDefinition, factory);
				return def.reloadContextAndExpressionSuccessful() ? def : null;
			}	else {
				var def = new RDFRuleDefinitionImpl(ruleDefinition, factory);
				return def.reloadContextAndExpressionSuccessful() ? def : null;
			}
		} else {
			return null;
		}
	}

}

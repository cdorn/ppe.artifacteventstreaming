package at.jku.isse.artifacteventstreaming.rule.definition;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntRelationalProperty;

import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.designspace.rule.arl.parser.ArlType;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DerivedPropertyRuleDefinition extends RDFRuleDefinitionImpl {

	private final OntRelationalProperty derivedPredicate;
	@Getter private ArlType derivedType;
	
	public DerivedPropertyRuleDefinition(@NonNull OntObject ruleDefinition, @NonNull RuleSchemaProvider factory,
			@NonNull String expression, @NonNull OntClass contextType, @NonNull OntRelationalProperty derivedPredicate) {
		super(ruleDefinition, factory, expression, contextType);

		this.derivedPredicate = derivedPredicate;
		ontObject.removeAll(factory.getDerivedPredicateProperty().asProperty())
		.addProperty(factory.getDerivedPredicateProperty().asProperty(), derivedPredicate);

		
		var error = getRuleError();
		if ((error == null || error.isEmpty()) && !isRuleOutputCompatibleWithProperty()) {
			setExpressionError("Rule output incompatible with derived property");
		}
	}

	protected DerivedPropertyRuleDefinition(@NonNull OntObject ruleDefinition, @NonNull RuleSchemaProvider factory) {
		super(ruleDefinition, factory);
		derivedPredicate = getDerivedPredicate();
	}

	
	public OntRelationalProperty getDerivedPredicate() {
		var stmt = ontObject.getProperty(factory.getDerivedPredicateProperty().asProperty());
		return stmt != null ? stmt.getResource().as(OntRelationalProperty.class) : null;
	}
	
	@Override
	protected boolean reloadContextAndExpressionSuccessful() {
		if (derivedPredicate == null) {
			log.warn("Cannot load rule definition from Rule Definition "+ontObject.getURI()+" as it does not have a rule expression");			
			return false;
		}
		if (!isRuleOutputCompatibleWithProperty()) {
			log.warn("Cannot load rule definition from Rule Definition "+ontObject.getURI()+" as rule output is incompatible with derived predicate");			
			return false;
		}
		return super.reloadContextAndExpressionSuccessful();
	}

	@Override
	public String toString() {
		return "DerivedPropertyRuleDefinition [Name=" + getName() + ", CtxType=" + getContextType()
				+ ", derivedPredicate=" + derivedPredicate + ", Expr=" + getRuleExpression() 
				+ ", ExprErr()=" + getExpressionError() + "]";
	}
	
	private boolean isRuleOutputCompatibleWithProperty() {
		//implement check if the property can actually take the rule's output data type, 
		// e.g., String -> String, bool -> bool, Set -> Set (and set content) etc
		var arlType = factory.getModelAccess().getArlTypeOfProperty(derivedPredicate);		
		var ruleResultType = getSyntaxTree().getResultType();
		derivedType = arlType;
		return ruleResultType.conformsTo(arlType);
	}
}

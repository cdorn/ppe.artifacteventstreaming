package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.designspace.rule.arl.exception.ParsingException;
import at.jku.isse.designspace.rule.arl.expressions.Expression;
import at.jku.isse.designspace.rule.arl.expressions.RootExpression;
import at.jku.isse.designspace.rule.arl.parser.ArlParser;
import at.jku.isse.designspace.rule.arl.parser.ArlType;
import lombok.NonNull;


public class RuleDefinitionImpl implements RDFRuleDefinition {

	private final OntObject ontObject;
	private final RuleFactory factory;
	private Expression<Object> syntaxTree;
	
	protected RuleDefinitionImpl(@NonNull OntObject ruleDefinition
			, @NonNull RuleFactory factory
			, @NonNull String expression
			, @NonNull OntClass contextType) {
		this.ontObject = ruleDefinition;
		this.factory = factory;
		setContextType(contextType);
		setRuleExpression(expression);
	}
	
	protected RuleDefinitionImpl(@NonNull OntObject ruleDefinition
			, @NonNull RuleFactory factory) {
			this.ontObject = ruleDefinition;
			this.factory = factory;					
			reloadContextAndExpression();
	}
	
	protected void reloadContextAndExpression() {
		var expression = getRuleExpression();
		if (expression == null || expression.length() == 0) {
			setExpressionError("Cannot load rule definition from Rule Definition "+ontObject.getURI()+" as it does not have a rule expression");				
		} else {
			var contextType = getRDFContextType();
			if (contextType == null) {
				setExpressionError("Cannot load rule definition from Rule Definition "+ontObject.getURI()+" as it does not reference a context type");
			} else {
				generateSyntaxTree(expression);
			}
		}
	}
	
	private void setContextType(OntClass contextType) {
		ontObject.removeAll(factory.getContextTypeProperty().asProperty())
			.addProperty(factory.getContextTypeProperty().asProperty(), contextType);
	}
	
	@Override
	public void setRuleExpression(@NonNull String expression) {
		ontObject.removeAll(factory.getExpressionProperty().asProperty())
				.addProperty(factory.getExpressionProperty().asProperty(), expression);
		var contextType = getRDFContextType();		
		if (contextType == null) {
			setExpressionError("Cannot load syntax tree for Rule Definition "+ontObject.getURI()+" as it does not reference a context type");
		} else {
			generateSyntaxTree(expression);
		}
	}
	
	@Override
	public OntClass getRuleDefinition() {
		return this.ontObject.as(OntClass.class);
	}
	
	private void generateSyntaxTree(@NonNull String expression) {		
		
		ArlType contextType = this.getContextType(); 
		ArlParser parser = new ArlParser();
        try {
            syntaxTree = new RootExpression((Expression) parser.parse(expression, contextType, null));
            setExpressionError("");
        }
        catch (ParsingException ex) {
        	syntaxTree = null;
        	setExpressionError(String.format("Parsing error in \"%s\": %s (Line=%d, Column=%d)", expression, ex.toString(), parser.getLine(), parser.getColumn()));
        }
        catch (Exception ex) {
            ex.printStackTrace();
            syntaxTree = null;
            setExpressionError(String.format("Error in \"%s\": %s (", expression, ex.toString()));
        }
	}		
	
	@Override
	public String getRuleExpression() {
		var stmt = ontObject.getProperty(factory.getExpressionProperty());
		return stmt != null ? stmt.getString() : null;
	}

	private void setExpressionError(String error) {
		ontObject.removeAll(factory.getExpressionErrorProperty())
			.addLiteral(factory.getExpressionErrorProperty(), error);
	}
	
	@Override
	public String getExpressionError() {
		var stmt = ontObject.getProperty(factory.getExpressionErrorProperty());
		return stmt != null ? stmt.getString() : null;
	}

	@Override
	public void setName(String description) {
		ontObject.removeAll(RDFS.label)
			.addLiteral(RDFS.label,description);
	}

	@Override
	public String getName() {
		var stmt = ontObject.getProperty(RDFS.label);
		return stmt != null ? stmt.getString() : null;
	}
	
	@Override
	public void setDescription(String description) {
		ontObject.removeAll(RDFS.comment)
			.addLiteral(RDFS.comment,description);
	}

	@Override
	public String getDescription() {
		var stmt = ontObject.getProperty(RDFS.comment);
		return stmt != null ? stmt.getString() : null;
	}

	public boolean hasExpressionError() {
		return !(getExpressionError() == null || getExpressionError().length() == 0);
	}

	@Override
	public OntClass getRDFContextType() {
		var res = ontObject.getPropertyResourceValue(factory.getContextTypeProperty().asProperty());
		if (res != null && res.canAs(OntClass.class)) {
			return res.as(OntClass.class);
		} else {
			return null; // can only happen when type is removed but this rule not yet
		}
	}
	
    @Override
    public Expression<Object> getSyntaxTree() {
        return this.syntaxTree;
    }
    
	@Override
	public void delete() {		
		ontObject.removeProperties();
		syntaxTree = null;
	}

    // methods to match ARL specific interfaces
	@Override
	public String getRule() {
		return getRuleExpression();
	}

	@Override
	public void setRule(String rule) {
		this.setRuleExpression(rule);
	}

	@Override
	public ArlType getContextType() {
		return ArlType.get(ArlType.TypeKind.INSTANCE, ArlType.CollectionKind.SINGLE, this.getRDFContextType());	
	}

	@Override
	public String getRuleError() {
		return getExpressionError();
	}

	@Override
	public String toString() {
		return "RuleDefinitionImpl [Name=" + getName() + ", CtxType=" + getContextType() + ", Expr=" + getRuleExpression() + ", ExpErr()="
				+ getExpressionError() + ", Name=" + getName() + ", CtxType=" + getContextType() + "]";
	}

	
}

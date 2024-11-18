package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.designspace.rule.arl.exception.ParsingException;
import at.jku.isse.designspace.rule.arl.expressions.Expression;
import at.jku.isse.designspace.rule.arl.expressions.RootExpression;
import at.jku.isse.designspace.rule.arl.parser.ArlParser;
import at.jku.isse.designspace.rule.arl.parser.ArlType;
import lombok.NonNull;


public class RuleDefinitionImpl implements RuleDefinition {

	private final OntIndividual ontObject;
	private final RuleFactory factory;
	private Expression<Object> syntaxTree;
	
	protected RuleDefinitionImpl(@NonNull OntIndividual ruleDefinition
			, @NonNull RuleFactory factory
			, @NonNull String expression
			, @NonNull OntClass contextType) {
		this.ontObject = ruleDefinition;
		this.factory = factory;
		setContextType(contextType);
		setRuleExpression(expression);
	}
	
	private void setContextType(OntClass contextType) {
		ontObject.removeAll(factory.getContextTypeProperty().asProperty())
			.addProperty(factory.getContextTypeProperty().asProperty(), contextType);
	}
	
	@Override
	public void setRuleExpression(@NonNull String expression) {
		ontObject.removeAll(factory.getExpressionProperty().asProperty())
		.addProperty(factory.getExpressionProperty().asProperty(), expression);
		generateSyntaxTree(expression, getContextType());
	}
	
	private void generateSyntaxTree(@NonNull String expression, @NonNull OntClass context) {
		ArlType contextType =  ArlType.get(ArlType.TypeKind.INSTANCE, ArlType.CollectionKind.SINGLE, context);	
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
	public void setTitle(String description) {
		ontObject.removeAll(RDFS.label)
			.addLiteral(RDFS.label,description);
	}

	@Override
	public String getTitle() {
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
	public OntClass getContextType() {
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
}

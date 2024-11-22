package at.jku.isse.artifacteventstreaming.rule;

import java.util.Map.Entry;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluation;
import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluationImpl;
import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import lombok.Getter;
import lombok.NonNull;

public class RuleEvaluationWrapperResourceImpl implements RuleEvaluationWrapperResource {

	@Getter
	private final OntIndividual ruleEvalObj;
	private final RuleFactory factory;
	private final OntIndividual contextInstance;
	private final RuleEvaluation delegate;
	private Object result = null;
	
	// either create from underlying ontclass or create new
	
	public static RuleEvaluationWrapperResourceImpl create(@NonNull RuleFactory factory, @NonNull RDFRuleDefinition def, @NonNull OntIndividual contextInstance) {
		var evalObj = def.getRuleDefinition().createIndividual();		
		return new RuleEvaluationWrapperResourceImpl(def, evalObj, contextInstance, factory);
	}
	
	public static RuleEvaluationWrapperResourceImpl loadFromModel(@NonNull OntIndividual ruleEvalObj, @NonNull RuleFactory factory, @NonNull RuleRepository ruleRepo) throws EvaluationException {
		// check if an rule eval instance
		var type = getRuleTypeClass(ruleEvalObj, factory); 							
		// check if has context
		var ctx = getContextInstanceFrom(ruleEvalObj, factory);		
		// check if has definition, if so fetch from factory
		var def = resolveDefinition(type, ruleRepo);
		// dont evaluate unless evaluate() is called or access to result (lazy loading/generation of eval result)
		return new RuleEvaluationWrapperResourceImpl(def, ruleEvalObj, ctx, factory);
	}
	
	private static OntClass getRuleTypeClass(@NonNull OntIndividual ruleEvalObj, @NonNull RuleFactory factory) throws EvaluationException {
		var result = ruleEvalObj.classes(true)
				.filter(type -> factory.getResultBaseType().hasSubClass(type, false)).findAny(); // there should be only one
		if (result.isEmpty()) {
			throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as it is not a subclass of %s", ruleEvalObj, factory.getResultBaseType().getURI()));
		} else {
			return result.get();
		}
	}
	
	private static OntIndividual getContextInstanceFrom(@NonNull OntIndividual ruleEvalObj, @NonNull RuleFactory factory) throws EvaluationException {
		var stmt = ruleEvalObj.getProperty(factory.getContextElementProperty().asProperty());
		if (stmt == null)
			throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as it doesn't reference a context instance element via %s", ruleEvalObj, factory.getContextElementProperty().getURI()));
		if (stmt.getObject().isResource()) {
			var res = stmt.getResource();
			if (res.canAs(OntIndividual.class)) {
				return res.as(OntIndividual.class);
			} else {
				throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as its referenced context instance %s is not an ontindividual", ruleEvalObj.getURI(), res));
			}			
		} else {
			throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as it doesn't reference a context instance element via %s but a literal", ruleEvalObj, factory.getContextElementProperty().getURI()));
		}
	}
	
	private static RDFRuleDefinition resolveDefinition(@NonNull OntClass ruleDef, @NonNull RuleRepository repo) throws EvaluationException {
		var def = repo.findRuleDefinitionForResource(ruleDef);
		if (def == null)
			throw new EvaluationException(String.format("Cannot obtain rule definition for entity %s", ruleDef));
		else 
			return def;
	}
	
	private RuleEvaluationWrapperResourceImpl(RDFRuleDefinition def, OntIndividual ruleEvalObj, OntIndividual contextInstance, RuleFactory factory ) {
		super();
		this.ruleEvalObj = ruleEvalObj;
		this.contextInstance = contextInstance;
		this.factory = factory;
		this.delegate = new RuleEvaluationImpl(def, contextInstance);
		setEnabledIfNotStatusAvailable();
		setContextIfNotSetYet(contextInstance);
		addAddRuleEvaluationToNewOrExistingScope(contextInstance, ruleEvalObj); // just to make sure that the context scope is set (no effect if already so)
	}
	
	private void setEnabledIfNotStatusAvailable() {
		var stmt = ruleEvalObj.getProperty(factory.getIsEnabledProperty());
		if (stmt == null) // not set, thus enable
			setEnabledStatus(true);
	}
	
	private void setContextIfNotSetYet(OntIndividual context) {
		var stmt = ruleEvalObj.getProperty(factory.getContextElementProperty().asProperty());
		if (stmt == null) {
			ruleEvalObj.addProperty(factory.getContextElementProperty().asNamed(), context);
		}
	}
	
	private void addAddRuleEvaluationToNewOrExistingScope(OntIndividual subject, OntIndividual ruleEval) {
		OntIndividual scope = null;
		var iter = subject.listProperties(factory.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var propStmt = stmt.getResource().getProperty(factory.getUsingPredicateProperty().asProperty());
			if (propStmt == null ) {
				//is this the scope collection to be used for this instance serving as context to rules
				scope = stmt.getResource().as(OntIndividual.class);
				iter.close();
				break;
			}
		}
		if (scope == null) {
			scope = createScopeSkeleton(subject);
		}
		scope.addProperty(factory.getUsedInRuleProperty().asNamed(), ruleEval);		
	}
	
	/**
	 * If rule is disabled, return stale result, if any.
	 * If evaluation has error, returns null
	 * result itself is not persisted, just whether rule is consistent or not (or persisted as null if not applicable)
	 */
	@Override
	public Object evaluate() {
		if (!isEnabled()) return result;
		
		result = delegate.evaluate();
		var error = delegate.getError();
		if (error != null) {			
			ruleEvalObj.removeAll(factory.getEvaluationErrorProperty())
			.addLiteral(factory.getEvaluationErrorProperty(), error);
			ruleEvalObj.removeAll(factory.getEvaluationHasConsistentResultProperty())
			.addLiteral(factory.getEvaluationHasConsistentResultProperty(), Boolean.FALSE);			
		} else {
			ruleEvalObj.removeAll(factory.getEvaluationErrorProperty());
			ruleEvalObj.removeAll(factory.getEvaluationHasConsistentResultProperty());
			if (result instanceof Boolean boolResult) {
				ruleEvalObj.addLiteral(factory.getEvaluationHasConsistentResultProperty(), boolResult); 	
			}
			updateRuleScope();
		}				
		return result;
	}

	private void updateRuleScope() {
		delegate.getAddedScopeElements().stream()		
			.forEach(scopeEntry -> addPropertyToScope(scopeEntry, ruleEvalObj));
		delegate.getRemovedScopeElements().stream()		
		.forEach(scopeEntry -> removePropertyFromScope(scopeEntry, ruleEvalObj));                                       
	}
	
	private void addPropertyToScope(Object entry, OntIndividual ruleEval) {
		var typed = (Entry) entry;
		var subject = (Resource) typed.getKey();
		var property = (Property) typed.getValue();
		
		OntIndividual scope = findScope(subject,  property);						
		if (scope == null) {
			// ensure individual has scopeCollection			
			scope = createScopeSkeleton(subject);
			scope.addProperty(factory.getUsingPredicateProperty().asNamed(), property);
			scope.addProperty(factory.getUsedInRuleProperty().asNamed(), ruleEval);
			// now link up eval to scope part			
			ruleEval.addProperty(factory.getHavingScopePartProperty().asNamed(), scope); 
			//TODO: currently this doesn work yet via inverseProperty definition of usedInRuleProperty, hence manually
		} else { //scope exists already for this property and instance, just set the rule usage 
			scope.addProperty(factory.getUsedInRuleProperty().asNamed(), ruleEval);
		}
	}		
	
	private void removePropertyFromScope(Object entry, OntIndividual ruleEval) {
		var typed = (Entry) entry;
		var subject = (Resource) typed.getKey();
		var property = (Property) typed.getValue();
		
		OntIndividual scope = findScope(subject,  property);						
		if (scope == null) {
			//there was no scope recorded before, nothing to do,			
		} else { //scope exists already for this property and instance, just remove the rule usage 		
			scope.remove(factory.getUsedInRuleProperty().asProperty(), ruleEval);
			//TODO: currently we need to remove this from the rule as well as inverse-of property does not work yet		
			ruleEval.remove(factory.getHavingScopePartProperty().asProperty(), scope);
		}
	}
	
	private OntIndividual findScope(Resource subject, Property property) {
		OntIndividual scope = null;
		var iter = subject.listProperties(factory.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var propStmt = stmt.getResource().getProperty(factory.getUsingPredicateProperty().asProperty());
			if (propStmt != null && propStmt.getResource().getURI().equals(property.getURI())) {
				//is this the scope collection for this particular property
				scope = stmt.getResource().as(OntIndividual.class);
				iter.close();
				break;
			}
		}
		return scope;
	}
	

	
	private OntIndividual createScopeSkeleton(Resource subject) {
		var scope = factory.getRuleScopeCollection().createIndividual();
		scope.addProperty(factory.getUsingElementProperty().asNamed(), subject);
		subject.addProperty(factory.getHasRuleScope().asProperty(), scope);
		return scope;
	}
	
	@Override
	public boolean isConsistent() {
		var stmt = ruleEvalObj.getProperty(factory.getEvaluationHasConsistentResultProperty());
		return stmt != null ? stmt.getBoolean() : Boolean.TRUE; // if never has been set or if not a boolean result but no error, then True
	}		

	@Override
	public String getEvaluationError() {
		var stmt = ruleEvalObj.getProperty(factory.getEvaluationErrorProperty());
		return stmt != null ? stmt.getString() : "";
	}

	@Override
	public Object getEvaluationResult() {
		if (result == null && getEvaluationError().length() == 0) { // never evaluated so far
			return evaluate();
		} else {
			return result;
		}
	}

	@Override
	public OntIndividual getContextInstance() {
		return contextInstance;
	}

	@Override
	public boolean isEnabled() {
		var stmt = ruleEvalObj.getProperty(factory.getIsEnabledProperty());
		return stmt != null ? stmt.getBoolean() : Boolean.FALSE; //if not set, then assumed false, because when we delete, we wont have any statements, hence need to assume disabled
	}
	
	private void setEnabledStatus(boolean status) {
		ruleEvalObj.removeAll(factory.getIsEnabledProperty())
		.addLiteral(factory.getIsEnabledProperty(), status);	
	}
	

	
	@Override
	public void enable() {
		if (!isEnabled()) {
			setEnabledStatus(true);
		}
	}

	@Override
	public void disable() {
		if (isEnabled()) {
			setEnabledStatus(false);
		}
	}
	
	@Override
	public void delete() {
		// remove from scope information of involved elements
		var iter = ruleEvalObj.listProperties(factory.getHavingScopePartProperty().asProperty());
		while(iter.hasNext()) {
			var scope = iter.next().getResource().as(OntIndividual.class);
			scope.remove(factory.getUsedInRuleProperty().asProperty(), ruleEvalObj);
		}
		//then remove self
		this.ruleEvalObj.removeProperties();
		delegate.delete();		
		
	}

	@Override
	public String toString() {
		return "RuleEvaluation [ruleDefinition="+delegate.getRuleDefinition().getName() +" contextInstance=" + contextInstance.getURI()
				+ ", isConsistent()=" + isConsistent() + ", getEvaluationError()=" + getEvaluationError()
				+ ", isEnabled()=" + isEnabled() + "]";
	}



	
	
}

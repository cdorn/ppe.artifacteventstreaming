package at.jku.isse.artifacteventstreaming.rule;

import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

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
	
	// either create new	
	/**
	 * @param factory for accessing properties
	 * @param def type of rule
	 * @param contextInstance for which instance to create the evaluation object wrapped by this class
	 * @return a new evaluation object wrapper, ensuring that the evaluation and context element point to the same rule scope. 
	 */
	public static RuleEvaluationWrapperResourceImpl create(@NonNull RuleFactory factory, @NonNull RDFRuleDefinition def, @NonNull OntIndividual contextInstance) {
		var evalObj = def.getRuleDefinition().createIndividual();		
		addAddRuleEvaluationToNewOrExistingScope(contextInstance, evalObj, factory); // just to make sure that the context scope is set (no effect if already so)
		return new RuleEvaluationWrapperResourceImpl(def, evalObj, contextInstance, factory);
	}
	
	private static void addAddRuleEvaluationToNewOrExistingScope(OntIndividual subject, OntIndividual ruleEval, RuleFactory factory) {
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
			scope = createScopeSkeleton(subject, factory);
		}
		scope.addProperty(factory.getUsedInRuleProperty().asNamed(), ruleEval); // if already there, then has no effect
		// set pointer back from ruleEval to scope
		ruleEval.addProperty(factory.getContextElementScopeProperty().asNamed(), scope);
	}
	
	private static OntIndividual createScopeSkeleton(Resource subject, RuleFactory factory) {
		var scope = factory.getRuleScopeCollection().createIndividual();
		scope.addProperty(factory.getUsingElementProperty().asNamed(), subject);
		subject.addProperty(factory.getHasRuleScope().asProperty(), scope);
		return scope;
	}
	
	// or create from underlying ontclass 
	/**
	 * @param ruleEvalObj pre-existing, that needs wrapping
	 * @param factory to access properties 
	 * @param ruleRepo to access existing definitions (or register new from ruleEvalObject reference
	 * @return rule evaluation wrapper for the provided ont individual
	 * @throws EvaluationException when ruleEvalObject is not a rule evaluation, or it does not point to context rule scope, or it does not point to a rule definition   
	 */
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
		var res = ruleEvalObj.getPropertyResourceValue(factory.getContextElementScopeProperty().asProperty()); //the rule scope, only one possible
		if (res == null)
			throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as it doesn't reference a context instance rule scope element via %s", ruleEvalObj, factory.getContextElementScopeProperty().getURI()));
		if (res.canAs(OntIndividual.class)) {
				var indiv = res.as(OntIndividual.class);
				var element = indiv.getPropertyResourceValue(factory.getUsingElementProperty().asProperty());
				if (element == null || !element.canAs(OntIndividual.class)) {				
					throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as the referenced context instace rule scope element doesn't point to an OntIndividual via %s ", ruleEvalObj, factory.getUsingElementProperty().getURI()));		
				} else {
					return element.as(OntIndividual.class);
				}
				
		} else {
			throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as its referenced context instance rule scope %s is not an ontindividual", ruleEvalObj.getId(), res));
		}					
	}
	
	private static RDFRuleDefinition resolveDefinition(@NonNull OntClass ruleDef, @NonNull RuleRepository repo) throws EvaluationException {
		var def = repo.findRuleDefinitionForResource(ruleDef);
		if (def == null)
			return repo.storeRuleDefinition(ruleDef.as(OntIndividual.class)); // we dynamically register the definition
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
	
	}
	
	private void setEnabledIfNotStatusAvailable() {
		var stmt = ruleEvalObj.getProperty(factory.getIsEnabledProperty());
		if (stmt == null) // not set, thus enable
			setEnabledStatus(true);
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
			scope = createScopeSkeleton(subject, factory);
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
		Set<OntIndividual> scopesToRemoveRuleFrom = new HashSet<>();
		while(iter.hasNext()) {
			var scope = iter.next().getResource().as(OntIndividual.class);
			scopesToRemoveRuleFrom.add(scope);			
		}
		// remove context instance scope
		iter = ruleEvalObj.listProperties(factory.getContextElementScopeProperty().asProperty());
		while(iter.hasNext()) { // should only be one, but to be on the save side
			var scope = iter.next().getResource().as(OntIndividual.class);
			scopesToRemoveRuleFrom.add(scope);
		}
		scopesToRemoveRuleFrom.forEach(scope -> scope.remove(factory.getUsedInRuleProperty().asProperty(), ruleEvalObj));
		
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

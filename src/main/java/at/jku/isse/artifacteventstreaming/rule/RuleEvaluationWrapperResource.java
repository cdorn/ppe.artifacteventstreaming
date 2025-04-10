package at.jku.isse.artifacteventstreaming.rule;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluation;
import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluationImpl;
import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import at.jku.isse.designspace.rule.arl.repair.RepairNode;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleEvaluationWrapperResource {

	@Getter
	private final OntIndividual ruleEvalObj;
	private final RuleSchemaProvider schemaProvider;
	private final OntIndividual contextInstance;
	@Getter
	private final RuleEvaluation delegate;
	private Object result = null;
	@Getter
	private RDFRuleDefinition definition;
	
	// either create new	
	/**
	 * @param factory for accessing properties
	 * @param def type of rule
	 * @param contextInstance for which instance to create the evaluation object wrapped by this class
	 * @return a new evaluation object wrapper, ensuring that the evaluation and context element point to the same rule scope. 
	 */
	public static RuleEvaluationWrapperResource create(@NonNull RuleSchemaProvider factory, @NonNull RDFRuleDefinition def, @NonNull OntIndividual contextInstance) {
		var uri = createEvalURI(def, contextInstance);
		var evalObj = def.getRuleDefinition().createIndividual(uri);	
		evalObj.addLabel(def.getName());
		addAddRuleEvaluationToNewOrExistingScope(contextInstance, evalObj, factory); // just to make sure that the context scope is set (no effect if already so)
		return new RuleEvaluationWrapperResource(def, evalObj, contextInstance, factory);
	}
	
	private static String createEvalURI(@NonNull RDFRuleDefinition def, @NonNull OntIndividual contextInstance) {
		return def.getRuleDefinition().getURI()+"::"+contextInstance.getLocalName()+"::"+contextInstance.getURI().hashCode(); // we assume here that context instance come from the same namespace, hence are distinguishable based on their localname, but add the hashcode of the uri to be on a safer side
	}
	
	private static void addAddRuleEvaluationToNewOrExistingScope(OntIndividual subject, OntIndividual ruleEval, RuleSchemaProvider factory) {
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
	
	private static OntIndividual createScopeSkeleton(Resource subject, RuleSchemaProvider factory) {
		var scope = factory.getRuleScopeCollection().createIndividual();
		scope.addProperty(factory.getUsingElementProperty().asNamed(), subject);
		subject.addProperty(factory.getHasRuleScope().asProperty(), scope);
		return scope;
	}
	
	// or create from underlying ont object 
	/**
	 * @param ruleEvalObj pre-existing, that needs wrapping
	 * @param factory to access properties 
	 * @param ruleRepo to access existing definitions (or register new from ruleEvalObject reference
	 * @return rule evaluation wrapper for the provided ont individual
	 * @throws EvaluationException when ruleEvalObject is not a rule evaluation, or it does not point to context rule scope, or it does not point to a rule definition   
	 */
	public static RuleEvaluationWrapperResource loadFromModel(@NonNull OntIndividual ruleEvalObj, @NonNull RuleSchemaProvider factory, @NonNull RuleRepository ruleRepo) throws EvaluationException {
		// check if an rule eval instance
		var type = getRuleTypeClass(ruleEvalObj, factory); 							
		// check if has context
		var ctx = getContextInstanceFrom(ruleEvalObj, factory);		
		// check if has definition, if so fetch from factory
		var def = resolveDefinition(type, ruleRepo);
		// dont evaluate unless evaluate() is called or access to result (lazy loading/generation of eval result)
		return new RuleEvaluationWrapperResource(def, ruleEvalObj, ctx, factory);
	}
	
	private static OntClass getRuleTypeClass(@NonNull OntIndividual ruleEvalObj, @NonNull RuleSchemaProvider factory) throws EvaluationException {
		var result = ruleEvalObj.classes(true)
				.filter(type -> factory.getResultBaseType().hasSubClass(type, false)).findAny(); // there should be only one
		if (result.isEmpty()) {
			throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as it is not a subclass of %s", ruleEvalObj, factory.getResultBaseType().getURI()));
		} else {
			return result.get();
		}
	}
	
	private static OntIndividual getContextInstanceFrom(@NonNull OntIndividual ruleEvalObj, @NonNull RuleSchemaProvider factory) throws EvaluationException {
		var res = ruleEvalObj.getPropertyResourceValue(factory.getContextElementScopeProperty().asProperty()); //the rule scope, only one possible
		if (res == null)
			throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as it doesn't reference a context instance rule scope element via %s", ruleEvalObj, factory.getContextElementScopeProperty().getURI()));
		if (res.canAs(OntIndividual.class)) {
				var indiv = res.as(OntIndividual.class);
				var element = indiv.getPropertyResourceValue(factory.getUsingElementProperty().asProperty());
				if (element == null || !element.canAs(OntIndividual.class)) {				
					var msg = String.format("Cannot create ruleevaluation wrapper for entity %s as the referenced context instace rule scope element doesn't point to an OntIndividual via %s ", ruleEvalObj, factory.getUsingElementProperty().getURI());
					log.warn(msg);
					throw new EvaluationException(msg);		
				} else {
					return element.as(OntIndividual.class);
				}
				
		} else {
			var msg = String.format("Cannot create ruleevaluation wrapper for entity %s as its referenced context instance rule scope %s is not an ontindividual", ruleEvalObj.getId(), res);
			log.warn(msg);
			throw new EvaluationException(msg);
		}					
	}
	
	private static RDFRuleDefinition resolveDefinition(@NonNull OntClass ruleDef, @NonNull RuleRepository repo) throws EvaluationException {
		var def = repo.findRuleDefinitionForResource(ruleDef);
		if (def == null)
			return repo.storeRuleDefinition(ruleDef.as(OntIndividual.class)); // we dynamically register the definition
		else 
			return def;
	}
	

	
	@SuppressWarnings("rawtypes")
	private RuleEvaluationWrapperResource(RDFRuleDefinition def, OntIndividual ruleEvalObj, OntIndividual contextInstance, RuleSchemaProvider factory ) {
		super();
		this.ruleEvalObj = ruleEvalObj;
		this.contextInstance = contextInstance;
		this.schemaProvider = factory;
		this.definition = def;
		this.delegate = new RuleEvaluationImpl(def, contextInstance);
		setEnabledIfNotStatusAvailable();		
	
	}
	
	private void setEnabledIfNotStatusAvailable() {
		var stmt = ruleEvalObj.getProperty(schemaProvider.getIsEnabledProperty());
		if (stmt == null) // not set, thus enable
			setEnabledStatus(true);
	}		
	
	
	/**
	 * If rule is disabled, returns null.
	 * If evaluation has error, returns null
	 * result itself is not persisted, just whether rule is consistent or not 
	 */
	public Entry<RuleEvaluation, Boolean> evaluate() {
		if (!isEnabled()) return null;
		
		var priorConsistency = isConsistent();
		result = delegate.evaluate();
		var error = delegate.getError();
		if (error != null) {			
			ruleEvalObj.removeAll(schemaProvider.getEvaluationErrorProperty())
			.addLiteral(schemaProvider.getEvaluationErrorProperty(), error);
			ruleEvalObj.removeAll(schemaProvider.getEvaluationHasConsistentResultProperty())
			.addLiteral(schemaProvider.getEvaluationHasConsistentResultProperty(), Boolean.FALSE);			
		} else {
			ruleEvalObj.removeAll(schemaProvider.getEvaluationErrorProperty());
			ruleEvalObj.removeAll(schemaProvider.getEvaluationHasConsistentResultProperty());
			if (result instanceof Boolean boolResult) {
				ruleEvalObj.addLiteral(schemaProvider.getEvaluationHasConsistentResultProperty(), boolResult); 	
			}
			updateRuleScope();
		}				
		return new AbstractMap.SimpleEntry<>(delegate, !Objects.equals(priorConsistency, isConsistent())); // returns if the outcome has changed;
	}

	@SuppressWarnings("unchecked")
	private void updateRuleScope() {
		delegate.getAddedScopeElements().stream()		
			.forEach(scopeEntry -> addPropertyToScope((Entry<Resource, Property>)scopeEntry, ruleEvalObj));
		delegate.getRemovedScopeElements().stream()		
		.forEach(scopeEntry -> removePropertyFromScope((Entry<Resource, Property>)scopeEntry, ruleEvalObj));                                       
	}
	
	private void addPropertyToScope(Entry<Resource, Property> typed, OntIndividual ruleEval) {
		var subject = typed.getKey();
		var property = typed.getValue();
		
		OntIndividual scope = findScope(subject,  property);						
		if (scope == null) {
			// ensure individual has scopeCollection			
			scope = createScopeSkeleton(subject, schemaProvider);
			scope.addProperty(schemaProvider.getUsingPredicateProperty().asNamed(), property);
			scope.addProperty(schemaProvider.getUsedInRuleProperty().asNamed(), ruleEval);
		} else {	
			 //scope exists already for this property and instance, just set the rule usage
		}
		// now link up eval to scope part			
		ruleEval.addProperty(schemaProvider.getHavingScopePartProperty().asNamed(), scope); 
		//currently this doesn work yet via inverseProperty definition of usedInRuleProperty, hence manually 
		scope.addProperty(schemaProvider.getUsedInRuleProperty().asNamed(), ruleEval);		
	}		
	
	private void removePropertyFromScope(Entry<Resource, Property> entry, OntIndividual ruleEval) {
		var subject = entry.getKey();
		var property = entry.getValue();
		
		OntIndividual scope = findScope(subject,  property);						
		if (scope == null) {
			//there was no scope recorded before, nothing to do,			
		} else { //scope exists already for this property and instance, just remove the rule usage 		
			scope.remove(schemaProvider.getUsedInRuleProperty().asProperty(), ruleEval);
			//currently we need to remove this from the rule as well as inverse-of property does not work yet		
			ruleEval.remove(schemaProvider.getHavingScopePartProperty().asProperty(), scope);
		}
	}
	
	private OntIndividual findScope(Resource subject, Property property) {
		OntIndividual scope = null;
		var iter = subject.listProperties(schemaProvider.getHasRuleScope().asProperty());
		while(iter.hasNext()) {
			var stmt = iter.next();
			var propStmt = stmt.getResource().getProperty(schemaProvider.getUsingPredicateProperty().asProperty());
			if (propStmt != null && propStmt.getResource().getURI().equals(property.getURI())) {
				//is this the scope collection for this particular property
				scope = stmt.getResource().as(OntIndividual.class);
				iter.close();
				break;
			}
		}
		return scope;
	}
	
	public boolean isConsistent() {
		var stmt = ruleEvalObj.getProperty(schemaProvider.getEvaluationHasConsistentResultProperty());
		return stmt != null ? stmt.getBoolean() : Boolean.TRUE; // if never has been set or if not a boolean result but no error, then True
	}		

	public String getEvaluationError() {
		var stmt = ruleEvalObj.getProperty(schemaProvider.getEvaluationErrorProperty());
		return stmt != null ? stmt.getString() : "";
	}

	public Object getEvaluationResult() {
		if (result == null && getEvaluationError().isEmpty()) { // never evaluated so far or not locally evaluated yet
			 evaluate();
		} 
		return result;		
	}

	public OntIndividual getContextInstance() {
		return contextInstance;
	}

	public boolean isEnabled() {
		if (ruleEvalObj == null) return false;
		var stmt = ruleEvalObj.getProperty(schemaProvider.getIsEnabledProperty());
		return stmt != null ? stmt.getBoolean() : Boolean.FALSE; //if not set, then assumed false, because when we delete, we wont have any statements, hence need to assume disabled
	}
	
	private void setEnabledStatus(boolean status) {
		ruleEvalObj.removeAll(schemaProvider.getIsEnabledProperty())
		.addLiteral(schemaProvider.getIsEnabledProperty(), status);	
	}
	
	public void enable() {
		if (!isEnabled()) {
			setEnabledStatus(true);
		}
	}

	public void disable() {
		if (isEnabled()) {
			setEnabledStatus(false);
		}
	}
	
	public void delete() {
		Set<OntIndividual> scopesToRemoveRuleFrom = new HashSet<>();
		// remove from scope information of involved elements				
		var iter = ruleEvalObj.listProperties(schemaProvider.getHavingScopePartProperty().asProperty());		
		while(iter.hasNext()) {
			var scope = iter.next().getResource().as(OntIndividual.class);
			scopesToRemoveRuleFrom.add(scope);			
		}
		// remove context instance scope
		iter = ruleEvalObj.listProperties(schemaProvider.getContextElementScopeProperty().asProperty());
		while(iter.hasNext()) { // should only be one, but to be on the save side
			var scope = iter.next().getResource().as(OntIndividual.class);
			scopesToRemoveRuleFrom.add(scope);
		}
		scopesToRemoveRuleFrom.forEach(scope -> scope.remove(schemaProvider.getUsedInRuleProperty().asProperty(), ruleEvalObj));
		
		//then remove self
		this.ruleEvalObj.removeProperties();
		delegate.delete();		
		
	}

	public String toString() {
		return "RuleEvaluation [ruleDefinition="+delegate.getRuleDefinition().getName() +" contextInstance=" + contextInstance.getURI()
				+ ", isConsistent()=" + isConsistent() + ", getEvaluationError()=" + getEvaluationError()
				+ ", isEnabled()=" + isEnabled() + "]";
	}

	public RepairNode getRepairTree() {
		if (this.isEnabled() && this.delegate != null && this.delegate.getError() == null) {
			if (this.delegate.getEvaluationTree() == null) {
				this.evaluate();
			}
			return delegate.getRepairTree();
		}
		return null;
	}



	
	
}

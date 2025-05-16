package at.jku.isse.artifacteventstreaming.rule;

import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.IntStream;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluation;
import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluationImpl;
import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import at.jku.isse.designspace.rule.arl.repair.AbstractRepairAction;
import at.jku.isse.designspace.rule.arl.repair.AbstractRepairNode;
import at.jku.isse.designspace.rule.arl.repair.RepairNode;
import at.jku.isse.designspace.rule.arl.repair.UnknownRepairValue;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleEvaluationWrapperResource extends RuleEvaluationDTO {

	@Getter
	private final RuleEvaluation delegate;
	private Object result = null;
	
	
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
	
	@SuppressWarnings("rawtypes")
	private RuleEvaluationWrapperResource(RDFRuleDefinition def, OntIndividual ruleEvalObj, OntIndividual contextInstance, RuleSchemaProvider factory ) {
		super(def, ruleEvalObj, contextInstance, factory);
		this.delegate = new RuleEvaluationImpl(def, contextInstance);
	}
	
	public Object getEvaluationResult() {
		if (result == null && getEvaluationError().isEmpty()) { // never evaluated so far or not locally evaluated yet
			 evaluate();
		} 
		return result;		
	}
	
	/**
	 * If rule is disabled, returns null.
	 * If evaluation has error, returns null
	 * result itself is not persisted, just whether rule is consistent or not 
	 */
	public Entry<RuleEvaluationDTO, Boolean> evaluate() {
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
			Boolean newConsistent = false;
			if (result instanceof Boolean boolResult) {
				ruleEvalObj.addLiteral(schemaProvider.getEvaluationHasConsistentResultProperty(), boolResult); 	
				newConsistent = boolResult;
			}
			updateRuleScope();
			super.setRootRepairNode(null); // remove old tree
			if (Boolean.FALSE.equals(newConsistent)) {
				// pessemistic recreation of repair tree.
				var root = transformAndStoreRepairs(delegate.getRepairTree(), null, -1);
				super.setRootRepairNode(root);
			} 
		}				
		
		
		return new AbstractMap.SimpleEntry<>(this, !Objects.equals(priorConsistency, isConsistent())); // returns if the outcome has changed;
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
	
	private static OntIndividual createScopeSkeleton(Resource subject, RuleSchemaProvider factory) {
		var scope = factory.getRuleScopeCollection().createIndividual();
		scope.addProperty(factory.getUsingElementProperty().asNamed(), subject);
		subject.addProperty(factory.getHasRuleScope().asProperty(), scope);
		return scope;
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

	@SuppressWarnings("unchecked")
	private void updateRuleScope() {
		delegate.getAddedScopeElements().stream()		
			.forEach(scopeEntry -> addPropertyToScope((Entry<Resource, Property>)scopeEntry, ruleEvalObj));
		delegate.getRemovedScopeElements().stream()		
		.forEach(scopeEntry -> removePropertyFromScope((Entry<Resource, Property>)scopeEntry, ruleEvalObj));                                       
	}
	
	@Override
	public void delete() {
		super.delete();
		delegate.delete();		
	}
	
	public RepairNodeDTO getRepairTree() {
		if (this.isConsistent()) // no repair tree for consistent rule
			return null;
		var root = super.getRepairRootNode();
		if (root != null) // if there is a serialized repair tree form, return that.
			return root;
		// we need to check if the cached repair tree dto is still valid, or if we have to regenerate it,
		// by checking the delegate repair nodes repair tree, bit ugly
		if (this.isEnabled() && this.delegate != null && this.delegate.getError() == null) {
			if (this.delegate.getEvaluationTree() == null) {
				this.evaluate(); //then there definitely has not been a repair tree generated
				return transformAndStoreRepairs(delegate.getRepairTree(), null, -1);
			}
			else if (this.getDelegate().getEvaluationTree().repairTree == null) { // repair tree not yet generated
				return transformAndStoreRepairs(delegate.getRepairTree(), null, -1);
			} else { // repair tree was there, we assume, caused by an earlier call from here, and thus return cached dto form
				return super.getRepairRootNode();
			}
		}
		return null;
	}

	private RepairNodeDTO transformAndStoreRepairs(RepairNode rawRootNode, RepairNodeDTO parentNode, int posInParent) {
		if (rawRootNode == null) return null;
		if (rawRootNode instanceof AbstractRepairAction repairAction) {
			return transform(repairAction, parentNode, posInParent);
		} else if (rawRootNode instanceof AbstractRepairNode repairNode){
			var node = new RepairNodeDTO(repairNode.getNodeType().toString(), parentNode, posInParent, this, super.schemaProvider);
			for (int i=0; i < repairNode.getChildren().size(); i++) {
				transformAndStoreRepairs(repairNode.getChildren().get(i), node, i);
			}
			return node;
		} else {
			log.error("Unknown RepairNode type encountered: "+rawRootNode.toString());
			return null;
		}
	}
	
	private RepairNodeDTO transform(AbstractRepairAction repairAction, RepairNodeDTO parentNode, int posInParent) {
		var subject = (OntIndividual)repairAction.getElement();
		var predicateName = repairAction.getProperty();
		var predicate = resolvePredicate(predicateName, subject);
		var value = repairAction.getValue();
		OntObject objValue = null;
		Object litValue = null;
		String restriction = null;
		if (value == UnknownRepairValue.UNKNOWN) {
			// we will have a restriction (once Anmol's stuff is integrated 
			litValue = value;
			var restr= repairAction.getRepairValueOption().getRestriction();
			if (restr != null) {
				restriction = restr.getRootNode().printNodeTree(false, 40);
			}
		} else {
			if (value instanceof OntObject ontObj) { // 
				objValue = ontObj;
			} else {
				litValue = value;
			}
		}
		return new RepairNodeDTO( 
					repairAction.getOperator().name(), 
					subject,
					predicate,
					litValue,
					objValue,
					restriction, 
					parentNode,
					posInParent,
					this,
					super.schemaProvider
					);
	}
	
	private OntProperty resolvePredicate(@NonNull String predicate, @NonNull OntIndividual subject) {
		var modelAccess = super.schemaProvider.getModelAccess();
		var propOpt = modelAccess.getTypeOfInstance(subject).findAny().map(type -> modelAccess.resolveToProperty(type, predicate));
		return propOpt.get();
	}
	
}

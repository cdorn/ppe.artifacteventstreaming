package at.jku.isse.artifacteventstreaming.rule.evaluation;

import java.util.HashSet;
import java.util.Set;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;

import at.jku.isse.artifacteventstreaming.rule.RepairNodeDTO;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.definition.RDFRuleDefinition;
import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleEvaluationDTO {
	@Getter protected final OntIndividual ruleEvalObj;
	protected final RuleSchemaProvider schemaProvider;
	private final OntIndividual contextInstance;
	@Getter private RDFRuleDefinition definition;
	protected RepairNodeDTO rootRepairNode;

		// create from underlying ont object 
		/**
		 * @param ruleEvalObj pre-existing, that needs wrapping
		 * @param factory to access properties 
		 * @param ruleRepo to access existing definitions (or register new from ruleEvalObject reference
		 * @return rule evaluation wrapper for the provided ont individual
		 * @throws EvaluationException when ruleEvalObject is not a rule evaluation, or it does not point to context rule scope, or it does not point to a rule definition   
		 */
		public static RuleEvaluationDTO loadFromModel(@NonNull OntIndividual ruleEvalObj, @NonNull RuleSchemaProvider factory, @NonNull RuleRepository ruleRepo) throws EvaluationException {
			// check if an rule eval instance
			var type = getRuleTypeClass(ruleEvalObj, factory); 							
			// check if has context
			var ctx = getContextInstanceFrom(ruleEvalObj, factory);		
			// check if has definition, if so fetch from factory
			var def = resolveDefinition(type, ruleRepo);
			// dont evaluate unless evaluate() is called or access to result (lazy loading/generation of eval result)
			return new RuleEvaluationDTO(def, ruleEvalObj, ctx, factory);
		}
		
		protected static OntClass getRuleTypeClass(@NonNull OntIndividual ruleEvalObj, @NonNull RuleSchemaProvider factory) throws EvaluationException {
			var result = ruleEvalObj.classes(true)
					.filter(type -> factory.getResultBaseType().hasSubClass(type, false)).findAny(); // there should be only one
			if (result.isEmpty()) {
				throw new EvaluationException(String.format("Cannot create ruleevaluation wrapper for entity %s as it is not a subclass of %s", ruleEvalObj, factory.getResultBaseType().getURI()));
			} else {
				return result.get();
			}
		}
		
		protected static OntIndividual getContextInstanceFrom(@NonNull OntIndividual ruleEvalObj, @NonNull RuleSchemaProvider factory) throws EvaluationException {
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
		
		protected static RDFRuleDefinition resolveDefinition(@NonNull OntClass ruleDef, @NonNull RuleRepository repo) throws EvaluationException {
			var def = repo.findRuleDefinitionForResource(ruleDef);
			if (def == null)
				return repo.storeRuleDefinition(ruleDef.as(OntIndividual.class)); // we dynamically register the definition
			else 
				return def;
		}
		
		@SuppressWarnings("rawtypes")
		protected RuleEvaluationDTO(RDFRuleDefinition def, OntIndividual ruleEvalObj, OntIndividual contextInstance, RuleSchemaProvider factory ) {
			this.ruleEvalObj = ruleEvalObj;
			this.contextInstance = contextInstance;
			this.schemaProvider = factory;
			this.definition = def;
			setEnabledIfNotStatusAvailable();		
		}
		
		private void setEnabledIfNotStatusAvailable() {
			var stmt = ruleEvalObj.getProperty(schemaProvider.getIsEnabledProperty());
			if (stmt == null) // not set, thus enable
				setEnabledStatus(true);
		}	
		
		public boolean isConsistent() {
			var stmt = ruleEvalObj.getProperty(schemaProvider.getEvaluationHasConsistentResultProperty());
			return stmt != null ? stmt.getBoolean() : Boolean.TRUE; // if never has been set or if not a boolean result but no error, then True
		}		

		public String getEvaluationError() {
			var stmt = ruleEvalObj.getProperty(schemaProvider.getEvaluationErrorProperty());
			return stmt != null ? stmt.getString() : "";
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
		
		public RepairNodeDTO getRepairRootNode() {
			if (rootRepairNode == null) {
				reloadRepairNodes();
			}
			return rootRepairNode;
		}
		
		protected void reloadRepairNodes() { // used to refetch upon rdf level update
			var optNode = RepairNodeDTO.loadTreeFromModel(this, schemaProvider);
			if (optNode.isPresent()) {
				rootRepairNode = optNode.get();
			} else {
				rootRepairNode = null;
			}
		}
		
		protected void setRootRepairNode(RepairNodeDTO root) { // used by rule evaluation wrapper resource upon (re)generating root repair
			if (root == null || rootRepairNode != null) {
				deleteRepairTree(); // remove prior version, if any, to keep data consistent.
			}
			rootRepairNode = root;
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
			scopesToRemoveRuleFrom.forEach(scope -> { 
				scope.remove(schemaProvider.getUsedInRuleProperty().asProperty(), ruleEvalObj); 
				removeEmptyScopes(scope);
			});
			
			// remove repair nodes
			deleteRepairTree();
			
			//then remove self
			this.ruleEvalObj.removeProperties();
		}
		
		private void removeEmptyScopes(OntIndividual scope) {
			var remainingRuleEvals = scope.getPropertyResourceValue(schemaProvider.getUsedInRuleProperty().asProperty());
			if (remainingRuleEvals == null) { // if scope now empty, remove scope
				// remove first from owner, then clear scope
				var owner = scope.getPropertyResourceValue(schemaProvider.getUsingElementProperty().asProperty());
				if (owner != null) { // perhaps already deleted in some cases
					scope.getModel().remove(owner, schemaProvider.getHasRuleScope().asProperty(), scope);
				}
				scope.removeProperties();
			}
		}
		
		private void deleteRepairTree() {
			RepairNodeDTO.removeTreeFromModel(this, schemaProvider);
		}

		public String toString() {
			return "RuleEvaluation [ruleDefinition="+definition.getName() +" contextInstance=" + contextInstance.getURI()
					+ ", isConsistent()=" + isConsistent() + ", getEvaluationError()=" + getEvaluationError()
					+ ", isEnabled()=" + isEnabled() + "]";
		}

}

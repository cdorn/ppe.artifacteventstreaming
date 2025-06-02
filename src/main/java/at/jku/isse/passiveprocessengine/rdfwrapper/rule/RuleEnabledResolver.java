package at.jku.isse.passiveprocessengine.rdfwrapper.rule;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntClass.Named;
import org.apache.jena.ontapi.model.OntRelationalProperty;
import org.apache.jena.rdf.model.Resource;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleException;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleRepositoryInspector;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.designspace.rule.arl.evaluator.EvaluationNode;
import at.jku.isse.designspace.rule.arl.evaluator.RuleDefinitionImpl;
import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluation;
import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluationImpl;
import at.jku.isse.designspace.rule.arl.parser.ArlType;
import at.jku.isse.designspace.rule.arl.repair.AlternativeRepairNode;
import at.jku.isse.designspace.rule.arl.repair.RepairNode;
import at.jku.isse.designspace.rule.arl.repair.RepairSingleValueOption;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFElement;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.metaschema.WrapperMetaModelSchemaTypes;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RuleEnabledResolver extends NodeToDomainResolver implements RuleEvaluationService {

	private final RepairService repairService;
	@Getter private final RuleSchemaProvider ruleSchema;
	@Getter protected final RuleRepository ruleRepo;
	private final RuleRepositoryInspector inspector;
	
	public RuleEnabledResolver(Branch branch, RepairService repairService, RuleSchemaProvider ruleSchema, RuleRepository repo, WrapperMetaModelSchemaTypes metaschema) {
		super(branch, metaschema);
		this.repairService = repairService;
		this.ruleRepo = repo;
		this.ruleSchema = ruleSchema;		
		this.inspector = new RuleRepositoryInspector(ruleSchema);
		initOverride();
	}

	@Override
	protected void init() {
		//overriding, below
	}
	
	protected void initOverride() {
		// register rule definition as type
		var ruleDefType = ruleSchema.getDefinitionType();
		initOrGetType(ruleDefType);
		var ruleEvalType = ruleSchema.getResultBaseType();
		initOrGetType(ruleEvalType);
		
		
		var ruleDefinitions = ruleRepo.getRuleDefinitions().stream().map(indiv -> indiv.getRuleDefinition().getURI()).collect(Collectors.toSet());
		model.classes()
			.filter(ontClass -> !isBlacklistedNamespace(ontClass.getNameSpace()))									
			.filter(ontClass -> !ruleDefinitions.contains(ontClass.getURI())) // we dont want to cache rule definitions here but via rule repo, the store just wrappers here			
			.forEach(this::loadTypeInstances);
		// store all rule definitions as wrappers
		ruleRepo.getRuleDefinitions().forEach(ruleDef -> {
			var wrapper = new RDFRuleDefinitionWrapper(ruleDef, this);			
			typeIndex.put(ruleDef.getRuleDefinition(), wrapper);	
		});
	}
	
	protected void removeRuleDefinition(RDFRuleDefinitionWrapper ruleDef) {
		// remove from type index, then remove from repo below
		typeIndex.remove(ruleDef.getType());
		ruleRepo.removeRuleDefinition(ruleDef.getId());
	}
	
	@Override
	protected RDFInstanceType initOrGetType(OntClass ontClass) {
		if (!typeIndex.containsKey(ontClass)) {
			var constructor = metaschemata.getMetaElements().getTypeConstructorForNamespace(ontClass.getURI());
			var type = createMostSpecificType(ontClass, constructor);
			typeIndex.put(ontClass, type);
			type.cacheSuperProperties();			
		}
		return typeIndex.get(ontClass);
	}
	
	private RDFInstanceType createMostSpecificType(OntClass ontClass, Constructor<? extends RDFInstanceType> typeClassConstructor) {
		if (typeClassConstructor == null) {
			return new RDFInstanceType(ontClass, this);
		} else {
			try {
				return typeClassConstructor.newInstance(ontClass, this);
			} catch (Exception e) {
				e.printStackTrace();
				log.error(e.getMessage());
				return null;
			}
		}
	}

	public RDFRuleDefinitionWrapper createInstance(@NonNull RDFInstanceType type, @NonNull String ruleURI, @NonNull String ruleExpression, String title) {
		OntClass ctxType = resolveTypeToClass(type);
		try {
			var ruleDef = ruleRepo.getRuleBuilder()
				.withRuleURI(ruleURI)
				.withContextType(ctxType)
				.withRuleExpression(ruleExpression)		
				.withRuleTitle(title)
				.build();
			var wrapper = new RDFRuleDefinitionWrapper(ruleDef, this);			
			typeIndex.put(ruleDef.getRuleDefinition(), wrapper);		
			return wrapper;
		} catch (RuleException e) {
			throw new RuntimeException("Unable to create rule: "+e.getMessage());
		}
	}
	
	public RDFRuleDefinitionWrapper createDerivedPropertyRule(RDFInstanceType ctxType, String ruleURI, String ruleExpression, OntRelationalProperty derivedProp) {
		try {
			var ruleDef = ruleRepo.getRuleBuilder()
				.withContextType(ctxType.getType())
				.withRuleURI(ruleURI)
				.withRuleExpression(ruleExpression) // deriving/mapping rule here
				.forDerivedProperty(derivedProp)
				.withRuleTitle("DerivedProperty"+derivedProp.getLocalName())
				.build();
				var wrapper = new RDFRuleDefinitionWrapper(ruleDef, this);			
				typeIndex.put(ruleDef.getRuleDefinition(), wrapper);
				return wrapper;
		} catch (RuleException e) {
			throw new RuntimeException("Unable to create rule: "+e.getMessage());
		}
	}
	
	public RDFRuleDefinitionWrapper getRuleByNameAndContext(String arg0, RDFInstanceType arg1) {
		// we ignore type and just use the name as a URI
		if (!isValidURL(arg0)) {
			arg0 = BASE_NS+arg0;
		}	
		var def = ruleRepo.findRuleDefinitionForURI(arg0);
		if (def != null) {
			return (RDFRuleDefinitionWrapper) typeIndex.computeIfAbsent(def.getRuleDefinition(), k-> new RDFRuleDefinitionWrapper(def, (RuleEnabledResolver) this));
		} else {
			return null;
		}
	}

	@Override
	public Optional<RDFInstanceType> findNonDeletedInstanceTypeByFQN(String arg0) {		
		if (!isValidURL(arg0)) {
			arg0 = BASE_NS+arg0;
		}	
		Named ontClass = model.getOntClass(arg0);
		if (ontClass == null) return Optional.empty();
		var type = typeIndex.get(ontClass);
		if (type != null) {
			return Optional.ofNullable(type);
		} else {
			var ruleDef = ruleRepo.findRuleDefinitionForResource(ontClass);
			if (ruleDef == null) return Optional.empty();
			var defWrapper = typeIndex.computeIfAbsent(ruleDef.getRuleDefinition(), k-> new RDFRuleDefinitionWrapper(ruleDef, this));
			return Optional.ofNullable(defWrapper);
		}
	}
	
	@Override
	protected RDFElement findIndividual(Resource node) {
		var localInst = super.findIndividual(node);
		if (localInst != null)
			return localInst;
		else { 
			var evalWrapper = ruleRepo.getEvaluations().get(node.getURI());
			if (evalWrapper != null)
				return new RDFRuleResultWrapper(evalWrapper, this); //FIXME: we are created new wrappers every time, but if we cache, we wont know when they need to be deleted					
			return null;
		}
	}
	

	public void setPropertyRepairable(RDFInstanceType type, String property, boolean isRepairable) {
		type.resolveToPropertyType(property).ifPresent(prop ->
				repairService.setPropertyRepairable(type.getType(), prop.getProperty().asProperty(), isRepairable));			
	}
	

	public boolean isPropertyRepairable(RDFInstanceType type, String property) {
		var optProp = type.resolveToPropertyType(property);
		if (optProp.isPresent()) {			
			return repairService.isPropertyRepairable(type.getType(), optProp.get().getProperty().asProperty());
		} else return false;
	}


	public RDFRuleDefinitionWrapper findByInternalId(String id) {
		var indiv = model.getOntClass(id);
		if (indiv != null) {			
			var ruleDef = super.typeIndex.get(indiv);
			if (ruleDef instanceof RDFRuleDefinitionWrapper wrapper) {
				return wrapper;
			}
		}
		return null;
	}


	public Set<ResultEntry> evaluateTransientRule(RDFInstanceType type, String constraint) throws Exception {
		RuleDefinitionImpl ruleDefImpl = tryCreateRule(type, constraint); // rethrow automatically error
		var contextType = resolveTypeToClass(type);
		// for each instance
		return contextType.individuals()
				.map(instance -> { 			
					//PPEInstance context = mapDesignSpaceInstanceToProcessDomainInstance(instance);
					RuleEvaluation ruleEval = new RuleEvaluationImpl(ruleDefImpl, instance);
					ruleEval.evaluate();				
					String error = ruleEval.getError();
					Boolean result = ruleEval.getError() == null ? (boolean) ruleEval.getResult() : null;			
					EvaluationNode rootEvalNode = ruleEval.getEvaluationTree(); 
					RepairNode rootRepairNode = null;
					if (Boolean.FALSE.equals(result)) {
						RepairNode repairTree = new AlternativeRepairNode(null);				
						rootEvalNode.expression.generateRepairTree(repairTree,RepairSingleValueOption.TRUE, rootEvalNode);
						repairTree.flattenRepairTree();
						rootRepairNode = repairTree;
					}
					var ctxWrapper = instanceIndex.computeIfAbsent(instance.getURI(), k -> new RDFInstance(instance, null, this));
					return new ResultEntry(ctxWrapper, result, error, rootEvalNode, rootRepairNode);
				})
				.collect(Collectors.toSet());	
	}


	public boolean isRuleCorrect(RDFInstanceType type, String constraint) throws Exception {
		return tryCreateRule(type, constraint)!=null;
	}
	
	private RuleDefinitionImpl tryCreateRule(RDFInstanceType type, String constraint) throws Exception {
		var contextType = resolveTypeToClass(type);
		if (contextType == null)
			throw new Exception("RDFInstanceType not found:" +type);
		ArlType arlType =  ArlType.get(ArlType.TypeKind.INSTANCE, ArlType.CollectionKind.SINGLE, contextType, ruleSchema.getModelAccess());		
		RuleDefinitionImpl ruleDefImpl = new RuleDefinitionImpl("", constraint, arlType, ruleSchema.getModelAccess());
		if (ruleDefImpl.getRuleError() != null) {
			throw new Exception(ruleDefImpl.getRuleError());
		}
		return ruleDefImpl;
	}

	public Set<ResultEntry> getEvaluationResults(RDFRuleDefinitionWrapper rule) {
		var def = rule.getRuleDef().getRuleDefinition();
		var evals =  ruleRepo.getEvaluations();
		return def.individuals()
				.map(eval -> evals.get(eval.getURI()))
				.filter(Objects::nonNull)
				.map(evalWrapper -> {
					var result = evalWrapper.isConsistent();
					var error = evalWrapper.getEvaluationError();
					var ctx = evalWrapper.getContextInstance();
					var ctxWrapper = instanceIndex.computeIfAbsent(ctx.getURI(), k -> new RDFInstance(ctx, null, this));
					var evalRoot = evalWrapper.getDelegate().getEvaluationTree();
					var repairRoot = evalWrapper.getDelegate().getRepairTree();
					return new ResultEntry(ctxWrapper, result, error, evalRoot, repairRoot);
				})
				.collect(Collectors.toSet());
	}

	public Map<RDFRuleDefinitionWrapper, Set<ResultEntry>> getEvaluationResultsWithInstanceInScope(RDFInstance instance) {
		Map<RDFRuleDefinitionWrapper, Set<ResultEntry>> resultMap = new HashMap<>();
		var indiv = instance.getInstance();
		var evals =  ruleRepo.getEvaluations();
		inspector.getEvalWrappersFromScopes(indiv).stream()
		.map(evalObj -> evals.get(evalObj.getURI()))
		.forEach(evalWrapper -> {
			var result = evalWrapper.isConsistent();
			var error = evalWrapper.getEvaluationError();
			var ctx = evalWrapper.getContextInstance();
			var ctxWrapper = instanceIndex.computeIfAbsent(ctx.getURI(), k -> new RDFInstance(ctx, null, this));
			var evalRoot = evalWrapper.getDelegate().getEvaluationTree();
			var repairRoot = evalWrapper.getDelegate().getRepairTree();
			var entry = new ResultEntry(ctxWrapper, result, error, evalRoot, repairRoot);
			var ruleDef = evalWrapper.getDefinition();
			RDFRuleDefinitionWrapper defWrapper = (RDFRuleDefinitionWrapper)typeIndex.computeIfAbsent(ruleDef.getRuleDefinition(), k-> new RDFRuleDefinitionWrapper(ruleDef, this));
			resultMap.computeIfAbsent(defWrapper, k -> new HashSet<ResultEntry>()).add(entry);
		});
		return resultMap;
	}	
}

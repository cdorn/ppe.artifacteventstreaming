package at.jku.isse.passiveprocessengine.rdfwrapper;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.Branch;
import at.jku.isse.artifacteventstreaming.rule.RepairService;
import at.jku.isse.artifacteventstreaming.rule.RuleException;
import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleRepositoryInspector;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.designspace.rule.arl.evaluator.EvaluationNode;
import at.jku.isse.designspace.rule.arl.evaluator.RuleDefinitionImpl;
import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluation;
import at.jku.isse.designspace.rule.arl.evaluator.RuleEvaluationImpl;
import at.jku.isse.designspace.rule.arl.parser.ArlType;
import at.jku.isse.designspace.rule.arl.repair.AlternativeRepairNode;
import at.jku.isse.designspace.rule.arl.repair.RepairNode;
import at.jku.isse.designspace.rule.arl.repair.RepairSingleValueOption;
import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.RuleDefinition;
import at.jku.isse.passiveprocessengine.core.RuleEvaluationService;
import lombok.Getter;

public class RuleEnabledResolver extends NodeToDomainResolver implements RuleEvaluationService {

	final RepairService repairService;
	@Getter final RuleSchemaProvider ruleSchema;
	final RuleRepository repo;
	final RuleRepositoryInspector inspector;
	
	public RuleEnabledResolver(Branch branch, RepairService repairService, RuleSchemaProvider ruleSchema, RuleRepository repo, MetaModelSchemaTypes cardinalityTypes) {
		super(branch, repo, cardinalityTypes);
		this.repairService = repairService;
		this.ruleSchema = ruleSchema;
		this.repo = repo;
		this.inspector = new RuleRepositoryInspector(ruleSchema);
		initOverride();
	}

	@Override
	protected void init() {
		//overriding, below
	}
	
	protected void initOverride() {
		var ruleDefinitions = repo.getRuleDefinitions().stream().map(indiv -> indiv.getRuleDefinition().getURI()).collect(Collectors.toSet());
		model.classes()
			.filter(ontClass -> !isBlacklistedNamespace(ontClass.getNameSpace()))									
			.filter(ontClass -> !ruleDefinitions.contains(ontClass.getURI())) // we dont want to cache rule evaluations here but via rule repo			
			.forEach(ontClass -> { 
			var type = new RDFInstanceType(ontClass, this);
			typeIndex.put(ontClass, type);
			type.cacheSuperProperties();
			if (!ontClass.equals(metaClass)) {
				var individuals = ontClass.individuals(true).toList();
				individuals.forEach(indiv -> instanceIndex.putIfAbsent(indiv.getURI(), new RDFInstance(indiv, type, this)));
			}
		} );
		
		repo.getRuleDefinitions().forEach(ruleDef -> {
			var wrapper = new RDFPPERuleDefinitionWrapper(ruleDef, this);			
			typeIndex.put(ruleDef.getRuleDefinition(), wrapper);	
		});
		
//		var ruleEvalWrappers = ruleSchema.getResultBaseType().individuals().collect(Collectors.toSet()); // we dont want to cache the result resources
//		model.individuals()
//			.filter(indiv -> !ruleEvalWrappers.contains(indiv))
//			.forEach(indiv -> instanceIndex.put(indiv, new RDFInstance(indiv, this)));
	}
	
	protected void removeRuleDefinition(RDFPPERuleDefinitionWrapper ruleDef) {
		// remove from type index, then remove from repo below
		typeIndex.remove(ruleDef.getType());
		repo.removeRuleDefinition(ruleDef.getId());
	}
	
	@Override
	public RuleDefinition createInstance(PPEInstanceType type, String ruleName, String ruleExpression) {
		OntClass ctxType = (OntClass) resolveTypeToClassOrDatarange(type);
		try {
			var ruleDef = repo.getRuleBuilder()
				.withRuleURI(NodeToDomainResolver.BASE_NS+ruleName)
				.withContextType(ctxType)
				.withRuleExpression(ruleExpression)
				.withRuleTitle(ruleName)
				.build();
			var wrapper = new RDFPPERuleDefinitionWrapper(ruleDef, this);			
			typeIndex.put(ruleDef.getRuleDefinition(), wrapper);
			return wrapper;
		} catch (RuleException e) {
			throw new RuntimeException("Unable to create rule: "+e.getMessage());
		}
	}

	@Override
	public void setPropertyRepairable(PPEInstanceType type, String property, boolean isRepairable) {
		if (type instanceof RDFInstanceType rdfType) {
			rdfType.resolveToPropertyType(property).ifPresent(prop ->
				repairService.setPropertyRepairable(rdfType.getType(), prop.getProperty().asProperty(), isRepairable));			
		} else {
			throw new RuntimeException("Expected RDFInstanceType but received "+type.getClass());
		}
	}

	@Override
	public RuleDefinition findByInternalId(String id) {
		var indiv = model.getOntClass(id);
		if (indiv != null) {			
			var ruleDef = super.typeIndex.get(indiv);
			if (ruleDef instanceof RDFPPERuleDefinitionWrapper wrapper) {
				return wrapper;
			}
		}
		return null;
	}

	@Override
	public Set<ResultEntry> evaluateTransientRule(PPEInstanceType type, String constraint) throws Exception {
		RuleDefinitionImpl ruleDefImpl = tryCreateRule(type, constraint); // rethrow automatically error
		var contextType = (OntClass)resolveTypeToClassOrDatarange(type);
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
					if (!result) {
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

	@Override
	public boolean isRuleCorrect(PPEInstanceType type, String constraint) throws Exception {
		return tryCreateRule(type, constraint)!=null;
	}
	
	private RuleDefinitionImpl tryCreateRule(PPEInstanceType type, String constraint) throws Exception {
		var contextType = (OntClass)resolveTypeToClassOrDatarange(type);
		if (contextType == null)
			throw new Exception("PPEInstanceType not found:" +type);
		ArlType arlType =  ArlType.get(ArlType.TypeKind.INSTANCE, ArlType.CollectionKind.SINGLE, contextType, ruleSchema.getModelAccess());		
		RuleDefinitionImpl ruleDefImpl = new RuleDefinitionImpl("", constraint, arlType, ruleSchema.getModelAccess());
		if (ruleDefImpl.getRuleError() != null) {
			throw new Exception(ruleDefImpl.getRuleError());
		}
		return ruleDefImpl;
	}

	@Override
	public Set<ResultEntry> getEvaluationResults(RuleDefinition rule) throws Exception {
		if (rule instanceof RDFPPERuleDefinitionWrapper wrapper) {
			var def = wrapper.getRuleDef().getRuleDefinition();
			var evals =  repo.getEvaluations();
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
		} else 
			throw new RuntimeException("Expected RDFPPERuleDefinitionWrapperImpl but received "+rule.getClass());
	}

	@Override
	public Map<RuleDefinition, Set<ResultEntry>> getEvaluationResultsWithInstanceInScope(PPEInstance instance)
			throws Exception {
		if (instance instanceof RDFInstance rdfEl) {
			Map<RuleDefinition, Set<ResultEntry>> resultMap = new HashMap<>();
			var indiv = rdfEl.getInstance();
			var evals =  repo.getEvaluations();
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
					var defWrapper = typeIndex.computeIfAbsent(ruleDef.getRuleDefinition(), k-> new RDFPPERuleDefinitionWrapper(ruleDef, this));
					resultMap.computeIfAbsent((RuleDefinition) defWrapper, k -> new HashSet<ResultEntry>()).add(entry);
				});
			return resultMap;
		} else {
			throw new RuntimeException("Expected RDFElement but received "+instance.getClass());
		}
	}

	
	
}

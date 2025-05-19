package at.jku.isse.artifacteventstreaming.rule.evaluation;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Seq;

import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaProvider;
import at.jku.isse.artifacteventstreaming.rule.definition.DerivedPropertyRuleDefinition;
import at.jku.isse.designspace.rule.arl.exception.EvaluationException;
import at.jku.isse.designspace.rule.arl.parser.ArlType;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DerivedPropertyRuleEvaluationWrapperResource extends RuleEvaluationWrapperResource {

	private final DerivedPropertyRuleDefinition derivedDef;
	


//	// or create from underlying ont object 
//	/**
//	 * @param ruleEvalObj pre-existing, that needs wrapping
//	 * @param factory to access properties 
//	 * @param ruleRepo to access existing definitions (or register new from ruleEvalObject reference
//	 * @return rule evaluation wrapper for the provided ont individual
//	 * @throws EvaluationException when ruleEvalObject is not a rule evaluation, or it does not point to context rule scope, or it does not point to a rule definition   
//	 */
//	public static DerivedPropertyRuleEvaluationWrapperResource loadFromModel(@NonNull OntIndividual ruleEvalObj, @NonNull RuleSchemaProvider factory, @NonNull RuleRepository ruleRepo) throws EvaluationException {
//		// check if an rule eval instance
//		var type = getRuleTypeClass(ruleEvalObj, factory); 							
//		// check if has context
//		var ctx = getContextInstanceFrom(ruleEvalObj, factory);		
//		// check if has definition, if so fetch from factory
//		var def = resolveDefinition(type, ruleRepo);
//		if (def != null && def instanceof DerivedPropertyRuleDefinition derivedDef) {
//			// dont evaluate unless evaluate() is called or access to result (lazy loading/generation of eval result)
//			return new DerivedPropertyRuleEvaluationWrapperResource(derivedDef, ruleEvalObj, ctx, factory);
//		} else {
//			log.error("Cannot instantiate DerivedPropertyRuleEvaluationWrapperResource as RDFRuleDefinition is either null or not an instance of DerivedPropertyRuleDefinition");
//			return null;
//		}
//	}
	
	protected DerivedPropertyRuleEvaluationWrapperResource(DerivedPropertyRuleDefinition def, OntIndividual ruleEvalObj,
			OntIndividual contextInstance, RuleSchemaProvider factory) {
		super(def, ruleEvalObj, contextInstance, factory);
		this.derivedDef = def;
	}

	@Override
	public Entry<RuleEvaluationDTO, Boolean> evaluate() {
		if (!isEnabled()) return null;
		var priorConsistency = isConsistent();
		boolean newConsistency = true;
		result = delegate.evaluate();
		var error = delegate.getError();
		if (error != null) {			
			newConsistency = false;
			ruleEvalObj.removeAll(schemaProvider.getEvaluationErrorProperty())
			.addLiteral(schemaProvider.getEvaluationErrorProperty(), error);
			if (priorConsistency != newConsistency) {
				ruleEvalObj.removeAll(schemaProvider.getEvaluationHasConsistentResultProperty())
				.addLiteral(schemaProvider.getEvaluationHasConsistentResultProperty(), Boolean.FALSE);		
			}
		} else {
			if (priorConsistency != newConsistency) {
				ruleEvalObj.removeAll(schemaProvider.getEvaluationErrorProperty());
				ruleEvalObj.removeAll(schemaProvider.getEvaluationHasConsistentResultProperty());
				ruleEvalObj.addLiteral(schemaProvider.getEvaluationHasConsistentResultProperty(), true); 	
			}
			updateRuleScope();
			// set new property value
			if (derivedDef.getDerivedType().getCollection() == ArlType.CollectionKind.SINGLE) {
				deriveSingleValue(result);
			} else if (derivedDef.getDerivedType().getCollection() == ArlType.CollectionKind.SET) {
				deriveSetValue((Set<?>)result);
			} else if (derivedDef.getDerivedType().getCollection() == ArlType.CollectionKind.LIST) {
				deriveListValue((List<?>)result);
			}
		}				
		return new AbstractMap.SimpleEntry<>(this, !Objects.equals(priorConsistency, newConsistency)); // returns if the outcome has changed;
	}

	private void deriveSingleValue(Object value) {
		var indiv = getContextInstance();
		var prop = derivedDef.getDerivedPredicate().asProperty();
		indiv.removeAll(prop);
		if (result == null) return; // when an entry is cleared/removed
		if (prop instanceof OntDataProperty dataProp) {
			indiv.addLiteral(dataProp, value);
		} else if (prop instanceof OntObjectProperty objProp) {
			indiv.addProperty(objProp.asNamed(),  (RDFNode)value);
		}
	}
	
	private void deriveSetValue(Set<?> result) {
		var indiv = getContextInstance();
		var prop = derivedDef.getDerivedPredicate().asProperty();
		indiv.removeAll(prop);
		if (result == null || result.isEmpty()) return; // when a set is cleared/removed
		if (prop instanceof OntDataProperty dataProp) {
			result.forEach(value -> indiv.addLiteral(dataProp, value) );
		} else if (prop instanceof OntObjectProperty objProp) {
			result.forEach(value -> indiv.addProperty(objProp.asNamed(),  (RDFNode)value));
		}
	}
	
	private void deriveListValue(List<?> result) {
		var indiv = getContextInstance();
		var prop = derivedDef.getDerivedPredicate().asProperty();
		var listContainer = getContextInstance().getPropertyResourceValue(prop);
		var list = (listContainer != null) ? listContainer.as(Seq.class) : null;
		if (result == null || result.isEmpty()) { // when a list  is cleared/removed
			var size = list.size();
			for (int i = size; i > 0 ; i--) { // if we remove from the start, all the remaining elements are moved down one step, --> very inefficient
				list.remove(i); 
			}
			return; 
		}
		if (prop instanceof OntDataProperty) {
			var model = indiv.getModel();
			result.forEach(value -> list.add(model.createTypedLiteral(value)));
		} else if (prop instanceof OntObjectProperty) {
			result.forEach(list::add);
		}
	}
	
}

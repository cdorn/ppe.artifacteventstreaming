package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.vocabulary.OWL2;

import at.jku.isse.artifacteventstreaming.rule.RuleRepository;
import at.jku.isse.artifacteventstreaming.rule.RuleSchemaFactory;
import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.core.RuleDefinition;
import at.jku.isse.passiveprocessengine.core.RuleResult;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RDFRuleResultWrapper extends RDFInstance implements RuleResult {

	private final RuleRepository ruleRepo;
	
	public RDFRuleResultWrapper(OntIndividual element, NodeToDomainResolver resolver, RuleRepository ruleRepo) {
		super(element, null, resolver);
		this.ruleRepo = ruleRepo;
	}
	
	@Override
	public String getName() {
		return element.getLabel();		
	}
	
	@Override
	public PPEInstanceType getInstanceType() {
		if (super.instanceType == null) {
			var types = element.as(OntIndividual.class).classes(true).toList();
			var optType = types.stream()
					.filter(type -> !type.getURI().equals(OWL2.NamedIndividual.getURI()))
					.filter(type -> !type.equals(ruleRepo.getFactory().getDefinitionType()))
					.findFirst();
			if (optType.isPresent()) {
				var type = resolver.findNonDeletedInstanceTypeByFQN(optType.get().getURI());
				if (type.isPresent() && type.get() instanceof RuleDefinition def)
					instanceType = def;
			}
		}
		return super.instanceType;
	}



	@Override
	public Boolean isConsistent() {
		return super.getTypedProperty(RuleSchemaFactory.ruleHasConsistentResultURI, Boolean.class, false);
	}

	@Override
	public PPEInstance getContextInstance() {
		var scope = element.getPropertyResourceValue(ruleRepo.getFactory().getContextElementScopeProperty().asProperty());
		if (scope != null) {
			var el = ruleRepo.getInspector().getElementFromScope(scope);
			if (el != null) {
				return (RDFInstance) resolver.resolveToRDFElement(el);
			} else {
				log.warn("Encountered rule scope without element reference "+element.getURI());
			}
		} else {
			log.warn("Encountered rule result without context scope in "+element.getURI());
		}
		return null;
		//return super.getTypedProperty("ruleContextElement", RDFInstance.class);
	}

}

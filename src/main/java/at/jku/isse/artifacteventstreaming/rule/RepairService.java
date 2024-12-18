package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObject;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;

import at.jku.isse.designspace.rule.arl.repair.RepairNode;
import lombok.NonNull;

public class RepairService {

	private final OntModel model;
	private final RuleRepository ruleRepo;
	private OntObjectProperty repairablePredicateProperty;

	public RepairService(OntModel model, RuleRepository ruleRepo) {
		super();
		this.model = model;
		this.ruleRepo = ruleRepo;
		initProperties();
	}				

	private void initProperties() {
		repairablePredicateProperty = model.getObjectProperty(RuleSchemaFactory.uri+"repairablePredicate");
		if (repairablePredicateProperty == null) {
			repairablePredicateProperty = model.createObjectProperty(RuleSchemaFactory.uri+"repairablePredicate");		
			repairablePredicateProperty.addDomain(model.createOntClass(OWL2.Class.getURI()));
			repairablePredicateProperty.addRange(model.createOntClass(RDF.Property.getURI()));
		}
	}

	public boolean isPropertyRepairable(@NonNull OntClass type, @NonNull Property property) {
		// TODO by default, no property is repairable -->  rethink this modeling?!				
		// repairability can be declared at multiple levels, independent where the property is defined (e.g., super class or sub class)
		var repairable = type.hasProperty(repairablePredicateProperty.asProperty(), property); // we use the property as a flag, if its there, means its repairable, if its not there, then its not repairable
		if (!repairable) {
			return type.superClasses(true).anyMatch(superClass -> isPropertyRepairable(superClass, property));
		} else {
			return repairable;
		}
	}

	public void setPropertyRepairable(@NonNull OntClass type, @NonNull Property property, boolean isRepairable) {    	    	
		if (isRepairable) {    	
			type.addProperty(repairablePredicateProperty.asProperty(), property);
		} else {    		
			type.remove(repairablePredicateProperty.asProperty(), property);        	
		}    	    	
	}
	
	public RepairNode getRepairRootNode(@NonNull OntObject ctxInstance, @NonNull RDFRuleDefinition def) {
		// find evaluations
		var optEval = ruleRepo.getEvaluations().findEvaluation(ctxInstance, def);
		if (optEval.isEmpty()) {
			return null;
		} else {
			var eval = optEval.get();
			return eval.getRepairTree();
		}
	}
}

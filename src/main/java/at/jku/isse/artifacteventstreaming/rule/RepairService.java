package at.jku.isse.artifacteventstreaming.rule;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;

import lombok.NonNull;

public class RepairService {

	private final OntModel model;
	private OntObjectProperty repairablePredicateProperty;

	public RepairService(OntModel model) {
		super();
		this.model = model;
		initProperties();
	}				

	private void initProperties() {
		repairablePredicateProperty = model.createObjectProperty(RuleSchemaFactory.uri+"repairablePredicate");
		repairablePredicateProperty.addDomain(model.createOntClass(OWL2.Class.getURI()));
		repairablePredicateProperty.addRange(model.createOntClass(RDF.Property.getURI()));
	}

	public boolean isPropertyRepairable(@NonNull OntClass type, @NonNull Property property) {
		// TODO by default, no property is repairable -->  rethink this modeling?!				
		// repairability can be declared at multiple levels
		var repairable = type.hasProperty(repairablePredicateProperty.asProperty(), property); 
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
}

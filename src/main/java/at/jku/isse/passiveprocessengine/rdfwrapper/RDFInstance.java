package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.vocabulary.RDF;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RDFInstance extends RDFElement implements PPEInstance {

	@Getter
	protected final OntIndividual instance;
	
	private PPEInstanceType type;

	public RDFInstance(OntIndividual element, NodeToDomainResolver resolver) {
		super(element, resolver);
		this.instance = element;
		//this.type = getInstanceType(); // does not work for RDFRuleResult that need the ruleRepo for its getInstanceType() method
	}
	
	@Override
	public PPEInstanceType getInstanceType() {
		if (type == null) {
			type = super.getInstanceType();
		} 
		return type;
	}

	@Override
	public void setInstanceType(PPEInstanceType childType) {
		if (BuildInType.isAtomicType(childType)) {
			log.warn(String.format("Tried to set instance type of %s to atomic type %s, not allowed, ignoring", instance.getURI(), childType.getId()));			
		} else {
			//if type is already a superclass
			var newType = ((RDFInstanceType)childType).getType();
			if (!instance.hasOntClass(newType, false)) {
				instance.addProperty(RDF.type, newType);
				//TODO: we assume we are setting to a more specific subtype, never a super type, in any case, we override the local cached type
				this.type = childType;
			}
		}
	}

}

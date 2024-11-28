package at.jku.isse.passiveprocessengine.rdfwrapper;

import org.apache.jena.ontapi.model.OntClass;

import at.jku.isse.passiveprocessengine.core.PPEInstanceType;

public interface AbstractionMapper {

	OntClass mapProcessDomainInstanceTypeToOntClass(PPEInstanceType ruleContext);

}

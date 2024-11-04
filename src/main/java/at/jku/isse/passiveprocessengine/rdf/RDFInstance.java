package at.jku.isse.passiveprocessengine.rdf;

import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.vocabulary.RDF;

import at.jku.isse.passiveprocessengine.core.PPEInstance;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import lombok.Getter;

public class RDFInstance extends RDFElement implements PPEInstance {

	@Getter
	protected final OntIndividual instance;

	public RDFInstance(OntIndividual element, NodeToDomainResolver resolver) {
		super(element, resolver);
		this.instance = element;
	}
	

	@Override
	public PPEInstanceType getInstanceType() {
		var stmt = instance.getProperty(RDF.type); // for now an arbitrary one of there are multiple ones
		if (stmt != null) {
			var node = stmt.getObject();
			return resolver.resolveToType(node);
		}
		return null;
	}

	


	@Override
	public void setInstanceType(PPEInstanceType childType) {
		// TODO Auto-generated method stub

	}



}

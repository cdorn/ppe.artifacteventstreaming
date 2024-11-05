package at.jku.isse.artifacteventstreaming.rdf;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.RDFNode;

import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.rdf.MapResourceType;
import at.jku.isse.passiveprocessengine.rdf.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdf.RDFInstanceType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RDFNodeToDomainResolver extends NodeToDomainResolver {

	private final OntModel model;
	@Getter
	private MapResourceType mapBase;
	
	
	@Override
	public PPEInstanceType resolveToType(RDFNode node) {
		if (node.isLiteral() || node instanceof OntDataRange.Named)
			return BuildInType.STRING; // just for testing
		else {
			PPEInstanceType type = new RDFInstanceType((OntClass) node, this);
			return type;
		}		
	}

	@Override
	public OntClass getMapEntryBaseType() {
		if (mapBase == null) {
			mapBase = new MapResourceType(model);			
		}
		return mapBase.getMapEntryClass();
	}

}

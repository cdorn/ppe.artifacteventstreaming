package at.jku.isse.artifacteventstreaming.rdf;


import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.rdf.model.RDFNode;
import at.jku.isse.passiveprocessengine.core.BuildInType;
import at.jku.isse.passiveprocessengine.core.PPEInstanceType;
import at.jku.isse.passiveprocessengine.rdf.ListResourceType;
import at.jku.isse.passiveprocessengine.rdf.MapResourceType;
import at.jku.isse.passiveprocessengine.rdf.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdf.RDFInstanceType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class RDFNodeToDomainResolver extends NodeToDomainResolver {

	private final OntModel model;
	@Getter
	private MapResourceType mapBase;
	@Getter
	private ListResourceType listBase;
	
	
	@Override
	public PPEInstanceType resolveToType(RDFNode node) {
		if (node instanceof OntDataRange.Named named) {
			RDFDatatype datatype = named.toRDFDatatype();
			if (datatype == null)
				return BuildInType.STRING; // just for testing
			// this is ugly, as we duplicate a type system, while we could work with the datatypes directly (but then having to rely on RDF/JENA)
			Class clazz = datatype.getJavaClass();
			if (clazz == null) {
				log.warn(String.format("Unsupported literal type %s returning STRING PPEInstanceType instead", datatype.getURI()));
				return BuildInType.STRING;
			}
			String clazzName = clazz.getSimpleName();			
			switch(clazzName) {
				case("Long"):
				case("Integer"):
					return BuildInType.INTEGER;
				case("Float"):
				case("Double"):
					return BuildInType.FLOAT;
				case("Date"):
					return BuildInType.DATE;
				case("Boolean"):
					return BuildInType.BOOLEAN;
				case("String"):
				case("Short"):
					return BuildInType.STRING;
				default: 
					log.warn(String.format("Unsupported literal type %s returning STRING PPEInstanceType instead", clazzName));
					return BuildInType.STRING;
			}
			
		} else if (node instanceof OntClass ontClass) {
			PPEInstanceType type = new RDFInstanceType(ontClass, this);
			return type;
		}		
		log.warn(String.format("Unknown RDFNode type %s cannot be resolved to a PPEInstanceType", node.toString()));
		return null;
	}

	@Override
	public OntClass getMapEntryBaseType() {
		if (mapBase == null) {
			mapBase = new MapResourceType(model);			
		}
		return mapBase.getMapEntryClass();
	}

	@Override
	public OntClass getListBaseType() {
		if (listBase == null) {
			listBase = new ListResourceType(model);
		}
		return listBase.getListClass();
	}

}

package at.jku.isse.passiveprocessengine.rdfwrapper.metaschema;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.vocabulary.XSD;

import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import lombok.Getter;

public class WrapperMetaModelSchemaTypes extends MetaModelSchemaTypes {

	private static final String META_CLASS_LOCALNAME = "MetaClass";

	public static final String META_NS = "http://isse.jku.at/artifactstreaming/metamodel#";
	
	@Getter public final OntClass metaClass;
	
	public WrapperMetaModelSchemaTypes(OntModel model, WrapperMetaModelOntology meta) {
		super(model, meta);
		metaClass = model.getOntClass(META_NS+META_CLASS_LOCALNAME);
	}

	public WrapperMetaModelSchemaTypes(OntModel model) {
		super(model);
		metaClass = model.getOntClass(META_NS+META_CLASS_LOCALNAME);
	}

	
	
	public static class WrapperMetaModelOntology extends MetaModelOntology {

		public WrapperMetaModelOntology(boolean isInMemory) {
			super(isInMemory);
			metaontology.begin(ReadWrite.WRITE);
			// extends with local ontologies
			//simple ones right in here
			var metaClass = metamodel.getOntClass(META_NS+META_CLASS_LOCALNAME);
			if (metaClass == null) {
				metaClass = metamodel.createOntClass(META_NS+META_CLASS_LOCALNAME);				
				var singleType = new SingleResourceType(metamodel); // we use this type provider only on the meta model, no actual runtime model uses this instance
				var mapType = new MapResourceType(metamodel, singleType);
				mapType.addLiteralMapProperty(metaClass, MetaElementFactory.propertyMetadataPredicate, metamodel.getDatatype(XSD.xstring));
			}
			
			// complicated ones in separate classes
			
			
			metaontology.commit();
			metaontology.end();
		}
		
	}
}

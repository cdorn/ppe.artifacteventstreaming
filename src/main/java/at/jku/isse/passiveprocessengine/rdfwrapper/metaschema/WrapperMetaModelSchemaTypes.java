package at.jku.isse.passiveprocessengine.rdfwrapper.metaschema;

import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.ReadWrite;

import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;
import at.jku.isse.passiveprocessengine.rdfwrapper.PrimitiveTypesFactory;
import lombok.Getter;

public class WrapperMetaModelSchemaTypes extends MetaModelSchemaTypes {

	@Getter public final MetaElementFactory metaElements;
	@Getter private final PrimitiveTypesFactory primitiveTypesFactory;
	
	public WrapperMetaModelSchemaTypes(OntModel model, WrapperMetaModelOntology meta) {
		super(model, meta);
		metaElements = new MetaElementFactory(model, super.getMapType());
		primitiveTypesFactory = new PrimitiveTypesFactory(model);
	}

	public WrapperMetaModelSchemaTypes(OntModel model) {
		super(model);
		metaElements = new MetaElementFactory(model, super.getMapType());
		primitiveTypesFactory = new PrimitiveTypesFactory(model);
	}
	
	public static class WrapperMetaModelOntology extends MetaModelOntology {

		public WrapperMetaModelOntology(boolean isInMemory) {
			super(isInMemory);
			metaontology.begin(ReadWrite.WRITE);
			// extends with local ontologies
			new MetaElementFactory.MetaSchemaFactory(metamodel);
			metaontology.commit();
			metaontology.end();
		}
		
		public static WrapperMetaModelOntology buildInMemoryOntology() {
			return new WrapperMetaModelOntology(true);
		}
		
		public static WrapperMetaModelOntology buildDBbackedOntology() {
			return new WrapperMetaModelOntology(false);
		}
	}
}

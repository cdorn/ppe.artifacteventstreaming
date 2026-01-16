package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.api.MetaModelOntologyProvider;
import at.jku.isse.artifacteventstreaming.schemasupport.MetaModelSchemaTypes.MetaModelOntology;

public class DefaultInMemoryMetaModelOntologyProvider implements MetaModelOntologyProvider {

	@Override
	public MetaModelOntology getMetaModelOntology() {
		return MetaModelSchemaTypes.MetaModelOntology.buildInMemoryOntology(); 	
	}
}

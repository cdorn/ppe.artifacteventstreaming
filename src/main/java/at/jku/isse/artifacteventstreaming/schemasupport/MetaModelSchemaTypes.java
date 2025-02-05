package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.List;
import java.util.Optional;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntDataProperty;
import org.apache.jena.ontapi.model.OntDataRange;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntObjectProperty;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.tdb2.TDB2Factory;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.replay.StatementAugmentationSession.StatementWrapper;
import lombok.Getter;
import lombok.NonNull;

public class MetaModelSchemaTypes {

	private static final String METAONTOLOGY = "metaontology";
	
	@Getter
	private MapResourceType mapType;
	@Getter
	private ListResourceType listType;
	@Getter
	private SingleResourceType singleType;	
	
	public MetaModelSchemaTypes(OntModel model, MetaModelOntology meta) {			
		// we init this model with the model fron the meta ontology
		meta.getMetaontology().begin(ReadWrite.READ);
		model.add(meta.getMetamodel());
		meta.getMetaontology().end();
				
		singleType = new SingleResourceType(model);
		mapType = new MapResourceType(model, singleType); 
		listType = new ListResourceType(model, singleType); 		
	}
	
	/**
	 * @deprecated Use {@link at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType#createBaseDataPropertyType(String,OntClass,OntDataRange)} instead
	 */
	public OntDataProperty createBaseDataPropertyType(String propUri, OntClass domain, OntDataRange range ) {
		return singleType.createBaseDataPropertyType(propUri, domain, range);
	}
	
	/**
	 * @deprecated Use {@link at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType#createBaseDataPropertyType(OntModel,String,List<OntClass>,OntDataRange)} instead
	 */
	public OntDataProperty createBaseDataPropertyType(@NonNull OntModel model, @NonNull String propUri, @NonNull List<OntClass> domains, @NonNull OntDataRange range ) {
		return singleType.createBaseDataPropertyType(model, propUri, domains, range);
	}

	/**
	 * @deprecated Use {@link at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType#createBaseObjectPropertyType(String,OntClass,OntClass)} instead
	 */
	public OntObjectProperty createBaseObjectPropertyType(@NonNull String propUri, @NonNull OntClass domain, @NonNull OntClass range ) {
		return singleType.createBaseObjectPropertyType(propUri, domain, range);
	}



	/**
	 * @deprecated Use {@link at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType#createSingleDataPropertyType(String,OntClass,OntDataRange)} instead
	 */
	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntDataRange range) {
		return singleType.createSingleDataPropertyType(propURI, domain, range);
	}
	
	/**
	 * @deprecated Use {@link at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType#createSingleDataPropertyType(String,List<OntClass>,OntDataRange)} instead
	 */
	public OntDataProperty createSingleDataPropertyType(@NonNull String propURI, @NonNull List<OntClass> domains, @NonNull OntDataRange range) {
		return singleType.createSingleDataPropertyType(propURI, domains, range);
	}


	/**
	 * @deprecated Use {@link at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType#createSingleObjectPropertyType(String,OntClass,OntClass)} instead
	 */
	public OntObjectProperty createSingleObjectPropertyType(@NonNull String propURI, @NonNull OntClass domain, @NonNull OntClass range) {
		return singleType.createSingleObjectPropertyType(propURI, domain, range);
	}

	public Optional<Resource> getCurrentListOwner(OntIndividual list) {
		return Optional.ofNullable(list.getPropertyResourceValue(getListType().getContainerProperty().asProperty()));
	}

	public Optional<Resource> getFormerListOwner(List<StatementWrapper> stmts) {
		return stmts.stream().filter(wrapper -> wrapper.op().equals(AES.OPTYPE.REMOVE))
			.map(StatementWrapper::stmt)
			.filter(stmt -> stmt.getPredicate().equals(getListType().getContainerProperty().asProperty()))
			.map(Statement::getResource)
			.findAny();
	}
	
	public static class MetaModelOntology {
		
		@Getter protected final Dataset metaontology;
		@Getter protected final OntModel metamodel;
		
		public MetaModelOntology(boolean isInMemory) {
			metaontology = loadMetaSchemaFromDB(isInMemory);
			metaontology.begin(ReadWrite.WRITE);
			this.metamodel = OntModelFactory.createModel(metaontology.getDefaultModel().getGraph(), OntSpecification.OWL2_DL_MEM);
			new SingleResourceType.SingleSchemaFactory(metamodel);
			new MapResourceType.MapSchemaFactory(metamodel);
			new ListResourceType.ListSchemaFactory(metamodel);
			metaontology.commit();
			metaontology.end();
		}
		
		private Dataset loadMetaSchemaFromDB(boolean isInMemory) {
			String directory = "ontologies/"+METAONTOLOGY ;
			if (isInMemory) {
				return TDB2Factory.createDataset();
			} else {
				Dataset dataset = TDB2Factory.connectDataset(directory) ;
				if (dataset == null) throw new RuntimeException("Cannot load meta ontology dataset");
				return dataset;
			}
		}
		
		public static MetaModelOntology buildInMemoryOntology() {
			return new MetaModelOntology(true);
		}
		
		public static MetaModelOntology buildDBbackedOntology() {
			return new MetaModelOntology(false);
		}
	}
	
}

package at.jku.isse.artifacteventstreaming.schemasupport;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.jena.ontapi.OntModelFactory;
import org.apache.jena.ontapi.OntSpecification;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.ontapi.model.OntProperty;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.vocabulary.RDFS;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.replay.StatementAugmentationSession.StatementWrapper;
import lombok.Getter;

public class MetaModelSchemaTypes {

	private static final String METAONTOLOGY = "metaontology";
	
	@Getter
	private final MapResourceType mapType;
	@Getter
	private final ListResourceType listType;
	@Getter
	private final SingleResourceType singleType;	
	@Getter
	private final SetResourceType setType = new SetResourceType();
	
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
	 * 
	 * @param model  pre-filled model that already contains metamodelontology
	 */
	public MetaModelSchemaTypes(OntModel model) {									
		singleType = new SingleResourceType(model);
		mapType = new MapResourceType(model, singleType); 
		listType = new ListResourceType(model, singleType); 		
	}
	
	/**
	 * @param ontClass and any of its subclasses to delete including all properties that were explicitly 
	 * defined to be in the domain of these classes, 
	 * do not use if an affected property is in the domain of multiple classes  
	 */
	public void deleteOntClassInclSubclasses(OntClass ontClass) {
		ontClass.subClasses().forEach(this::deleteOntClassInclOwnedProperties);
		deleteOntClassInclOwnedProperties(ontClass);
	}
	
	/**
	 * @param ontClass to delete including all properties that were explicitly 
	 * defined to be in the domain of that class, 
	 * super and subclasses and their properties are not removed, 
	 * removing a class in the middle of a hierarchy will leave its subclasses dangling
	 * to include deletion of subclasses, use {@link #deleteOntClassInclSubclasses(OntClass) }
	 * do not use if an affected property is in the domain of multiple classes  
	 */
	public void deleteOntClassInclOwnedProperties(OntClass ontClass) {
		getExplicitlyDeclaredProperties(ontClass).forEach(prop -> {
			// determine set/map/list/single type
			// then call matching delete
			if (singleType.isSingleProperty(prop)) {
				singleType.removeSingleProperty(ontClass, prop);
			} else
			if (listType.isListContainerReferenceProperty(prop)) {
				listType.removeListContainerReferenceProperty(ontClass, prop);
			} else 
			if (mapType.isMapContainerReferenceProperty(prop)) {
				mapType.removeMapContainerReferenceProperty(prop);
			} else {
				singleType.removeBaseProperty(prop);
			}
		});
		ontClass.removeProperties();
	}
	
	/**
	 * @param ontClass for which to retrieve only those properties that have in domain this particular class (without using inference), therefore ignoring class/type hierarchy
	 * @return any property that has the provided ontClass as domain
	 */
	public static Stream<OntProperty> getExplicitlyDeclaredProperties(OntClass ontClass) {
		var model = ontClass.getModel();
		var iter = model.listResourcesWithProperty(RDFS.domain, ontClass);
		List<Resource> predicates = new LinkedList<>();
		while (iter.hasNext()) {
			predicates.add(iter.next());
		}
		return predicates.stream().map(res -> res.as(OntProperty.class));
		
		//Stream<OntProperty> properties = Stream.of(model.objectProperties(), model.dataProperties(), model.annotationProperties()).flatMap(it -> it);;
		//return properties.distinct().filter(prop -> prop.domains().anyMatch(domain -> domain.equals(ontClass)));
	}

	public Optional<Resource> getCurrentListOwner(OntIndividual list) {
		return Optional.ofNullable(list.getPropertyResourceValue(getListType().getContainerProperty().asProperty()));
	}

	public Optional<Resource> getFormerListOwner(List<StatementWrapper> stmts) {
		return stmts.stream().filter(wrapper -> wrapper.getOp().equals(AES.OPTYPE.REMOVE))
			.map(StatementWrapper::getStmt)
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

package at.jku.isse.passiveprocessengine.rdfwrapper.metaschema;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.vocabulary.XSD;

import at.jku.isse.artifacteventstreaming.schemasupport.MapResource;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetaElementFactory {

	private static final String META_CLASS_LOCALNAME = "MetaClass";
	public static final String META_NS = "http://isse.jku.at/artifactstreaming/metamodel#";
	
	public static final String propertyMetadataPredicate_URI = META_NS+"propertyMetadata";

	private static final String constructorIndexType_URI = META_NS+"ConstructorIndex";
	private static final String constructorIndex_URI = META_NS+"ConstructorIndex1";
	private static final String propertyConstructorForClass_URI = META_NS+"useConstructorForClass";
	
	private final MapResourceType mapType;
	private final Map<String, Constructor<? extends RDFInstance>> instanceConstructors = new HashMap<>();
	private MapResource rawIndex;
	private final OntModel model;
	
	@Getter public final OntClass metaClass;
	
	public MetaElementFactory(@NonNull OntModel model, @NonNull MapResourceType mapType) {
		this.mapType = mapType;		
		this.model = model;
		metaClass = model.getOntClass(META_NS+META_CLASS_LOCALNAME);
		loadInstanceSubtypeClasses(model);
	}
	
	private void loadInstanceSubtypeClasses(OntModel model) {
		var constructorIndexType = model.getOntClass(constructorIndexType_URI);
		var constructorIndexInstance = constructorIndexType.createIndividual(constructorIndex_URI);
		var propertyConstructorForClass = model.getObjectProperty(propertyConstructorForClass_URI);
		rawIndex = MapResource.asUnsafeMapResource(constructorIndexInstance, propertyConstructorForClass.asNamed(), mapType);
		rawIndex.entrySet().stream()
			.filter(entry -> entry.getValue().isLiteral())
			.forEach(entry -> {
			var fqn = entry.getValue().asLiteral().getString();
			try {
				var clazz = Class.forName(fqn);
				if (clazz.isInstance(RDFInstance.class)) {
					Constructor<? extends RDFInstance> constructor = (Constructor<? extends RDFInstance>) clazz.getConstructor(OntIndividual.class, RDFInstanceType.class, NodeToDomainResolver.class);
					instanceConstructors.put(entry.getKey(), constructor);
				} else {
					log.warn(String.format("Persisted ConstructorIndex contained class %s that is not a subtype of RDFInstance", fqn));
				}
			} catch(Exception e) {
				log.warn(String.format("Error obtaining constructor from %s due to %s", fqn, e.getMessage()));
			}
		});
	}
	
	public Constructor<? extends RDFInstance> getConstructorForNamspace(String uri) {
		return instanceConstructors.get(uri);
	}
	
	public void registerInstanceSpecificClass(@NonNull String uri, @NonNull Class<? extends RDFInstance> clazz) {
		try {
			var constructor = clazz.getConstructor(OntIndividual.class, RDFInstanceType.class, NodeToDomainResolver.class);
			instanceConstructors.put(uri, constructor);
			//store with branch if never stored before or different fqn
			var fqn = clazz.getCanonicalName();
			var currentValue = rawIndex.get(uri);
			if (currentValue == null || !currentValue.isLiteral() || !currentValue.asLiteral().getString().equals(fqn) ) {
				rawIndex.put(uri, model.createTypedLiteral(fqn));
			}
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
			log.error(e.getMessage());
		}		
	}
	
	
	
	protected static class MetaSchemaFactory {
		
		private final OntModel model;

		public MetaSchemaFactory(OntModel metaOntology) {
			this.model = metaOntology;	
			initTypes();			
		}				

		private void initTypes() {
			var singleType = new SingleResourceType(model); // we use this type provider only on the meta model, no actual runtime model uses this instance
			var mapType = new MapResourceType(model, singleType);
			
			var metaClass = model.getOntClass(META_NS+META_CLASS_LOCALNAME);
			if (metaClass == null) {
				metaClass = model.createOntClass(META_NS+META_CLASS_LOCALNAME);				
				mapType.addLiteralMapProperty(metaClass, MetaElementFactory.propertyMetadataPredicate_URI, model.getDatatype(XSD.xstring));
			}
			
			var constructorIndex = model.getOntClass(constructorIndexType_URI);
			if (constructorIndex == null) {
				constructorIndex = model.createOntClass(constructorIndexType_URI);
				mapType.addLiteralMapProperty(constructorIndex, propertyConstructorForClass_URI, model.getDatatype(XSD.xstring));
			}
			
		}
	}
}

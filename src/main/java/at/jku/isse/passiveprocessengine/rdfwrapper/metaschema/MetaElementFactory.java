package at.jku.isse.passiveprocessengine.rdfwrapper.metaschema;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.ontapi.model.OntIndividual;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.vocabulary.XSD;

import at.jku.isse.artifacteventstreaming.schemasupport.BasePropertyType;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResource;
import at.jku.isse.artifacteventstreaming.schemasupport.MapResourceType;
import at.jku.isse.artifacteventstreaming.schemasupport.SingleResourceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.NodeToDomainResolver;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstance;
import at.jku.isse.passiveprocessengine.rdfwrapper.RDFInstanceType;
import at.jku.isse.passiveprocessengine.rdfwrapper.rule.RuleEnabledResolver;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetaElementFactory {

	private static final String META_CLASS_LOCALNAME = "MetaClass";
	public static final String META_NS = "http://isse.jku.at/artifactstreaming/metamodel#";
	
	public static final String propertyMetadataPredicate_URI = META_NS+"propertyMetadata";

	private static final String constructorIndexType_URI = META_NS+"ConstructorIndex";
	private static final String constructorIndex_URI = META_NS+"DefaultInstanceConstructorIndex";
	private static final String propertyConstructorForClass_URI = META_NS+"useConstructorForInstance";
	private static final String propertyTypeConstructorForClass_URI = META_NS+"useConstructorForType";
	
	private final MapResourceType mapType;
	private final Map<String, Constructor<? extends RDFInstance>> instanceConstructors = new HashMap<>();
	private final Map<String, Constructor<? extends RDFInstanceType>> typeConstructors = new HashMap<>();
	private MapResource rawInstanceConstructorIndex;
	private MapResource rawTypeConstructorIndex;
	private final OntModel model;
	
	@Getter public final OntClass metaClass;
	
	public MetaElementFactory(@NonNull OntModel model, @NonNull MapResourceType mapType) {
		this.mapType = mapType;		
		this.model = model;
		metaClass = model.getOntClass(META_NS+META_CLASS_LOCALNAME);
		loadInstanceConstructors(model);
		loadTypeConstructors(model);
	}
	
	private void loadInstanceConstructors(OntModel model) {
		var constructorIndexType = model.getOntClass(constructorIndexType_URI);
		var constructorIndexResource = constructorIndexType.createIndividual(constructorIndex_URI);
		var propertyConstructorForClass = model.getObjectProperty(propertyConstructorForClass_URI);
		rawInstanceConstructorIndex = MapResource.asUnsafeMapResource(constructorIndexResource, propertyConstructorForClass.asNamed(), mapType);
		rawInstanceConstructorIndex.entrySet().stream()
			.filter(entry -> entry.getValue().isLiteral())
			.forEach(entry -> {
			var fqn = entry.getValue().asLiteral().getString();
			try {
				var clazz = Class.forName(fqn);
				if (RDFInstance.class.isAssignableFrom(clazz)) {
					Constructor<? extends RDFInstance> constructor = (Constructor<? extends RDFInstance>) clazz.getConstructor(OntIndividual.class, RDFInstanceType.class, NodeToDomainResolver.class);
					instanceConstructors.put(entry.getKey(), constructor);
				} else {
					log.warn(String.format("Persisted Instance ConstructorIndex contained class %s that is not a subtype of RDFInstance", fqn));
				}
			} catch(Exception e) {
				log.warn(String.format("Error obtaining constructor from %s due to %s", fqn, e.getMessage()));
			}
		});
	}
	
	private void loadTypeConstructors(OntModel model) {
		var constructorIndexType = model.getOntClass(constructorIndexType_URI);
		var constructorIndexResource = constructorIndexType.createIndividual(constructorIndex_URI);
		var propertyConstructorForClass = model.getObjectProperty(propertyTypeConstructorForClass_URI);
		rawTypeConstructorIndex = MapResource.asUnsafeMapResource(constructorIndexResource, propertyConstructorForClass.asNamed(), mapType);
		rawTypeConstructorIndex.entrySet().stream()
			.filter(entry -> entry.getValue().isLiteral())
			.forEach(entry -> {
			var fqn = entry.getValue().asLiteral().getString();
			try {
				var clazz = Class.forName(fqn);
				if (RDFInstanceType.class.isAssignableFrom(clazz)) {
					Constructor<? extends RDFInstanceType> constructor = (Constructor<? extends RDFInstanceType>) clazz.getConstructor(OntClass.class, RuleEnabledResolver.class);
					typeConstructors.put(entry.getKey(), constructor);
				} else {
					log.warn(String.format("Persisted Type ConstructorIndex contained class %s that is not a subtype of RDFInstanceType", fqn));
				}
			} catch(Exception e) {
				log.warn(String.format("Error obtaining constructor from %s due to %s", fqn, e.getMessage()));
			}
		});
	}
	
	public Constructor<? extends RDFInstance> getInstanceConstructorForNamespace(String uri) {
		return instanceConstructors.get(uri);
	}
	
	public Constructor<? extends RDFInstanceType> getTypeConstructorForNamespace(String uri) {
		return typeConstructors.get(uri);
	}
	
	public void unregisterTypeSpecificClass(@NonNull String uri) {
		rawTypeConstructorIndex.remove(uri);
		typeConstructors.remove(uri);
	}
	
	public void registerTypeSpecificClass(@NonNull String uri, @NonNull Class<? extends RDFInstanceType> clazz) {
		var constructor = findTypeConstructor(clazz);
		if (constructor == null) {
			log.error("Couldn't find expected constructor for: "+clazz.getName());
			return;
		} 
		//clazz.getConstructor(OntIndividual.class, RDFInstanceType.class, NodeToDomainResolver.class);
		typeConstructors.put(uri, constructor);
		//store with branch if never stored before or different fqn
		var fqn = clazz.getCanonicalName();
		var currentValue = rawTypeConstructorIndex.get(uri);
		if (currentValue == null || !currentValue.isLiteral() || !currentValue.asLiteral().getString().equals(fqn) ) {
			rawTypeConstructorIndex.put(uri, model.createTypedLiteral(fqn));
		}
	}
	
	private Constructor<? extends RDFInstanceType> findTypeConstructor(@NonNull Class<? extends RDFInstanceType> clazz) {
		for (Constructor constr : clazz.getConstructors()) {
			if (constr.getParameterCount() == 2 && RuleEnabledResolver.class.isAssignableFrom(constr.getParameterTypes()[1]))
				return constr;
		} 
		return null;
	}
	
	public void unregisterInstanceSpecificClass(@NonNull String uri) {
		rawInstanceConstructorIndex.remove(uri);
		instanceConstructors.remove(uri);
	}
	
	public void registerInstanceSpecificClass(@NonNull String uri, @NonNull Class<? extends RDFInstance> clazz) {
		var constructor = findConstructor(clazz);
		if (constructor == null) {
			log.error("Couldn't find constructor for: "+clazz.getName());
			return;
		} 
		//clazz.getConstructor(OntIndividual.class, RDFInstanceType.class, NodeToDomainResolver.class);
		instanceConstructors.put(uri, constructor);
		//store with branch if never stored before or different fqn
		var fqn = clazz.getCanonicalName();
		var currentValue = rawInstanceConstructorIndex.get(uri);
		if (currentValue == null || !currentValue.isLiteral() || !currentValue.asLiteral().getString().equals(fqn) ) {
			rawInstanceConstructorIndex.put(uri, model.createTypedLiteral(fqn));
		}
	}
	
	private Constructor<? extends RDFInstance> findConstructor(@NonNull Class<? extends RDFInstance> clazz) {
		for (Constructor constr : clazz.getConstructors()) {
			if (constr.getParameterCount() == 3 && NodeToDomainResolver.class.isAssignableFrom(constr.getParameterTypes()[2]))
				return constr;
		} 
		return null;
	}
	
	protected static class MetaSchemaFactory {
		
		private final OntModel model;

		public MetaSchemaFactory(OntModel metaOntology) {
			this.model = metaOntology;	
			initTypes();			
		}				

		private void initTypes() {
			var baseType = new BasePropertyType(model);// we use this type provider only on the meta model, no actual runtime model uses this instance
			var mapType = new MapResourceType(model, baseType);
			
			var metaClass = model.getOntClass(META_NS+META_CLASS_LOCALNAME);
			if (metaClass == null) {
				metaClass = model.createOntClass(META_NS+META_CLASS_LOCALNAME);				
				mapType.addLiteralMapProperty(metaClass, MetaElementFactory.propertyMetadataPredicate_URI, model.getDatatype(XSD.xstring));
			}
			
			var instanceConstructorIndex = model.getOntClass(constructorIndexType_URI);
			if (instanceConstructorIndex == null) {
				instanceConstructorIndex = model.createOntClass(constructorIndexType_URI);
				mapType.addLiteralMapProperty(instanceConstructorIndex, propertyConstructorForClass_URI, model.getDatatype(XSD.xstring));
				mapType.addLiteralMapProperty(instanceConstructorIndex, propertyTypeConstructorForClass_URI, model.getDatatype(XSD.xstring));
				// we use the same resource to store instance constructor names and type constructor names
			}
		}
	}
}

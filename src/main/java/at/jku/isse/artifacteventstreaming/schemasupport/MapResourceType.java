package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.api.TransactionAware;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.model.*;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.function.library.leviathan.log;
import org.apache.jena.vocabulary.XSD;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class MapResourceType implements TransactionAware {

	public static final String OBJECT_VALUE = "objectValue";
	public static final String LITERAL_VALUE = "literalValue";
	private static final String ENTRY_TYPE = "EntryType";
	public static final String MAP_NS = "http://at.jku.isse.map#";	
	public static final String ENTRY_TYPE_URI = MAP_NS+ENTRY_TYPE;
	public static final String KEY_PROPERTY_URI = MAP_NS+"key";
	public static final String LITERAL_VALUE_PROPERTY_URI = MAP_NS+LITERAL_VALUE;
	public static final String OBJECT_VALUE_PROPERTY_URI = MAP_NS+OBJECT_VALUE;
	public static final String CONTAINEROWNER_PROPERTY_URI = MAP_NS+"containerOwnerRef";
	public static final String MAP_OWNERSHIP_SUPERPROPERTY_URI = MAP_NS+"mapRef";
		
	@Getter
	private final OntDataProperty keyProperty;
	@Getter
	private final OntDataProperty literalValueProperty;
	@Getter
	private final OntObjectProperty.Named objectValueProperty;
	@Getter
	private final OntObjectProperty mapOwnedByProperty;
	@Getter
	private final OntObjectProperty mapOwnershipSuperProperty;
	
	@Getter
	private final OntClass mapEntryClass;
	private final BasePropertyType primaryPropertyType;

	// caching for performance to avoid searching rdf model
	@Getter(AccessLevel.PACKAGE)
	private final Set<OntClass> subclassesCache = new HashSet<>();
	@Getter(AccessLevel.PACKAGE)
    private final Set<Property> ownsPropertyCache = new HashSet<>();

	// transaction rollback tracking
	private final Set<OntClass> removedSubclassesDuringTx = new HashSet<>();
	private final Set<OntClass> addedSubclassesDuringTx = new HashSet<>();
	private final Set<Property> removedOwnsPropertyDuringTx = new HashSet<>();
	private final Set<Property> addedOwnsPropertyDuringTx = new HashSet<>();

	public MapResourceType(@NonNull OntModel model, @NonNull BasePropertyType primaryType) {
        this.primaryPropertyType = primaryType;
		mapEntryClass = model.getOntClass(ENTRY_TYPE_URI);
		keyProperty = model.getDataProperty(KEY_PROPERTY_URI);
		literalValueProperty = model.getDataProperty(LITERAL_VALUE_PROPERTY_URI);
		objectValueProperty = model.getObjectProperty(OBJECT_VALUE_PROPERTY_URI);
		mapOwnedByProperty = model.getObjectProperty(CONTAINEROWNER_PROPERTY_URI);
		mapOwnershipSuperProperty = model.getObjectProperty(MAP_OWNERSHIP_SUPERPROPERTY_URI);
		initHierarchyCache();
	}		
			
	private void initHierarchyCache() {
		mapEntryClass.subClasses().forEach(subclassesCache::add);
		mapOwnershipSuperProperty.subProperties(true)
				.forEach(subProp -> ownsPropertyCache.add(subProp.asProperty()));
	}

	public boolean isMapEntrySubclass(OntObjectProperty mapEntryProperty) {
		return  mapEntryProperty.ranges(true).anyMatch(rangeClass -> rangeClass.equals(mapEntryClass)
				|| rangeClass.hasSuperClass(mapEntryClass, true));
	}
	
	public boolean isMapContainerReferenceProperty(OntProperty prop) {
		return mapOwnershipSuperProperty.subProperties(true).anyMatch(subProp -> subProp.equals(prop));
	}
	
	public OntObjectProperty addLiteralMapProperty(OntClass resource, String propertyURI, OntDataRange valueType) {
		OntModel model = resource.getModel();
		if (primaryPropertyType.existsPrimaryProperty(propertyURI)) {
			return null;  //as we cannot guarantee that the property that was identified is an OntObjectProperty		
		}
		var uri = generateMapEntryTypeURI(propertyURI);
		var mapType = model.getOntClass(uri);
		if (mapType != null) { 
			return null; // such a class already exists, do not need to create properties for it
		}
		mapType = model.createOntClass(uri);
		mapType.addSuperClass(mapEntryClass);
		subclassesCache.add(mapType);

		//use base property to enable tracking of existing properties
		OntDataProperty valueProp = primaryPropertyType.createBaseDataPropertyType(model, propertyURI+LITERAL_VALUE, List.of(mapType), valueType);
		valueProp.addSuperProperty(literalValueProperty);


		OntObjectProperty hasMap = primaryPropertyType.createBaseObjectPropertyType(resource.getModel(), propertyURI, List.of(resource), mapType);
		mapOwnershipSuperProperty.addSubProperty(hasMap);
		ownsPropertyCache.add(hasMap.asProperty());
		return hasMap;
	}

	public OntObjectProperty addObjectMapProperty(OntClass resource, String propertyURI, OntClass valueType) {
		OntModel model = resource.getModel();
		if (primaryPropertyType.existsPrimaryProperty(propertyURI)) {
			return null;  //as we cannot guarantee that the property that was identified is an OntObjectProperty		
		}
		var uri = generateMapEntryTypeURI(propertyURI);
		var mapType = model.getOntClass(uri);
		if (mapType != null) { 
			return null; // such a class already exists, do not need to create properties for it
		}
		mapType = model.createOntClass(uri);
		mapType.addSuperClass(mapEntryClass);
		subclassesCache.add(mapType);

		OntObjectProperty valueProp = primaryPropertyType.createBaseObjectPropertyType(resource.getModel(), propertyURI+OBJECT_VALUE, List.of(mapType), valueType);
		valueProp.addSuperProperty(objectValueProperty);

		OntObjectProperty hasMap = primaryPropertyType.createBaseObjectPropertyType(resource.getModel(), propertyURI, List.of(resource), mapType);
		mapOwnershipSuperProperty.addSubProperty(hasMap);
		ownsPropertyCache.add(hasMap.asProperty());
		return hasMap;
	}

	private String generateMapEntryTypeURI(String propertyURI) {
		return propertyURI+ENTRY_TYPE;
	}

	// Remote changes syncing in (deleted/adding of property definitions )

	public void addToOwnershipPropertyCacheIfApplicable(Property prop, Set<Resource> domains) {

		String baseURI = attemptStripEnding(prop.getURI());

		// if this property matches the subtype for any of the domains, then this is a map resource property
		var optSubtype = domains.stream()
				.filter(domain -> domain.getURI().equals(generateMapEntryTypeURI(baseURI)))
				.findAny();
		if (optSubtype.isPresent()) {
			var baseProp = mapEntryClass.getModel().getProperty(baseURI);
			if (ownsPropertyCache.add(baseProp)) {
				if (!removedOwnsPropertyDuringTx.remove(baseProp)) {
					// only add to temp if this has not been removed, so non-effective changes wont show up in rollback info
					addedOwnsPropertyDuringTx.add(baseProp);
				}
			}
			var typeRes = optSubtype.get();
			var subtypeClass = mapEntryClass.getModel().getOntClass(typeRes.getURI());
			if (subtypeClass != null) {
				if (subclassesCache.add(subtypeClass)) {
					if (!removedSubclassesDuringTx.remove(subtypeClass)) {
						addedSubclassesDuringTx.add(subtypeClass);
					}
				}
			} else {
				log.error("Schema Corruption: resource {} is in domain of map property {} with matching expected uri but not an ontclass", typeRes.getURI(), prop.getURI());
			}
		}
		// too slow:
		//		if (prop.canAs(OntObjectProperty.class)) {
//			var ontProp = prop.as(OntObjectProperty.class);
//			// rather expensive, better to find additional hints to evaluate this
//			if (ontProp.superProperties(true).anyMatch(sp -> sp.equals(mapOwnershipSuperProperty))) {
//				ownsPropertyCache.add(prop);
//			}
//		}
	}

	private String attemptStripEnding(String uri) {
		if (uri.endsWith(OBJECT_VALUE)) {
			return uri.substring(0, uri.length()-OBJECT_VALUE.length());
		} else if (uri.endsWith(LITERAL_VALUE)) {
			return uri.substring(0, uri.length()-LITERAL_VALUE.length());
		}
		return uri;
	}

	/**
	 * used when notified about external (i.e., synced) removal of property, hence underlying model contains no triples anymore, just cleanup cache
	 * @param propertyURI
	 */
	public void cleanupCacheAfterRemotePropertyRemoval(String propertyURI) {
		//keep in sync with method below
		ownsPropertyCache.stream()
				.filter(p -> p.getURI().equals(propertyURI))
				.findFirst()
				.ifPresent(p -> {
					ownsPropertyCache.remove(p);
					if (!addedOwnsPropertyDuringTx.remove(p)) {
						removedOwnsPropertyDuringTx.add(p);
					}
				});
		var mapTypeURI = generateMapEntryTypeURI(propertyURI);
		subclassesCache.stream()
				.filter(c -> c.getURI().equals(mapTypeURI))
				.findFirst()
				.ifPresent(c -> {
					subclassesCache.remove(c);
					if (!addedSubclassesDuringTx.remove(c)) {
						removedSubclassesDuringTx.add(c);
					}
				});
	}

	@Override
	public void afterTransactionStarted() {
		clearRollbackInfo();
	}

	@Override
	public void afterTransactionAborted() {
		subclassesCache.addAll(removedSubclassesDuringTx);
		subclassesCache.removeAll(addedSubclassesDuringTx);
		ownsPropertyCache.addAll(removedOwnsPropertyDuringTx);
		ownsPropertyCache.removeAll(addedOwnsPropertyDuringTx);
		clearRollbackInfo();
	}

	@Override
	public void afterTransactionCommitted() {
		clearRollbackInfo();
	}

	private void clearRollbackInfo() {
		removedSubclassesDuringTx.clear();
		addedSubclassesDuringTx.clear();
		removedOwnsPropertyDuringTx.clear();
		addedOwnsPropertyDuringTx.clear();
	}

	/**
	 * @param mapOwnershipProperty OntProperty to remove from its owning class including the specific map entry type and its value predicate
	 */
	public void removeMapOwnershipPropertyDefinition(OntProperty mapOwnershipProperty) {
		var model = mapOwnershipProperty.getModel();
		// remove listType:
		var mapType = model.getOntClass(generateMapEntryTypeURI(mapOwnershipProperty.getURI()));
		if (mapType != null) {
			// remove from cache
			subclassesCache.remove(mapType);
			// remove any predicates from any properties that happen to be defined
			MetaModelSchemaTypes.getExplicitlyDeclaredProperties(mapType).forEach(primaryPropertyType::removeBaseProperty);
			// remove predicates association from mapType itself
			mapType.removeProperties();
			// remove map reference property
		}
		ownsPropertyCache.remove(mapOwnershipProperty.asProperty());
		primaryPropertyType.removeBaseProperty(mapOwnershipProperty);
	}



	// Instance-level checks ================================================

	public boolean isMapEntry(Resource ontInd) {
		// TOO slow: return ontInd.classes(true).anyMatch(type -> subclassesCache.contains(type) || type.equals(mapEntryClass));
		// as we control map entry creation, we can take a shortcut:
		var uri = ontInd.getURI();
		return uri != null && uri.startsWith(UntypedMapResource.MAP_ENTRY_URI_PREFIX);
	}
	
//	public boolean wasMapEntry(List<Resource> delTypes) {
//		// too slow, see above
//		return delTypes.stream().anyMatch(type -> type.getURI().equals(getMapEntryClass().getURI()) ||
//				subclassesCache.stream().map(RDFNode::asResource).anyMatch(clazz -> clazz.equals(type))  );
//	}

	public boolean wasMapEntryBasedOnURI(Resource res) {
		// too slow, see above
		//return delTypes.stream().anyMatch(type -> type.getURI().equals(getMapEntryClass().getURI()) ||
		//		subclassesCache.stream().map(RDFNode::asResource).anyMatch(clazz -> clazz.equals(type))  );
		return isMapEntry(res);
	}

	public Property findReferencePropertyFromCache(Resource owner, Resource entry) {
		var iter = owner.getModel().listStatements(owner, null, entry);
		while (iter.hasNext()) {
			Property pred = iter.next().getPredicate();
			if (ownsPropertyCache.contains(pred)) {
				iter.close();
				return pred;
			}
		}
		return null;
	}

	public List<Property> findMapReferencePropertiesBetween(Resource subject, Resource mapEntry) {
		List<Property> props = new ArrayList<>();
		var iter = subject.getModel().listStatements(subject, null, mapEntry);
		while (iter.hasNext()) {
			props.add(iter.next().getPredicate());
		}
		if (props.size() > 1) {
			props.remove(mapOwnershipSuperProperty.asProperty());
		}
		return props;
	}
	
	protected String hashAsIdPart(String... args) {
		return primaryPropertyType.hashAsIdPart(args);
	}
	
	protected static class MapSchemaFactory {
		
		
		private final OntModel model;
		
		public MapSchemaFactory(OntModel metaOntology) {
			this.model = metaOntology;		
			initTypes();			

		}				
		
		private void initTypes() {
			var mapEntryClass = model.getOntClass(ENTRY_TYPE_URI);
			if (mapEntryClass == null) {
				mapEntryClass = model.createOntClass(ENTRY_TYPE_URI);
			}
			
			var keyProp = model.getDataProperty(KEY_PROPERTY_URI);
			if (keyProp == null) {
				keyProp = model.createDataProperty(KEY_PROPERTY_URI);
				keyProp.addDomain(mapEntryClass);
				keyProp.addRange(model.getDatatype(XSD.xstring));
			}
			
			var literalValueProp = model.getDataProperty(LITERAL_VALUE_PROPERTY_URI);
			if (literalValueProp == null) {
				literalValueProp = model.createDataProperty(LITERAL_VALUE_PROPERTY_URI);			
				literalValueProp.addDomain(mapEntryClass);
			}
			
			var objectValueProp = model.getObjectProperty(OBJECT_VALUE_PROPERTY_URI);
			if (objectValueProp == null) {
				objectValueProp = model.createObjectProperty(OBJECT_VALUE_PROPERTY_URI);
				objectValueProp.addDomain(mapEntryClass);
			}
			
			var containerProperty = model.getObjectProperty(CONTAINEROWNER_PROPERTY_URI);
			if (containerProperty == null) {
				containerProperty = model.createObjectProperty(CONTAINEROWNER_PROPERTY_URI);
				containerProperty.addDomain(mapEntryClass);
			}
			
			var mapReferenceSuperProperty = model.getObjectProperty(MAP_OWNERSHIP_SUPERPROPERTY_URI);
			if (mapReferenceSuperProperty == null) {
				mapReferenceSuperProperty = model.createObjectProperty(MAP_OWNERSHIP_SUPERPROPERTY_URI);
				mapReferenceSuperProperty.addRange(mapEntryClass);
			}
		}
	}
}

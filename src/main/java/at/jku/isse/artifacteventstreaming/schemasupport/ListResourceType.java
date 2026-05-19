package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.api.AES;
import at.jku.isse.artifacteventstreaming.api.TransactionAware;
import at.jku.isse.artifacteventstreaming.replay.StatementAugmentationSession.StatementWrapper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.ontapi.model.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.function.library.leviathan.log;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class ListResourceType implements TransactionAware {
    public static final String LIST_NS = "http://at.jku.isse.list#";
    public static final String LIST_COLLECTION_URI_PREFIX = "http://at.jku.isse.list/collection/";
    public static final String LIST_BASETYPE_URI = LIST_NS + "seq";
    public static final String OBJECT_LIST_NAME = "_liObject";
    public static final String LITERAL_LIST_NAME = "_liLiteral";
    public static final String LIST_TYPE_NAME = "_list";
    private static final String OWNED_BY_PROPERTY_URI = LIST_NS + "containerOwnerRef";
    private static final String OWNS_SUPERPROPERTY_URI = LIST_NS + "hasList";
    public static final Resource LI = ResourceFactory.createResource(RDF.uri + "li");


    @Getter // pointing back to container owner
    private final OntObjectProperty ownedByProperty;
    @Getter // pointing to container from owner
    private final OntObjectProperty ownsListSuperProperty;
    @Getter
    private final OntClass listClass;
    private final SingleResourceType singleType;
    private final BasePropertyType primaryPropertyType;

    // Caches, to be kept consistent upon syncing statement changes
    @Getter(AccessLevel.PACKAGE)
    private final Set<OntClass> subclassesCache = new HashSet<>();
    @Getter(AccessLevel.PACKAGE)
    private final Set<Property> ownershipPropertyCache = new HashSet<>();

    // transaction rollback tracking
    private final Set<OntClass> removedSubclassesDuringTx = new HashSet<>();
    private final Set<OntClass> addedSubclassesDuringTx = new HashSet<>();
    private final Set<Property> removedOwnershipPropertyDuringTx = new HashSet<>();
    private final Set<Property> addedOwnershipPropertyDuringTx = new HashSet<>();


    public ListResourceType(@NonNull OntModel model, @NonNull BasePropertyType primaryType, @NonNull SingleResourceType singleType) {
        this.singleType = singleType;
        this.primaryPropertyType = primaryType;
        listClass = model.getOntClass(RDF.Seq);
        ownedByProperty = model.getObjectProperty(OWNED_BY_PROPERTY_URI);
        ownsListSuperProperty = model.getObjectProperty(OWNS_SUPERPROPERTY_URI);
        initHierarchyCache();
    }

    private void initHierarchyCache() {
        listClass.subClasses().forEach(subclassesCache::add);
        ownsListSuperProperty.subProperties(true)
                .forEach(subProp -> ownershipPropertyCache.add(subProp.asProperty()));
    }

    public OntObjectProperty addObjectListProperty(@NonNull OntClass owningResource, @NonNull String listPropertyURI, @NonNull OntClass valueType) {
        OntModel model = owningResource.getModel();
        if (singleType.primaryPropertyType.existsPrimaryProperty(listPropertyURI)) {
            return null;  //as we cannot guarantee that the property that was identified is an OntObjectProperty
        }
        // create the specific class for this list
        OntClass listType = model.createOntClass(generateListTypeURI(listPropertyURI));
        listType.addSuperClass(listClass);
        // create the property that points to this list type // ensure we only point to one list only
        var prop = primaryPropertyType.createBaseObjectPropertyType(model, listPropertyURI, List.of(owningResource), listType);
        //var maxOneProp = singleType.getMaxOneObjectCardinalityRestriction(model, prop, listType);
        //owningResource.addProperty(RDFS.subClassOf, maxOneProp);
        //NOTE: we cannot use createSingleObject... to avoid putting this property into the single property cache as this is a list property

        // now also restrict the list content to be of valueType, and property to be a subproperty of 'li'
        var liProp = primaryPropertyType.createBaseObjectPropertyType(model, generateSpecificObjectListProperty(listPropertyURI), List.of(listType), valueType);
        liProp.addProperty(RDFS.subPropertyOf, LI);
        var restr = createAllValuesFromRestriction(model, liProp, valueType);
        // add the restriction to the list type
        listType.addProperty(RDFS.subClassOf, restr);
        ownsListSuperProperty.addSubProperty(prop);

        trackSubclassAddition(listType);
        trackOwnershipPropertyAddition(prop.asProperty());
        return prop;
    }


    public OntObjectProperty addLiteralListProperty(@NonNull OntClass owningResource, @NonNull String listPropertyURI, @NonNull OntDataRange valueType) {
        OntModel model = owningResource.getModel();
        if (primaryPropertyType.existsPrimaryProperty(listPropertyURI)) {
            return null;  //as we cannot guarantee that the property that was identified is an OntObjectProperty
        }
        // create the specific class for this list
        OntClass listType = model.createOntClass(generateListTypeURI(listPropertyURI));
        listType.addSuperClass(listClass);
        // create the property that points to this list type // ensure we only point to one list only
        var prop = primaryPropertyType.createBaseObjectPropertyType(model, listPropertyURI, List.of(owningResource), listType);
        //NOTE: we cannot use createSingleData... to avoid putting this property into the single property cache as this is a list property
        //var maxOneProp = singleType.getMaxOneObjectCardinalityRestriction(model, prop, listType);
        //owningResource.addProperty(RDFS.subClassOf, maxOneProp);

        // now also restrict the list content to be of valueType, and property to be a subproperty of 'li'
        // use base property to track known property URIs
        var liProp = primaryPropertyType.createBaseDataPropertyType(model, generateSpecificLiteralListPropertyURI(listPropertyURI), List.of(listType), valueType);
        liProp.addProperty(RDFS.subPropertyOf, LI);
        var restr = createAllValuesFromRestriction(model, liProp, valueType);
        // add the restriction to the list type
        listType.addProperty(RDFS.subClassOf, restr);
        ownsListSuperProperty.addSubProperty(prop);

        trackSubclassAddition(listType);
        trackOwnershipPropertyAddition(prop.asProperty());
        return prop;
    }

    private Resource createAllValuesFromRestriction(OntModel model, OntProperty onProperty, OntObject rangeOrClass) {
        var anonId = createValueRestrictionAnonId(onProperty);
        return model.createResource(anonId)
                .addProperty(RDF.type, OWL2.Restriction)
                .addProperty(OWL2.onProperty, onProperty)
                .addProperty(OWL2.allValuesFrom, rangeOrClass);
    }

    private AnonId createValueRestrictionAnonId(OntProperty onProperty) {
        return new AnonId("anon::" + onProperty.getURI() + "::valueRestriction");
    }

    private String generateSpecificLiteralListPropertyURI(String listPropertyURI) {
        return listPropertyURI + LITERAL_LIST_NAME;
    }

    private String generateSpecificObjectListProperty(String listPropertyURI) {
        return listPropertyURI + OBJECT_LIST_NAME;
    }

    private String generateListTypeURI(String listPropertyURI) {
        return listPropertyURI + LIST_TYPE_NAME;
    }
    private String attemptStripEnding(String uri) {
        if (uri.endsWith(OBJECT_LIST_NAME)) {
            return uri.substring(0, uri.length()-OBJECT_LIST_NAME.length());
        } else if (uri.endsWith(LITERAL_LIST_NAME)) {
            return uri.substring(0, uri.length()-LITERAL_LIST_NAME.length());
        }
        return uri;
    }

    // Remote changes syncing in (deleted/adding of property definitions )

    public void addToOwnershipPropertyCacheIfApplicable(Property prop, Set<Resource> domains) {
        String baseURI = attemptStripEnding(prop.getURI());

        // if this property matches the subtype for any of the domains, then this is a list resource property
        var optSubtype = domains.stream()
                .filter(domain -> domain.getURI().equals(generateListTypeURI(baseURI)))
                .findAny();
        if (optSubtype.isPresent()) {
            var baseProp = listClass.getModel().getProperty(baseURI);
            trackOwnershipPropertyAddition(baseProp);
            var typeRes = optSubtype.get();
            var subtypeClass = listClass.getModel().getOntClass(typeRes.getURI());
            if (subtypeClass != null) {
                trackSubclassAddition(subtypeClass);
            } else {
                log.error("Schema Corruption: resource {} is in domain of list property {} with matching expected uri but not an ontclass", typeRes.getURI(), prop.getURI());
            }
        }
    }

    /**
     * used when notified about external (i.e., synced) removal of property, hence underlying model contains no triples anymore, just cleanup cache
     * @param propertyURI
     */
    public void cleanupCacheAfterRemotePropertyRemoval(String propertyURI) {
        var listTypeURI = generateListTypeURI(propertyURI);
        subclassesCache.stream()
                .filter(ontClazz -> ontClazz.getURI().equals(listTypeURI))
                .findFirst()
                .ifPresent(this::trackSubclassRemoval);
        ownershipPropertyCache.stream()
                .filter(p -> p.getURI().equals(propertyURI))
                .findFirst()
                .ifPresent(this::trackOwnershipPropertyRemoval);
    }

    private void trackSubclassAddition(OntClass cls) {
        if (subclassesCache.add(cls)) {
            if (!removedSubclassesDuringTx.remove(cls)) {
                addedSubclassesDuringTx.add(cls);
            }
        }
    }

    private void trackSubclassRemoval(OntClass cls) {
        if (subclassesCache.remove(cls)) {
            if (!addedSubclassesDuringTx.remove(cls)) {
                removedSubclassesDuringTx.add(cls);
            }
        }
    }

    private void trackOwnershipPropertyAddition(Property prop) {
        if (ownershipPropertyCache.add(prop)) {
            if (!removedOwnershipPropertyDuringTx.remove(prop)) {
                addedOwnershipPropertyDuringTx.add(prop);
            }
        }
    }

    private void trackOwnershipPropertyRemoval(Property prop) {
        if (ownershipPropertyCache.remove(prop)) {
            if (!addedOwnershipPropertyDuringTx.remove(prop)) {
                removedOwnershipPropertyDuringTx.add(prop);
            }
        }
    }

    @Override
    public void afterTransactionStarted() {
        clearRollbackInfo();
    }

    @Override
    public void afterTransactionAborted() {
        subclassesCache.addAll(removedSubclassesDuringTx);
        subclassesCache.removeAll(addedSubclassesDuringTx);
        ownershipPropertyCache.addAll(removedOwnershipPropertyDuringTx);
        ownershipPropertyCache.removeAll(addedOwnershipPropertyDuringTx);
        clearRollbackInfo();
    }

    @Override
    public void afterTransactionCommitted() {
        clearRollbackInfo();
    }

    private void clearRollbackInfo() {
        removedSubclassesDuringTx.clear();
        addedSubclassesDuringTx.clear();
        removedOwnershipPropertyDuringTx.clear();
        addedOwnershipPropertyDuringTx.clear();
    }

    /**
     * @param listReferenceProperty OntProperty to remove from its owning class including the sequence li-subproperty
     */
    public void removeListOwnershipPropertyDefinition(@NonNull OntProperty listReferenceProperty) {
        var model = listReferenceProperty.getModel();
        // remove listType:
        var listType = model.getOntClass(generateListTypeURI(listReferenceProperty.getURI()));
        if (listType != null) {
            // remove from cache
            trackSubclassRemoval(listType);
            // remove liSubproperty predicates and any predicates from other properties that happen to be defined
            MetaModelSchemaTypes.getExplicitlyDeclaredProperties(listType).forEach(prop -> {
                if (isLiProperty(prop)) {
                    // remove restriction
                    var anonId = createValueRestrictionAnonId(prop);
                    var restrRes = model.createResource(anonId); // only way to retrieve anon resource again
                    listType.remove(RDFS.subClassOf, restrRes);
                    restrRes.removeProperties();
                    primaryPropertyType.removeBaseProperty(prop);
                } else {
                    prop.removeProperties();
                }
            });
            // remove predicates association from listType itself
            listType.removeProperties();
        }
        // remove list reference property
        trackOwnershipPropertyRemoval(listReferenceProperty.asProperty());
        primaryPropertyType.removeBaseProperty(listReferenceProperty);
    }


    private static boolean isLiProperty(Resource prop) {
        StmtIterator iter = prop.listProperties(RDFS.subPropertyOf);
        while (iter.hasNext()) {
            if (iter.next().getResource().equals(LI)) {
                return true;
            }
        }
        return false;
    }

    public boolean isOwnsListProperty(@NonNull OntProperty prop) {
        return ownershipPropertyCache.contains(prop.asProperty())
                || ownsListSuperProperty.subProperties(true).anyMatch(subProp -> subProp.equals(prop));
    }



    // Instance-level checks ================================================

    public boolean isListCollection(Resource ontInd) {
        //Too slow: return ontInd.classes(true).anyMatch(subclassesCache::contains);
        // we dont check for type: || type.equals(listClass) as we are interest only which are not just list but also have the backlink to owner
        // as we control list collection uri gereration, take a shortcut:
        var uri = ontInd.getURI();
        return uri != null && uri.startsWith(LIST_COLLECTION_URI_PREFIX);
    }

//    public boolean wasListCollection(List<Resource> delTypes) {
//        too slow, see above: return delTypes.stream().anyMatch(type -> type.getURI().equals(getListClass().getURI()) ||
//                        subclassesCache.stream().map(RDFNode::asResource).anyMatch(clazz -> clazz.equals(type)));

//    }

    public boolean wasListCollectionBasedOnURI(Resource res) {
        return isListCollection(res);
    }

    /**
     * @param owner                 the container of the list resource
     * @param listReferenceProperty the property pointing to the list resource from the owner,
     *                              if no list resource exists or the resource is not rdf:Seq, removes all properties and creates new list resource
     * @return list resource, typed to the range of the listReferenceProperty (if range has a owl2:Class referenced)
     */
    public Seq getOrCreateSequenceFor(OntObject owner, OntRelationalProperty listReferenceProperty) {
        var seq = owner.getPropertyResourceValue(listReferenceProperty.asProperty());
        if (seq == null || !seq.canAs(Seq.class)) {
            owner.removeAll(listReferenceProperty.asProperty());
            var ownerId = owner.isAnon() ? owner.getId().toString() : owner.getURI();
            var fragmentPart = primaryPropertyType.hashAsIdPart(ownerId, listReferenceProperty.getURI());
            seq = owner.getModel().createSeq(LIST_COLLECTION_URI_PREFIX + listReferenceProperty.getLocalName() + "#" + fragmentPart);
            owner.addProperty(listReferenceProperty.asProperty(), seq);
            seq.addProperty(getOwnedByProperty().asProperty(), owner);

            // if available, add the list's specific subtype
            var optRange = listReferenceProperty.ranges().findAny();
            if (optRange.isPresent()) {
                seq.addProperty(RDF.type, optRange.get());
            }
        }
        return seq.as(Seq.class);
    }

    public List<Property> findListReferencePropertiesBetween(Resource subject, Resource object) {
        List<Property> props = new ArrayList<>();
        var iter = subject.getModel().listStatements(subject, null, object);
        while (iter.hasNext()) {
            props.add(iter.next().getPredicate());
        }
        if (props.size() > 1) {
            props.remove(ownsListSuperProperty.asProperty());
        }
        return props;
    }

    public Optional<Resource> getCurrentListOwner(Resource list) {
        return Optional.ofNullable(list.getPropertyResourceValue(getOwnedByProperty().asProperty()));
    }

    public Optional<Resource> getFormerListOwner(List<StatementWrapper> stmts) {
        return stmts.stream().filter(wrapper -> wrapper.op().equals(AES.OPTYPE.REMOVE))
                .map(StatementWrapper::stmt)
                .filter(stmt -> stmt.getPredicate().equals(getOwnedByProperty().asProperty()))
                .map(Statement::getResource)
                .findAny();
    }


    protected static class ListSchemaFactory {

        private final OntModel model;

        public ListSchemaFactory(OntModel metaOntology) {
            this.model = metaOntology;
            initTypes();

        }

        private void initTypes() {
            var listClass = model.getOntClass(RDF.Seq);
            if (listClass == null) {
                listClass = model.createOntClass(RDF.Seq.getURI());
            }

            var containerProperty = model.getObjectProperty(OWNED_BY_PROPERTY_URI);
            if (containerProperty == null) {
                containerProperty = model.createObjectProperty(OWNED_BY_PROPERTY_URI);
                containerProperty.addDomain(listClass);
            }

            var listReferenceSuperProperty = model.getObjectProperty(OWNS_SUPERPROPERTY_URI);
            if (listReferenceSuperProperty == null) {
                listReferenceSuperProperty = model.createObjectProperty(OWNS_SUPERPROPERTY_URI);
                listReferenceSuperProperty.addRange(listClass);
            }
        }

    }
}

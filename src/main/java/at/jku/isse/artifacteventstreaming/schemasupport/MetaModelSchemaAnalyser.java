package at.jku.isse.artifacteventstreaming.schemasupport;

import lombok.NonNull;
import org.apache.jena.ontapi.model.OntClass;
import org.apache.jena.rdf.model.Property;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class MetaModelSchemaAnalyser {

    /**
     *
     * @param primarySchema
     * @param childSchema
     * @return null if no mismatch error otherwise String containing  all mismatches
     */
    public static String areMetaModelsEqualOrError(
            @NonNull MetaModelSchemaTypes primarySchema
            , @NonNull MetaModelSchemaTypes childSchema) {
        return areSnapShotsEqualOrError(captureCache(primarySchema), captureCache(childSchema));
    }

    public record CacheSnapshot(
            Set<String> primaryPropertyURIs,
            Set<String> functionalPropertyURIs,
            Set<String> listOwnershipURIs,
            Set<String> mapOwnsURIs,
            Set<String> listSubclassURIs,
            Set<String> mapSubclassURIs
    ) {}

    public static CacheSnapshot captureCache(@NonNull MetaModelSchemaTypes schema) {
        var primaryURIs = schema.getPrimaryPropertyType().getKnownPropertyURIs();
        return new CacheSnapshot(
                primaryURIs,
                primaryURIs.stream()
                        .filter(uri -> schema.getSingleType().isSingleProperty(uri))
                        .collect(Collectors.toCollection(HashSet::new)),
                schema.getListType().getOwnershipPropertyCache().stream()
                        .map(Property::getURI).collect(Collectors.toCollection(HashSet::new)),
                schema.getMapType().getOwnsPropertyCache().stream()
                        .map(Property::getURI).collect(Collectors.toCollection(HashSet::new)),
                schema.getListType().getSubclassesCache().stream()
                        .map(OntClass::getURI).collect(Collectors.toCollection(HashSet::new)),
                schema.getMapType().getSubclassesCache().stream()
                        .map(OntClass::getURI).collect(Collectors.toCollection(HashSet::new))
        );
    }

    /**
     * @param expected
     * @param actual
     * @return null if no mismatch error otherwise String containing  all mismatches
     */
    public static String areSnapShotsEqualOrError(@NonNull CacheSnapshot expected, @NonNull CacheSnapshot actual) {
        var sb = new StringBuilder();
        if (!expected.primaryPropertyURIs().equals(actual.primaryPropertyURIs()))
            sb.append("Primary property cache mismatch \n");
        if (!expected.functionalPropertyURIs().equals(actual.functionalPropertyURIs()))
            sb.append("Functional property cache mismatch \n");
        if (!expected.listOwnershipURIs().equals(actual.listOwnershipURIs()))
            sb.append("ist ownership cache mismatch \n");
        if (!expected.mapOwnsURIs().equals(actual.mapOwnsURIs()))
            sb.append("Map ownership cache mismatch \n");
        if (!expected.listSubclassURIs().equals(actual.listSubclassURIs()))
            sb.append("List subclass cache mismatch \n");
        if (!expected.mapSubclassURIs().equals(actual.mapSubclassURIs()))
            sb.append("Map subclass cache mismatch \n");
        var error = sb.toString();
        if (error.isEmpty())
            return null;
        else
            return error;
    }

    /**
     *
     * @param first
     * @param second
     * @return snapshot that contains only the entries that are present in first but not in second, i.e. the "diff" of first to second
     */
    public static CacheSnapshot generateDiffOfFirst(@NonNull MetaModelSchemaTypes first, @NonNull MetaModelSchemaTypes second) {
        var firstCache = captureCache(first);
        var secondCache =  captureCache(second);
        firstCache.primaryPropertyURIs.removeAll(secondCache.primaryPropertyURIs);
        firstCache.functionalPropertyURIs.removeAll(secondCache.functionalPropertyURIs);
        firstCache.listOwnershipURIs.removeAll(secondCache.listOwnershipURIs);
        firstCache.mapOwnsURIs.removeAll(secondCache.mapOwnsURIs);
        firstCache.listSubclassURIs.removeAll(secondCache.listSubclassURIs);
        firstCache.mapSubclassURIs.removeAll(secondCache.mapSubclassURIs);
        return firstCache;
    }
}

package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class MetaModelSchemaCacheSynchronizer {

    private final MetaModelSchemaTypes metaschema;
    private final String branchId;

    public void handleCommit(Commit commit) {
        // collect all statements that have to do with property spec removal

        Map<String, Set<ContainedStatement>> propDefRemovedStatements = collectPropertySpecStatements(commit.getRemovedStatements());
        propDefRemovedStatements.entrySet().forEach(entry -> metaschema.cleanupCachesAfterRemotePropertyRemoval(entry.getKey(), entry.getValue()));

        Map<String, Set<ContainedStatement>> propDefAddedStatements = collectPropertySpecStatements(commit.getAddedStatements());
        propDefAddedStatements.entrySet().forEach(entry -> {
            var model = entry.getValue().iterator().next().getSubject().getModel();
            metaschema.syncCachesAfterRemotePropertyAdded(entry.getKey() , model, entry.getValue());
        });
    }

    private @org.jspecify.annotations.NonNull Map<String, Set<ContainedStatement>> collectPropertySpecStatements(Collection<ContainedStatement> stmts) {
        return stmts.stream()
                .filter(this::isPropertySpecification)
                .collect(Collectors.toMap(
                        stmt -> stmt.getSubject().getURI(),
                        Set::of,
                        (set1, set2) -> {
                            Set<ContainedStatement> merged = new HashSet<>(set1);
                            merged.addAll(set2);
                            return merged;
                        }
                ));
    }

    private boolean isPropertySpecification(@NonNull ContainedStatement stmt) {
        var predicate = stmt.getPredicate();
        return  predicate.equals(RDFS.domain)
                || predicate.equals(RDFS.range)
                || predicate.equals(RDFS.subPropertyOf)
                || isPropertyDefinition(stmt);

    }

    public static boolean isPropertyDefinition(ContainedStatement stmt) {
        String uri = stmt.getObject().isResource() ? stmt.getResource().getURI() : null;
        return stmt.getPredicate().equals(RDF.type)
                && uri != null
                && ( uri.equals(RDF.Nodes.Property.getURI())
                || uri.equals(OWL2.ObjectProperty.getURI())
                || uri.equals(OWL2.DatatypeProperty.getURI()));
    }
}

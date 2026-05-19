package at.jku.isse.artifacteventstreaming.schemasupport;

import at.jku.isse.artifacteventstreaming.api.Commit;
import at.jku.isse.artifacteventstreaming.api.ContainedStatement;
import at.jku.isse.artifacteventstreaming.branch.incoming.PropertyDefinitionAddedCacheUpdater;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.OWL2;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class MetaModelSchemaCacheSynchronizer {

    private final MetaModelSchemaTypes metaschema;
    private final String branchId;

    public void handleCommit(Commit commit) {
        commit.getRemovedStatements().stream()
                .filter(PropertyDefinitionAddedCacheUpdater::isPropertyDefinition)
                .map(stmt -> stmt.getSubject().getURI())
                .filter(uri -> metaschema.getPrimaryPropertyType().getKnownPropertyURIs().contains(uri))
                .forEach(uri -> handleRemovedProperty(uri, commit));


        Set<ContainedStatement> domainAddedStmts = new HashSet<>();
        Set<ContainedStatement> propertiesAddedStmts = new HashSet<>();

        commit.getAddedStatements().stream()
                .forEach(stmt -> {
                    // if property has domain stmt
                    if (isPropertyOntClassDomainStatement(stmt)) {
                        domainAddedStmts.add(stmt);
                    } else if (isPropertyDefinition(stmt)) {
                        propertiesAddedStmts.add(stmt);
                    }
                });

        propertiesAddedStmts.stream()
                .filter(stmt -> !metaschema.getPrimaryPropertyType().getKnownPropertyURIs().contains(stmt.getSubject().getURI()))
                // only for new unknown properties
                .forEach(stmt -> {
                    var propertyURI = stmt.getSubject().getURI();
                    log.debug("Handling added property {} from commit {} applied to branch {} ", propertyURI, commit.getCommitId(), branchId);
                    var domains = domainsForProperty(propertyURI, domainAddedStmts);
                    metaschema.syncCachesAfterRemotePropertyAdded(propertyURI, stmt.getSubject().getModel(), domains);
                });
    }

    private void handleRemovedProperty(String propertyURI, Commit commit) {
        log.debug(String.format("Handling removed property %s from commit %s applied to branch %s ", propertyURI, commit.getCommitId(), branchId));
        metaschema.cleanupCachesAfterRemotePropertyRemoval(propertyURI);
    }


    private boolean isPropertyOntClassDomainStatement(ContainedStatement stmt) {
        return stmt.getObject().isResource()
                && stmt.getPredicate().equals(RDFS.domain)
                && stmt.getResource().getURI() != null;
    }

    public static boolean isPropertyDefinition(ContainedStatement stmt) {
        String uri = stmt.getObject().isResource() ? stmt.getResource().getURI() : null;
        return stmt.getPredicate().equals(RDF.type)
                && uri != null
                && ( uri.equals(RDF.Nodes.Property.getURI())
                || uri.equals(OWL2.ObjectProperty.getURI())
                || uri.equals(OWL2.DatatypeProperty.getURI()));
    }

    private Set<Resource> domainsForProperty(String propertyURI, Set<ContainedStatement> domainStatements) {
        return domainStatements.stream()
                .filter(stmt -> stmt.getSubject().getURI().equals(propertyURI))
                .map(Statement::getResource)
                .collect(Collectors.toSet());
    }


}

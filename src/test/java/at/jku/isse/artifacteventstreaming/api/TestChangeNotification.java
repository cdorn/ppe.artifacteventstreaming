package at.jku.isse.artifacteventstreaming.api;

import at.jku.isse.artifacteventstreaming.branch.BranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.BranchImpl;
import at.jku.isse.artifacteventstreaming.branch.CoreBranchBuilder;
import at.jku.isse.artifacteventstreaming.branch.CoreBranchImpl;
import io.micrometer.observation.ObservationRegistry;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.tdb2.TDB2Factory;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestChangeNotification {

    public static URI repoURI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/repo2");

    OntModel model;
    CoreBranch branch;

    @BeforeEach
    void setup() {
        Dataset repoDataset = DatasetFactory.createTxnMem();
        branch = new CoreBranchBuilder(repoURI, repoDataset, ObservationRegistry.NOOP)
                .setDataset(TDB2Factory.createDataset())
                .build();
        model = branch.getModel();
    }

    @Test
    void testIndividualTrueChanges() throws Exception {
        var lock = branch.startWriteTransaction();
        Resource testResource = model.createResource(repoURI+"#art1");
        model.add(testResource, RDFS.label, model.createTypedLiteral(1));
        model.remove(testResource, RDFS.label, model.createTypedLiteral(2)); // this should not result in an event, as there is no change to the model
        model.remove(testResource, RDFS.label, model.createTypedLiteral(2));
        Commit commit = branch.commitChanges("TestCommit");
        branch.completeTransaction(lock);

        branch.startReadTransaction();
        RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
        assertEquals(1, model.size());
        assertEquals(1, commit.getAddedStatements().size());
        assertEquals(0, commit.getRemovedStatements().size());
        branch.completeTransaction(null);

        lock = branch.startWriteTransaction();
        model.add(testResource, RDFS.label, model.createTypedLiteral(1)); // this should not result in an event, as there is no change to the model
        Commit commit2 = branch.commitChanges("TestCommit2");
        branch.completeTransaction(lock);

        branch.startReadTransaction();
        RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
        assertEquals(1, model.size());
        assertNull(commit2);
    }

    @Test
    void testNoFalseRemoveChanges() throws Exception {
        var lock = branch.startWriteTransaction();
        Resource testResource = model.createResource(repoURI+"#art1");
        var stmt1 = new StatementImpl(testResource, RDFS.label, model.createTypedLiteral(1));
        var stmt2 = new StatementImpl(testResource, RDFS.label, model.createTypedLiteral(2));
        var stmt3 = new StatementImpl(testResource, RDFS.label, model.createTypedLiteral(3));
        var stmt4 = new StatementImpl(testResource, RDFS.label, model.createTypedLiteral(4));
        var stmt5 = new StatementImpl(testResource, RDFS.label, model.createTypedLiteral(5));

        model.add(List.of(stmt1, stmt2, stmt3));
        model.remove(List.of(stmt4, stmt5)); // this should not result in an event, as there is no change to the model

        Commit commit = branch.commitChanges("TestCommit");
        branch.completeTransaction(lock);

        branch.startReadTransaction();
        RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
        assertEquals(3, model.size());
        assertEquals(3, commit.getAddedStatements().size());
        assertEquals(0, commit.getRemovedStatements().size());
        branch.completeTransaction(null);

    }

    @Test
    void testNoFalseAdditionChanges() throws Exception {
        var lock = branch.startWriteTransaction();
        Resource testResource = model.createResource(repoURI+"#art1");
        var stmt1 = new StatementImpl(testResource, RDFS.label, model.createTypedLiteral(1));
        var stmt2 = new StatementImpl(testResource, RDFS.label, model.createTypedLiteral(2));
        var stmt3 = new StatementImpl(testResource, RDFS.label, model.createTypedLiteral(3));

        model.add(List.of(stmt1, stmt2, stmt3));
        Commit commit = branch.commitChanges("TestCommit");
        branch.completeTransaction(lock);

        branch.startReadTransaction();
        RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
        assertEquals(3, model.size());
        assertEquals(3, commit.getAddedStatements().size());
        assertEquals(0, commit.getRemovedStatements().size());
        branch.completeTransaction(null);

        lock = branch.startWriteTransaction();
        model.add(List.of(stmt1, stmt2, stmt3)); // this should not result in an event, as there is no change to the model
        Commit commit2 = branch.commitChanges("TestCommit2");
        branch.completeTransaction(lock);

        branch.startReadTransaction();
        RDFDataMgr.write(System.out, model, Lang.TURTLE) ;
        assertEquals(3, model.size());
        assertNull(commit2);
    }
}

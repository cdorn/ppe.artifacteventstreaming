package at.jku.isse.artifacteventstreaming.branch;

import at.jku.isse.artifacteventstreaming.api.CoreBranch;
import io.micrometer.observation.ObservationRegistry;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.Lock;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.RepeatedTest;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * startWriteTransaction() used to drain the shared stmtAggregator and notify commit handlers
 * before ever taking the write lock. A second thread racing in on startWriteTransaction()
 * (e.g. a merge consumer) could therefore wipe out another, still-open transaction's buffered
 * additions/removals before that transaction got a chance to commit them.
 */
class TestStartWriteTransactionRace {

    static final URI REPO_URI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/racestart-test");
    static final String ART1_URI = REPO_URI + "#art1";

    private CoreBranch buildBranch() {
        return new CoreBranchBuilder(REPO_URI, DatasetFactory.createTxnMem(), ObservationRegistry.NOOP).build();
    }

    @RepeatedTest(20)
    void concurrentStartWriteTransactionMustNotDropOtherThreadsPendingStatements() throws Exception {
        CoreBranch branch = buildBranch();
        OntModel model = branch.getModel();
        Resource art1 = model.createResource(ART1_URI);

        Lock localLock = branch.startWriteTransaction();
        model.add(art1, RDFS.label, model.createTypedLiteral("local"));

        // simulates a concurrent merge thread trying to start its own write transaction
        // while the local transaction above is still open and not yet committed
        CountDownLatch contenderDone = new CountDownLatch(1);
        AtomicReference<Exception> contenderError = new AtomicReference<>();
        Thread contender = new Thread(() -> {
            try {
                Lock contenderLock = branch.startWriteTransaction();
                branch.abortWriteTransaction();
                branch.completeTransaction(contenderLock);
            } catch (Exception e) {
                contenderError.set(e);
            } finally {
                contenderDone.countDown();
            }
        });
        contender.start();

        // give the contender thread a chance to run its pre-lock drain while we still hold the lock
        Thread.sleep(50);

        var commit = branch.commitChanges("local change");
        branch.completeTransaction(localLock);

        assertTrue(contenderDone.await(5, TimeUnit.SECONDS), "contender thread did not finish in time");
        assertNull(contenderError.get(), "contender thread threw: " + contenderError.get());

        assertNotNull(commit,
                "the local thread's addition must not be silently dropped by a concurrent startWriteTransaction()");
    }
}

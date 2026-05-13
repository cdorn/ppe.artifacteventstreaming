package at.jku.isse.artifacteventstreaming.branch;

import at.jku.isse.artifacteventstreaming.api.CoreBranch;
import io.micrometer.observation.ObservationRegistry;
import org.apache.jena.ontapi.model.OntModel;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.Lock;
import org.apache.jena.vocabulary.RDFS;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class TestPromoteToWriteTransaction {

    static final URI REPO_URI = URI.create("http://at.jku.isse.artifacteventstreaming/testrepos/promote-test");
    static final String ART1_URI = REPO_URI + "#art1";

    private CoreBranch buildBranch() {
        return new CoreBranchBuilder(REPO_URI, DatasetFactory.createTxnMem(), ObservationRegistry.NOOP).build();
    }

    /**
     * Calling promote when no transaction is active must return null (nothing to promote).
     */
    @Test
    void promoteReturnsNullWhenNotInTransaction() {
        CoreBranch branch = buildBranch();
        assertNull(branch.promoteToWriteTransaction());
    }

    /**
     * Calling promote when already in a write transaction must return null (already promoted).
     */
    @Test
    void promoteReturnsNullWhenAlreadyInWriteTransaction() {
        CoreBranch branch = buildBranch();
        Lock writeLock = branch.startWriteTransaction();
        try {
            assertNull(branch.promoteToWriteTransaction());
        } finally {
            branch.abortWriteTransaction();
            branch.completeTransaction(writeLock);
        }
    }

    /**
     * A READ_PROMOTE transaction can be promoted in-place when no other writer has committed
     * since the read transaction started. A concurrent reader must not block this.
     */
    @Test
    void promoteSucceedsInPlaceWhenNoConcurrentWrite() throws Exception {
        CoreBranch branch = buildBranch();

        // Main thread opens a READ_PROMOTE transaction
        branch.startReadTransaction();

        // A concurrent reader runs (and finishes) — reads don't block promotion
        CountDownLatch readerDone = new CountDownLatch(1);
        Thread reader = new Thread(() -> {
            branch.startReadTransaction();
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            branch.completeTransaction(null);
            readerDone.countDown();
        });
        reader.start();
        assertTrue(readerDone.await(5, TimeUnit.SECONDS), "Concurrent reader did not finish in time");

        // No write was committed, so dataset.promote() succeeds and the same transaction is reused
        Lock lock = branch.promoteToWriteTransaction();
        try {
            assertNotNull(lock, "Lock must be returned on successful in-place promotion");
            assertEquals(ReadWrite.WRITE, branch.getDataset().transactionMode(),
                    "Dataset must be in WRITE mode after promotion");
        } finally {
            branch.abortWriteTransaction();
            branch.completeTransaction(lock);
        }
    }

    /**
     * When another thread has committed a write after the current thread started its read
     * transaction, dataset.promote() returns false. promoteToWriteTransaction() must then
     * end the stale read transaction and open a fresh WRITE transaction that sees the
     * concurrent writer's committed data.
     */
    @Test
    void promoteStartsFreshWriteTransactionAfterConcurrentWrite() throws Exception {
        CoreBranch branch = buildBranch();
        //branch.startCommitHandlers(); // required by concludeTransaction -> commitChanges
        OntModel model = branch.getModel();
        Resource art1 = model.createResource(ART1_URI);

        // Main thread opens a READ_PROMOTE transaction — art1 has no label at this point
        branch.startReadTransaction();

        // Background writer commits a change while the read transaction is still open
        CountDownLatch writerDone = new CountDownLatch(1);
        AtomicReference<Exception> writerError = new AtomicReference<>();
        Thread writer = new Thread(() -> {
            try {
                Lock writeLock = branch.startWriteTransaction();
                model.add(art1, RDFS.label, model.createTypedLiteral(42));
                branch.commitChanges("concurrent write");
                branch.completeTransaction(writeLock);
            } catch (Exception e) {
                writerError.set(e);
            } finally {
                writerDone.countDown();
            }
        });
        writer.start();
        assertTrue(writerDone.await(5, TimeUnit.SECONDS), "Writer thread did not complete in time");
        assertNull(writerError.get(), "Writer thread threw an exception: " + writerError.get());

        // At this point a write was committed after the read transaction started.
        // dataset.promote() will return false, so promoteToWriteTransaction() must
        // end the stale read and open a new WRITE transaction.
        Lock promotedLock = branch.promoteToWriteTransaction();
        try {
            assertNotNull(promotedLock,
                    "A lock must still be returned even when a fresh write transaction is needed");
            assertEquals(ReadWrite.WRITE, branch.getDataset().transactionMode(),
                    "Dataset must be in WRITE mode after falling back to a fresh write transaction");

            // The fresh WRITE transaction starts from the latest committed snapshot,
            // so the writer's committed data must now be visible
            assertTrue(art1.hasProperty(RDFS.label),
                    "Fresh write transaction must see the property committed by the concurrent writer");
            assertEquals(42, art1.getProperty(RDFS.label).getInt(),
                    "Fresh write transaction must see the value committed by the concurrent writer");
        } finally {
            branch.abortWriteTransaction();
            branch.completeTransaction(promotedLock);
        }
    }
}

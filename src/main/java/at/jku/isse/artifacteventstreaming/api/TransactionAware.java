package at.jku.isse.artifacteventstreaming.api;

public interface TransactionAware {
    /**
     * called before a new write transaction is started, and no write lock obtained yet
     * dataset is not in a transaction yet, so any reads/writes will lead to an exception.
     * Note: will also be called before a promote (but not if tx is already a write transaction)
     */
    default void beforeTransactionStarted() {}

    /**
     * called just after a new write transaction is started, (and a lock is obtained)
     * the client starting the transaction has not obtained the lock yet at this stage
     * Note: will also be called after a successful promote (but not if tx is already a write transaction)
     */
    default void afterTransactionStarted() {}

    /**
     * transaction still active, so reads can still occur,
     * any writes make no sense as any changes will be reverted upon abort
     * Note: a commit without changes also leads to a transaction abort
     */
    default void beforeTransactionAborted() {}

    /**
     * transaction no longer active, any writes have been reverted,
     * any new reads or writes will lead to an exception
     * Note: a commit without changes also leads to a transaction abort
     */
    default void afterTransactionAborted() {}

    /**
     * before commit services are called, write transaction still open and available to writes
     */
    default void beforeTransactionCommitted() {}

    /**
     * after changes have been successfully commited, the write lock is still held,
     * but no reads/writes possible as transaction is closed, so any reads/writes will lead to an exception
     */
    default void afterTransactionCommitted() {}
}

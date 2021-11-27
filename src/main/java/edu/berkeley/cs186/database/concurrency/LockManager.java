package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            for(Lock lock : locks) {
                if(lock.transactionNum != except && !LockType.compatible(lockType, lock.lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            locks.removeIf(curLock -> curLock.transactionNum.equals(lock.transactionNum));
            locks.add(lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            locks.remove(lock);
            waitingQueue.removeIf(lockRequest -> lockRequest.lock.equals(lock));
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) {
                waitingQueue.addFirst(request);
            }
            else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();
            while (requests.hasNext()) {
                LockRequest request = requests.next();
                for (Lock lock : locks) {
                    if (!LockType.compatible(lock.lockType, request.lock.lockType)
                            && (lock.transactionNum != request.transaction.getTransNum())) {
                        return;
                    }
                }
                waitingQueue.pollFirst();
                locks.removeAll(request.releasedLocks);
                locks.add(request.lock);
                if (transactionLocks.containsKey(request.transaction.getTransNum())) {
                    transactionLocks.get(request.transaction.getTransNum()).add(request.lock);
                } else {
                    List<Lock> lockList = new ArrayList<>();
                    lockList.add(request.lock);
                    transactionLocks.put(request.transaction.getTransNum(), lockList);
                }
                request.transaction.unblock();
            }
        }

        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            for (Lock lock: locks) {
                if (lock.transactionNum.equals(transaction)) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        /**
         * Gets the lock `transaction` has on this resource.
         */
        public Lock getTransactionLock(long transaction) {
            for (Lock lock: locks) {
                if (lock.transactionNum.equals(transaction)) {
                    return lock;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        if (releaseNames.isEmpty()) {
            acquire(transaction, name, lockType);
            return;
        }
        boolean shouldBlock = false;
        synchronized (this) {
            LockType curLockType = getLockType(transaction, name);
            if (curLockType != LockType.NL && !releaseNames.contains(name)) {
                throw new DuplicateLockRequestException("A lock is already held by this transaction and isn't being released");
            }
            List<Lock> locks = getLocks(transaction);
            Set<ResourceName> releaseNamesSet = new HashSet<>(releaseNames);
            if(locks != null && !locks.isEmpty()) {
                // if held locks doesn't include release locks
                for(Lock heldLock : locks) {
                    for(ResourceName resourceName : releaseNames) {
                        if(heldLock.name.equals(resourceName)) {
                            releaseNamesSet.remove(resourceName);
                        }
                    }
                }
                if(!releaseNamesSet.isEmpty()) {
                    throw new NoLockHeldException(String.format("transaction %d does not hold any lock but it is going to release lock",
                            transaction.getTransNum()));
                }
            }

            ResourceEntry entry = getResourceEntry(name);
            // not compatible or waitingQueue not empty
            if (!entry.checkCompatible(lockType, transaction.getTransNum()) || !entry.waitingQueue.isEmpty()) {
                shouldBlock = true;
            }
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            if (shouldBlock) {
                for (ResourceName resourceName : releaseNames) {
                    ResourceEntry curEntry = getResourceEntry(resourceName);
                    Lock curLock = curEntry.getTransactionLock(transaction.getTransNum());
                    LockRequest lockRequest;
                    if (name.equals(resourceName)) {
                        lockRequest = new LockRequest(transaction, lock, Collections.singletonList(curLock));
                    }
                    else {
                        lockRequest = new LockRequest(transaction, null, Collections.singletonList(curLock));
                    }
                    curEntry.addToQueue(lockRequest, true);
                }
                transaction.prepareBlock();
            } else {
                for (ResourceName resourceName : releaseNames) {
                    ResourceEntry resourceEntry = getResourceEntry(resourceName);
                    Lock lock1 = resourceEntry.getTransactionLock(transaction.getTransNum());
                    resourceEntry.releaseLock(lock1);
                    resourceEntry.processQueue();
                    transactionLocks.get(transaction.getTransNum()).remove(lock);
                }
                entry.grantOrUpdateLock(lock);
                resourceEntries.put(name, entry);
                List<Lock> newLocks = new ArrayList<>();
                newLocks.add(lock);
                transactionLocks.put(transaction.getTransNum(), newLocks);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            if (getLockType(transaction, name) != LockType.NL) {
                throw new DuplicateLockRequestException("The transaction already holds a lock on this resource");
            }
            ResourceEntry entry = getResourceEntry(name);
            // not compatible or waitingQueue not empty
            if (!entry.checkCompatible(lockType, transaction.getTransNum()) || !entry.waitingQueue.isEmpty()) {
                shouldBlock = true;
            }
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            if (shouldBlock) {
                LockRequest lockRequest = new LockRequest(transaction, lock);
                entry.addToQueue(lockRequest, false);
                transaction.prepareBlock();
            } else {
                entry.grantOrUpdateLock(lock);
                resourceEntries.put(name, entry);
                if (transactionLocks.containsKey(transaction.getTransNum())) {
                    transactionLocks.get(transaction.getTransNum()).add(lock);
                } else {
                    List<Lock> locks = new ArrayList<>();
                    locks.add(lock);
                    transactionLocks.put(transaction.getTransNum(), locks);
                }
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // You may modify any part of this method.
        synchronized (this) {
            Lock lock = getLock(transaction, name);
            if (lock == null) {
                throw new NoLockHeldException("No lock on this resource is hold by the transaction");
            }
            ResourceEntry resourceEntry = getResourceEntry(name);
            resourceEntry.releaseLock(lock);
            resourceEntry.processQueue();
            transactionLocks.get(transaction.getTransNum()).remove(lock);
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            Lock oldLock = getLock(transaction, name);
            if (oldLock == null) {
                throw new NoLockHeldException("No lock on this resource is hold by the transaction");
            }
            LockType oldLockType = oldLock.lockType;
            if (oldLockType == newLockType) {
                throw new DuplicateLockRequestException("The transaction already holds a lock on this resource");
            }
            if (!LockType.substitutable(newLockType, oldLockType)) {
                throw new InvalidLockException("The requested lock type is not a promotion");
            }
            ResourceEntry entry = getResourceEntry(name);
            // not compatible or waitingQueue not empty
            if (!entry.checkCompatible(newLockType, transaction.getTransNum()) || !entry.waitingQueue.isEmpty()) {
                shouldBlock = true;
            }
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
            if (shouldBlock) {
                LockRequest lockRequest = new LockRequest(transaction, newLock, Collections.singletonList(oldLock));
                entry.addToQueue(lockRequest, true);
                transaction.prepareBlock();
            } else {
                entry.grantOrUpdateLock(newLock);
                resourceEntries.put(name, entry);
                transactionLocks.get(transaction.getTransNum()).remove(oldLock);
                transactionLocks.get(transaction.getTransNum()).add(newLock);
            }
        }
        if (shouldBlock) {
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        List<Lock>lockList = getLocks(name);
        for (Lock lock : lockList) {
            if (lock.transactionNum.equals(transaction.getTransNum())) {
                return lock.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized Lock getLock(TransactionContext transaction, ResourceName name) {
        List<Lock>lockList = getLocks(name);
        for (Lock lock : lockList) {
            if (lock.transactionNum.equals(transaction.getTransNum())) {
                return lock;
            }
        }
        return null;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}

package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }
        if (requestType == LockType.S) {
            ensureLocksOnAncestors(transaction, parentContext, LockType.S);
            switch (explicitLockType) {
                case IX:
                    lockContext.promote(transaction, LockType.SIX);
                    break;
                case NL:
                    lockContext.acquire(transaction, LockType.S);
                    break;
                case IS:
                    lockContext.escalate(transaction);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + explicitLockType);
            }
        }
        else if (requestType == LockType.X) {
            ensureLocksOnAncestors(transaction, parentContext, LockType.X);
            switch (explicitLockType) {
                case IX:
                    lockContext.escalate(transaction);
                    break;
                case NL:
                    lockContext.acquire(transaction, LockType.X);
                    break;
                case IS:
                    lockContext.escalate(transaction);
                    lockContext.promote(transaction, LockType.X);
                    break;
                case S:
                    lockContext.promote(transaction, LockType.X);
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + explicitLockType);
            }
        }
    }

    private static void ensureLocksOnAncestors(TransactionContext transaction, LockContext lockContext, LockType requestType) {
        LockContext curContext = lockContext;
        ArrayList<LockContext> lockContexts = new ArrayList<>();
        while (curContext != null) {
            lockContexts.add(curContext);
            curContext = curContext.parent;
        }
        Collections.reverse(lockContexts);
        for (LockContext context : lockContexts) {
            if (requestType == LockType.S) {
                if (context.getExplicitLockType(transaction) == LockType.NL) {
                    context.acquire(transaction, LockType.IS);
                }
            }
            else if (requestType == LockType.X) {
                LockType explicitLockType = context.getExplicitLockType(transaction);
                switch (explicitLockType) {
                    case NL:
                        context.acquire(transaction, LockType.IX);
                        break;
                    case S:
                        context.promote(transaction, LockType.SIX);
                        break;
                    case IS:
                        context.promote(transaction, LockType.IX);
                        break;
                }
            }
        }
    }
}

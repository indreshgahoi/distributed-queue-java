package io.github.indreshgahoi.queue.node.application.service;

import io.github.indreshgahoi.queue.node.application.port.out.RuntimeQueue;
import io.github.indreshgahoi.queue.node.domain.model.RuntimePartitionIdentity;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Owns admission and closure ordering for one runtime partition.
 *
 * <p>The lock is held only while changing lifecycle state or the admission
 * count. Queue work executes through a permit after the lock is released, so
 * slow storage for one partition cannot hold a node-wide lifecycle monitor.
 */
final class RuntimePartitionHandle implements AutoCloseable {
    private final RuntimePartitionIdentity identity;
    private final RuntimeQueue queue;
    private final ReentrantLock lifecycleLock = new ReentrantLock();
    private final Condition lifecycleChanged = lifecycleLock.newCondition();
    private State state = State.READY;
    private int activeOperations;
    private boolean closeInProgress;

    RuntimePartitionHandle(
            RuntimePartitionIdentity identity,
            RuntimeQueue queue
    ) {
        this.identity = Objects.requireNonNull(identity, "identity");
        this.queue = Objects.requireNonNull(queue, "queue");
    }

    RuntimePartitionIdentity identity() {
        return identity;
    }

    Optional<Admission> tryAcquire() {
        lifecycleLock.lock();
        try {
            if (state != State.READY) {
                return Optional.empty();
            }
            activeOperations++;
            return Optional.of(new Admission(this));
        } finally {
            lifecycleLock.unlock();
        }
    }

    void beginClosing() {
        lifecycleLock.lock();
        try {
            if (state == State.READY) {
                state = State.CLOSING;
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Override
    public void close() {
        if (!claimCloseAndAwaitDrain()) {
            return;
        }

        RuntimeException closeFailure = null;
        try {
            queue.close();
        } catch (RuntimeException failure) {
            closeFailure = failure;
        } finally {
            markClosed();
        }
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private boolean claimCloseAndAwaitDrain() {
        lifecycleLock.lock();
        try {
            if (state == State.CLOSED) {
                return false;
            }
            state = State.CLOSING;
            if (closeInProgress) {
                while (state != State.CLOSED) {
                    lifecycleChanged.awaitUninterruptibly();
                }
                return false;
            }
            closeInProgress = true;
            while (activeOperations > 0) {
                lifecycleChanged.awaitUninterruptibly();
            }
            return true;
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void release() {
        lifecycleLock.lock();
        try {
            activeOperations--;
            if (activeOperations < 0) {
                throw new IllegalStateException(
                        "runtime admission count became negative"
                );
            }
            if (activeOperations == 0) {
                lifecycleChanged.signalAll();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void markClosed() {
        lifecycleLock.lock();
        try {
            state = State.CLOSED;
            lifecycleChanged.signalAll();
        } finally {
            lifecycleLock.unlock();
        }
    }

    static final class Admission implements AutoCloseable {
        private final RuntimePartitionHandle owner;
        private final AtomicBoolean released = new AtomicBoolean();

        private Admission(RuntimePartitionHandle owner) {
            this.owner = owner;
        }

        RuntimeQueue queue() {
            return owner.queue;
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true)) {
                owner.release();
            }
        }
    }

    private enum State {
        READY,
        CLOSING,
        CLOSED
    }
}

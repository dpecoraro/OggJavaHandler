package com.santander.goldengate.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks asynchronous Kafka deliveries and makes a GoldenGate transaction
 * commit wait until every record in that transaction has been acknowledged.
 */
final class TransactionDeliveryTracker {

    static final int DEFAULT_MAX_PENDING_DELIVERIES = 1_000;

    private final Semaphore capacity;
    private final Map<String, TransactionState> transactions = new ConcurrentHashMap<>();
    private final Object lifecycleMonitor = new Object();
    private final AtomicBoolean accepting = new AtomicBoolean(true);
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    private final AtomicLong accepted = new AtomicLong();
    private final AtomicLong acknowledged = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong committedTransactions = new AtomicLong();
    private final AtomicLong backpressureCount = new AtomicLong();
    private final AtomicLong backpressureNanos = new AtomicLong();
    private final AtomicLong deliveryLatencyNanos = new AtomicLong();
    private final AtomicLong maxDeliveryLatencyNanos = new AtomicLong();

    TransactionDeliveryTracker(int maxPendingDeliveries) {
        if (maxPendingDeliveries <= 0) {
            throw new IllegalArgumentException("maxPendingDeliveries must be greater than zero");
        }
        this.capacity = new Semaphore(maxPendingDeliveries);
    }

    Delivery begin(String transactionKey) throws Exception {
        if (transactionKey == null || transactionKey.isEmpty()) {
            throw new IllegalArgumentException("transactionKey must not be empty");
        }
        ensureAccepting();
        throwIfFailed();

        long waitStarted = System.nanoTime();
        boolean acquiredImmediately = capacity.tryAcquire();
        if (!acquiredImmediately) {
            backpressureCount.incrementAndGet();
            try {
                capacity.acquire();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw ex;
            } finally {
                backpressureNanos.addAndGet(System.nanoTime() - waitStarted);
            }
        }

        boolean registered = false;
        try {
            synchronized (lifecycleMonitor) {
                ensureAccepting();
                throwIfFailed();
                TransactionState state = transactions.computeIfAbsent(
                        transactionKey, ignored -> new TransactionState());
                synchronized (state) {
                    if (state.closed) {
                        throw new IllegalStateException(
                                "Transaction already closed for delivery: " + transactionKey);
                    }
                    state.pending++;
                }
                accepted.incrementAndGet();
                pending.incrementAndGet();
                registered = true;
                return new Delivery(state, System.nanoTime());
            }
        } finally {
            if (!registered) {
                capacity.release();
            }
        }
    }

    void complete(Delivery delivery, Throwable failure) {
        if (delivery == null || !delivery.completed.compareAndSet(false, true)) {
            return;
        }

        long latency = Math.max(0L, System.nanoTime() - delivery.startedNanos);
        deliveryLatencyNanos.addAndGet(latency);
        updateMax(maxDeliveryLatencyNanos, latency);

        synchronized (delivery.state) {
            if (failure != null && delivery.state.failure == null) {
                delivery.state.failure = failure;
            }
            delivery.state.pending--;
            if (delivery.state.pending < 0) {
                delivery.state.pending = 0;
            }
            delivery.state.notifyAll();
        }

        if (failure == null) {
            acknowledged.incrementAndGet();
        } else {
            failed.incrementAndGet();
            firstFailure.compareAndSet(null, failure);
        }
        pending.decrementAndGet();
        capacity.release();
    }

    void commit(String transactionKey) throws Exception {
        awaitTransaction(transactionKey, true);
    }

    void rollback(String transactionKey) throws Exception {
        awaitTransaction(transactionKey, false);
    }

    private void awaitTransaction(String transactionKey, boolean committed) throws Exception {
        if (transactionKey == null || transactionKey.isEmpty()) {
            throwIfFailed();
            return;
        }
        TransactionState state = transactions.get(transactionKey);
        if (state == null) {
            throwIfFailed();
            if (committed) {
                committedTransactions.incrementAndGet();
            }
            return;
        }

        Throwable transactionFailure;
        try {
            synchronized (state) {
                state.closed = true;
                while (state.pending > 0) {
                    try {
                        state.wait();
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        throw ex;
                    }
                }
                transactionFailure = state.failure;
            }
        } finally {
            transactions.remove(transactionKey, state);
        }

        if (transactionFailure != null) {
            throw deliveryException(transactionFailure);
        }
        throwIfFailed();
        if (committed) {
            committedTransactions.incrementAndGet();
        }
    }

    void stopAccepting() {
        synchronized (lifecycleMonitor) {
            accepting.set(false);
        }
    }

    void awaitAll() throws Exception {
        stopAccepting();
        Throwable failure = null;
        List<Map.Entry<String, TransactionState>> snapshot =
                new ArrayList<>(transactions.entrySet());
        for (Map.Entry<String, TransactionState> entry : snapshot) {
            try {
                awaitTransaction(entry.getKey(), false);
            } catch (Exception ex) {
                if (failure == null) {
                    failure = ex;
                }
            }
        }
        if (failure != null) {
            throw deliveryException(failure);
        }
        throwIfFailed();
    }

    long acceptedCount() {
        return accepted.get();
    }

    long acknowledgedCount() {
        return acknowledged.get();
    }

    long failedCount() {
        return failed.get();
    }

    long pendingCount() {
        return pending.get();
    }

    long committedTransactionCount() {
        return committedTransactions.get();
    }

    long backpressureCount() {
        return backpressureCount.get();
    }

    long backpressureNanos() {
        return backpressureNanos.get();
    }

    long averageDeliveryLatencyNanos() {
        long completed = acknowledged.get() + failed.get();
        return completed == 0 ? 0L : deliveryLatencyNanos.get() / completed;
    }

    long maxDeliveryLatencyNanos() {
        return maxDeliveryLatencyNanos.get();
    }

    int activeTransactionCount() {
        return transactions.size();
    }

    private void ensureAccepting() {
        if (!accepting.get()) {
            throw new IllegalStateException("Delivery tracker is shutting down");
        }
    }

    private void throwIfFailed() throws Exception {
        Throwable failure = firstFailure.get();
        if (failure != null) {
            throw deliveryException(failure);
        }
    }

    private Exception deliveryException(Throwable failure) {
        if (failure instanceof Exception) {
            return (Exception) failure;
        }
        return new IllegalStateException("Kafka delivery failed", failure);
    }

    private static void updateMax(AtomicLong maximum, long candidate) {
        long current = maximum.get();
        while (candidate > current && !maximum.compareAndSet(current, candidate)) {
            current = maximum.get();
        }
    }

    static final class Delivery {
        private final TransactionState state;
        private final long startedNanos;
        private final AtomicBoolean completed = new AtomicBoolean();

        private Delivery(TransactionState state, long startedNanos) {
            this.state = state;
            this.startedNanos = startedNanos;
        }
    }

    private static final class TransactionState {
        private int pending;
        private boolean closed;
        private Throwable failure;
    }
}

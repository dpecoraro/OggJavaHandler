package com.santander.goldengate.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

class TransactionDeliveryTrackerTest {

    @Test
    void acceptsMultipleDeliveriesWithoutWaitingForIndividualAcknowledgements() throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(3);

        TransactionDeliveryTracker.Delivery first = tracker.begin("tx-1");
        TransactionDeliveryTracker.Delivery second = tracker.begin("tx-1");
        TransactionDeliveryTracker.Delivery third = tracker.begin("tx-1");

        assertEquals(3, tracker.acceptedCount());
        assertEquals(3, tracker.pendingCount());

        tracker.complete(first, null);
        tracker.complete(second, null);
        tracker.complete(third, null);
        tracker.commit("tx-1");

        assertEquals(3, tracker.acknowledgedCount());
        assertEquals(0, tracker.pendingCount());
        assertEquals(1, tracker.committedTransactionCount());
    }

    @Test
    void commitWaitsForEveryAcknowledgement() throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(10);
        TransactionDeliveryTracker.Delivery delivery = tracker.begin("tx-wait");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> commit = executor.submit(() -> {
                try {
                    tracker.commit("tx-wait");
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });

            assertThrows(TimeoutException.class, () -> commit.get(100, TimeUnit.MILLISECONDS));
            tracker.complete(delivery, null);
            commit.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void asynchronousFailureIsPropagatedAtCommit() throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(10);
        TransactionDeliveryTracker.Delivery delivery = tracker.begin("tx-failure");
        SerializationException failure = new SerializationException("serialization failed");

        tracker.complete(delivery, failure);

        SerializationException thrown = assertThrows(
                SerializationException.class, () -> tracker.commit("tx-failure"));
        assertSame(failure, thrown);
        assertEquals(1, tracker.failedCount());
        assertEquals(0, tracker.pendingCount());
    }

    @Test
    void globalLimitAppliesBackpressureUntilCapacityIsReleased() throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(1);
        TransactionDeliveryTracker.Delivery first = tracker.begin("tx-first");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TransactionDeliveryTracker.Delivery> blocked =
                    executor.submit(() -> tracker.begin("tx-second"));

            assertThrows(TimeoutException.class, () -> blocked.get(100, TimeUnit.MILLISECONDS));
            assertEquals(1, tracker.pendingCount());

            tracker.complete(first, null);
            TransactionDeliveryTracker.Delivery second = blocked.get(1, TimeUnit.SECONDS);
            assertEquals(1, tracker.pendingCount());
            assertEquals(1, tracker.backpressureCount());
            assertTrue(tracker.backpressureNanos() > 0);

            tracker.complete(second, null);
            tracker.commit("tx-first");
            tracker.commit("tx-second");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void outOfOrderAcknowledgementDoesNotReleaseCommitEarly() throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(10);
        TransactionDeliveryTracker.Delivery first = tracker.begin("tx-order");
        TransactionDeliveryTracker.Delivery second = tracker.begin("tx-order");
        tracker.complete(second, null);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> commit = executor.submit(() -> {
                try {
                    tracker.commit("tx-order");
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
            assertThrows(TimeoutException.class, () -> commit.get(100, TimeUnit.MILLISECONDS));
            tracker.complete(first, null);
            commit.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void transactionsAreIsolated() throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(10);
        TransactionDeliveryTracker.Delivery first = tracker.begin("tx-a");
        TransactionDeliveryTracker.Delivery second = tracker.begin("tx-b");
        tracker.complete(first, null);

        tracker.commit("tx-a");
        assertEquals(1, tracker.activeTransactionCount());
        assertEquals(1, tracker.pendingCount());

        tracker.complete(second, null);
        tracker.commit("tx-b");
        assertEquals(0, tracker.activeTransactionCount());
    }

    @Test
    void rollbackAndShutdownRemovePendingState() throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(10);
        TransactionDeliveryTracker.Delivery rollbackDelivery = tracker.begin("tx-rollback");
        tracker.complete(rollbackDelivery, null);
        tracker.rollback("tx-rollback");

        TransactionDeliveryTracker.Delivery shutdownDelivery = tracker.begin("tx-shutdown");
        tracker.complete(shutdownDelivery, null);
        tracker.awaitAll();

        assertEquals(0, tracker.activeTransactionCount());
        assertEquals(0, tracker.pendingCount());
        assertThrows(IllegalStateException.class, () -> tracker.begin("tx-new"));
    }

    @Test
    void duplicateCallbackIsIgnored() throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(1);
        TransactionDeliveryTracker.Delivery delivery = tracker.begin("tx-duplicate");

        tracker.complete(delivery, null);
        tracker.complete(delivery, new SerializationException("late duplicate"));
        tracker.commit("tx-duplicate");

        assertEquals(1, tracker.acknowledgedCount());
        assertEquals(0, tracker.failedCount());
        assertEquals(0, tracker.pendingCount());
    }

    @Test
    void shutdownRejectsADeliveryWaitingForCapacity() throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(1);
        TransactionDeliveryTracker.Delivery first = tracker.begin("tx-running");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TransactionDeliveryTracker.Delivery> blocked =
                    executor.submit(() -> tracker.begin("tx-blocked"));
            assertThrows(TimeoutException.class, () -> blocked.get(100, TimeUnit.MILLISECONDS));

            tracker.stopAccepting();
            tracker.complete(first, null);

            assertThrows(java.util.concurrent.ExecutionException.class,
                    () -> blocked.get(1, TimeUnit.SECONDS));
            tracker.awaitAll();
            assertEquals(0, tracker.pendingCount());
            assertEquals(0, tracker.activeTransactionCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void asynchronousWindowOutperformsSequentialAcknowledgementForNetworkDelays() throws Exception {
        List<Long> delaysMillis = Arrays.asList(5L, 10L, 25L);
        long sequentialNanos = 0L;
        long asynchronousNanos = 0L;

        for (long delayMillis : delaysMillis) {
            sequentialNanos += runDelayedDeliveries(delayMillis, 12, true);
            asynchronousNanos += runDelayedDeliveries(delayMillis, 12, false);
        }

        assertTrue(asynchronousNanos * 3 < sequentialNanos,
                "Expected the asynchronous window to be at least three times faster");
    }

    private long runDelayedDeliveries(long delayMillis, int count, boolean sequential)
            throws Exception {
        TransactionDeliveryTracker tracker = new TransactionDeliveryTracker(count);
        ScheduledExecutorService callbacks = Executors.newScheduledThreadPool(2);
        long started = System.nanoTime();
        try {
            for (int index = 0; index < count; index++) {
                String transaction = sequential ? "tx-" + index : "tx-window";
                TransactionDeliveryTracker.Delivery delivery = tracker.begin(transaction);
                callbacks.schedule(() -> tracker.complete(delivery, null),
                        delayMillis, TimeUnit.MILLISECONDS);
                if (sequential) {
                    tracker.commit(transaction);
                }
            }
            if (!sequential) {
                tracker.commit("tx-window");
            }
            return System.nanoTime() - started;
        } finally {
            callbacks.shutdownNow();
        }
    }
}

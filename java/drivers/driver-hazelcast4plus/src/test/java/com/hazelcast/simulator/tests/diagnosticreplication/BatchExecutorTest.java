package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.tests.diagnosticreplication.OperationQueue.Operation;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.GET;
import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.PUT;
import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.REMOVE;
import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.SET;
import static org.assertj.core.api.Assertions.assertThat;

public class BatchExecutorTest {

    private ScheduledExecutorService responseExecutor;

    @Before
    public void setup() {
        responseExecutor = Executors.newScheduledThreadPool(5);
    }

    @After
    public void teardown()
            throws InterruptedException {
        responseExecutor.shutdownNow();
        if (!responseExecutor.awaitTermination(20, TimeUnit.SECONDS)) {
            throw new RuntimeException("Failed to shutdown executor");
        }
    }

    @Test
    public void test()
            throws InterruptedException {
        AtomicInteger totalLatenciesProcessed = new AtomicInteger();
        AtomicLong totalLatencyMillis = new AtomicLong();

        int operationConcurrency = 5;
        BatchExecutor underTest = new BatchExecutor(responseExecutor, this::startOp, (op, latency) -> {
            totalLatenciesProcessed.incrementAndGet();
            totalLatencyMillis.addAndGet(latency.toMillis());
        });

        Duration batchDuration = Duration.ofSeconds(20);
        long start = System.nanoTime();
        underTest.executeBatch(new ReplicationRecipe.Batch(
                Set.of(new MapOperation("a", GET, 500), new MapOperation("b", PUT, 500), new MapOperation("c", SET, 500),
                        new MapOperation("d", REMOVE, 500))), batchDuration, operationConcurrency);
        Duration actualDuration = Duration.ofNanos(System.nanoTime() - start);
        assertThat(actualDuration.toMillis() - batchDuration.toMillis()).isBetween(-1000L, 1000L);
        assertThat(totalLatenciesProcessed.get()).isEqualTo(2000);
        assertThat(totalLatencyMillis.get()).isBetween(20000L, 25000L);
    }

    private ActiveOperation startOp(Operation op) {
        long start = System.nanoTime();
        long delay =  switch (op.type()) {
            case GET, REMOVE -> 5L;
            case PUT -> 20L;
            case SET -> 10;
        };
        CompletableFuture<Void> future = new CompletableFuture<>();
        responseExecutor.schedule(() -> future.complete(null), delay, TimeUnit.MILLISECONDS);
        return new ActiveOperation(start, future);
    }
}

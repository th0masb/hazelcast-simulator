package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.tests.diagnosticreplication.OperationQueue.Operation;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.GET;
import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.PUT;
import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.REMOVE;
import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.SET;
import static org.assertj.core.api.Assertions.assertThat;

public class BatchExecutorTest {

    private ExecutorService responseExecutor;

    @Before
    public void setup() {
        responseExecutor = Executors.newFixedThreadPool(5);
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

        long start = System.currentTimeMillis();
        underTest.executeBatch(new ReplicationRecipe.Batch(
                List.of(new MapOperation("a", GET, 500), new MapOperation("b", PUT, 500), new MapOperation("c", SET, 500),
                        new MapOperation("d", REMOVE, 500))), Duration.ofSeconds(20), operationConcurrency);
        Duration actualDuration = Duration.ofNanos(System.nanoTime() - start);
        assertThat(totalLatenciesProcessed.get()).isEqualTo(2000);
//        assertThat(totalLatencyMillis.get()).isEqualTo(20000);
        //assertThat(actualDuration.toSeconds())
    }

    private ActiveOperation startOp(Operation op) {
        long start = System.nanoTime();
        return new ActiveOperation(start, CompletableFuture.runAsync(() -> {
            long delay = switch (op.type()) {
                case GET, REMOVE -> 5L;
                case PUT -> 20L;
                case SET -> 10;
            };
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}

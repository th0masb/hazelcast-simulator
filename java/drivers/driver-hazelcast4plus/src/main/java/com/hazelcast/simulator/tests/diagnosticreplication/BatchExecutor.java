package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.tests.diagnosticreplication.OperationQueue.Operation;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class BatchExecutor {

    private final BiConsumer<Operation, Duration> latencyConsumer;
    private final Function<Operation, ActiveOperation> operationSubmitter;

    public BatchExecutor(BiConsumer<Operation, Duration> latencyConsumer,
                         Function<Operation, ActiveOperation> operationSubmitter) {
        this.latencyConsumer = latencyConsumer;
        this.operationSubmitter = operationSubmitter;
    }

    void executeBatch(Batch batch, Duration targetDuration, int operationConcurrency)
            throws InterruptedException {
        final long start = System.nanoTime();
        final long targetEnd = start + targetDuration.toNanos();

        Semaphore throttle = new Semaphore(operationConcurrency);
        OperationQueue queue = new OperationQueue(batch);
        AtomicReference<Throwable> error = new AtomicReference<>();

        while (queue.getRemainingOperations() > 0) {
            throttle.acquire();
            if (error.get() != null) {
                throttle.release();
                break;
            }
            Operation nextOp = queue.next();
            ActiveOperation op = operationSubmitter.apply(nextOp);
            op.operation().thenRun(() -> {
                Duration opLatency = Duration.ofNanos(System.nanoTime() - op.start());
                latencyConsumer.accept(nextOp, opLatency);
                Duration durationUntilBatchEnd = Duration.ofNanos(Math.max(0, targetEnd - System.nanoTime()));
                int remainingOps = queue.getRemainingOperations();
                if (remainingOps > 0) {
                    long millisPerRemainingOp = (durationUntilBatchEnd.toMillis() / remainingOps) * operationConcurrency;
                    try {
                        // TODO possibly better to track the moving average of op latencies
                        Thread.sleep(Math.max(0, millisPerRemainingOp - opLatency.toMillis()));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }).whenComplete((_v, ex) -> {
                error.compareAndSet(null, ex);
                throttle.release();
            });
        }

        // Wait for all operations in flight
        // TODO Should we cancel all existing ops?
        throttle.acquire(operationConcurrency);
        Throwable err = error.get();
        if (err != null) {
            throw new RuntimeException(err);
        }
    }
}

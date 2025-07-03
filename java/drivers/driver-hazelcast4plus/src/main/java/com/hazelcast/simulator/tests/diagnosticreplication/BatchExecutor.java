package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.tests.diagnosticreplication.OperationQueue.Operation;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.concurrent.TimeUnit.SECONDS;

public class BatchExecutor {

    private static final long ACQUIRE_TIMEOUT_SECS = 300;

    private final ScheduledExecutorService responseExecutor;
    private final BiConsumer<Operation, Duration> latencyConsumer;
    private final Function<Operation, ActiveOperation> operationSubmitter;

    public BatchExecutor(ScheduledExecutorService responseExecutor, Function<Operation, ActiveOperation> operationSubmitter,
                         BiConsumer<Operation, Duration> latencyConsumer) {
        this.responseExecutor = responseExecutor;
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
            if (!throttle.tryAcquire(ACQUIRE_TIMEOUT_SECS, SECONDS)) {
                throw new IllegalStateException("Timeout on permit acquisition for submitting next operation");
            }
            if (error.get() != null) {
                throttle.release();
                break;
            }
            Operation nextOp = queue.next();
            ActiveOperation op = operationSubmitter.apply(nextOp);
            op.operation().whenCompleteAsync((result, err) -> {
                if (err != null) {
                    error.compareAndSet(null, err);
                    throttle.release();
                } else {
                    Duration latency = Duration.ofNanos(System.nanoTime() - op.start());
                    latencyConsumer.accept(nextOp, latency);
                    int remainingOps = queue.getRemainingOperations();
                    Duration sleepDuration = computePauseDuration(targetEnd, remainingOps, operationConcurrency, latency);
                    // Release the throttle after a pause to spread the load across the time allocated for the batch
                    responseExecutor.schedule(() -> throttle.release(), sleepDuration.toMillis(), TimeUnit.MILLISECONDS);
                }
            }, responseExecutor);
        }

        // Wait for all operations in flight to complete
        if (!throttle.tryAcquire(operationConcurrency, ACQUIRE_TIMEOUT_SECS, SECONDS)) {
            throw new IllegalStateException("Timeout waiting for all operations to complete");
        }

        // Propagate any operation failure to stop the test
        if (error.get() != null) {
            throw new RuntimeException(error.get());
        }
    }

    private Duration computePauseDuration(long targetEnd, int remainingOps, int operationConcurrency, Duration opLatency) {
        if (remainingOps == 0) {
            return Duration.ZERO;
        }
        Duration durationUntilBatchEnd = Duration.ofNanos(Math.max(0, targetEnd - System.nanoTime()));
        long millisPerRemainingOp = (durationUntilBatchEnd.toMillis() / remainingOps) * operationConcurrency;
        return Duration.ofMillis(Math.max(0, millisPerRemainingOp - opLatency.toMillis()));
    }
}

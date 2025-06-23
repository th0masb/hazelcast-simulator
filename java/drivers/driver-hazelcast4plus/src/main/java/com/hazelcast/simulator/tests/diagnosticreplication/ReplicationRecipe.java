package com.hazelcast.simulator.tests.diagnosticreplication;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @param batchDuration The duration in which all assigned operations in a single batch should be completed
 * @param batches The sequence of operations we need to perform split into discrete batches
 */
public record ReplicationRecipe(Duration batchDuration, List<Batch> batches) {
    /**
     * @param operations The operation definition mapped to the number of times it should be performed
     */
    public record Batch(Map<MapOperation, Integer> operations) {
    }
}

package com.hazelcast.simulator.tests.diagnosticreplication;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;

/**
 * @param batchDuration The duration in which all assigned operations in a single batch should be completed
 * @param batches The sequence of operations we need to perform split into discrete batches
 */
public record ReplicationRecipe(List<MapSeed> mapSeeds, Duration batchDuration, List<Batch> batches) {
    /**
     * @param mapName
     * @param size
     * @param averageValueBytes
     */
    public record MapSeed(String mapName, long size, int averageValueBytes) {
    }

    /**
     * @param operations The operation definition mapped to the number of times it should be performed
     */
    public record Batch(List<MapOperation> operations) {
        /**
         * @param mapName
         * @param type
         */
        public record MapOperation(String mapName, Type type, int count) {
            public enum Type {
                GET,
                PUT,
                SET,
                REMOVE,
            }
        }
    }

    public static ObjectMapper createObjectMapper() {
        return new ObjectMapper().findAndRegisterModules();
    }
}

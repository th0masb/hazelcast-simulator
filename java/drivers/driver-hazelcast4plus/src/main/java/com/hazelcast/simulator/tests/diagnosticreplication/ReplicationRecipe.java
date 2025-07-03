package com.hazelcast.simulator.tests.diagnosticreplication;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.List;
import java.util.Set;

/**
 * @param batchDuration The duration in which all assigned operations in a single batch should be completed
 * @param batches       The ordered sequence of operation batches we need to perform
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ReplicationRecipe(Set<MapSeed> mapSeeds, Duration batchDuration, List<Batch> batches) {
    /**
     * @param mapName           The map name
     * @param size              Number of global entries in the map
     * @param averageValueBytes Average size of entries in the map
     */
    public record MapSeed(String mapName, long size, int averageValueBytes) {
    }

    /**
     * @param operations The set of operations in this batch
     */
    public record Batch(Set<MapOperation> operations) {
        /**
         * @param mapName The map on which the operation should be performed
         * @param type    The operation type
         * @param count   The number of times it should be performed
         */
        public record MapOperation(String mapName, Type type, int count) {
            public enum Type {
                GET, PUT, SET, REMOVE,
            }
        }
    }

    public static ObjectMapper createObjectMapper() {
        return new ObjectMapper().findAndRegisterModules();
    }
}

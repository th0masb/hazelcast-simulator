package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.MapSeed;
import com.hazelcast.simulator.worker.WorkerIndex;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.GET;
import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.PUT;
import static org.assertj.core.api.Assertions.assertThat;

public class StateDistributionTest {

    private final WorkerIndex index = new WorkerIndex(3, 5);

    @Test
    public void testMapStateInitialisation() {
        Map<String, MapState> output = StateDistribution.initMapStates(index,
                List.of(new MapSeed("mapA", 6, 1024), new MapSeed("mapB", 10, 512), new MapSeed("mapC", 23, 2048),
                        new MapSeed("mapD", 99, 256)));

        assertThat(output).isEqualTo(Map.of("mapA", new MapState(index, 1024, 1), "mapB", new MapState(index, 512, 2), "mapC",
                new MapState(index, 2048, 4), "mapD", new MapState(index, 256, 20)));
    }

    @Test
    public void testBatchInitialisation() {
        List<Batch> output = StateDistribution.initBatches(index, List.of(new Batch(
                List.of(new MapOperation("mapA", GET, 101), new MapOperation("mapA", PUT, 103), new MapOperation("mapB", GET, 99),
                        new MapOperation("mapB", PUT, 1)))));

        assertThat(output).isEqualTo(List.of(new Batch(
                List.of(new MapOperation("mapA", GET, 20), new MapOperation("mapA", PUT, 21), new MapOperation("mapB", GET, 19),
                        new MapOperation("mapB", PUT, 1)))));
    }
}

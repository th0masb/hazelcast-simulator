package com.hazelcast.simulator.tests.diagnosticreplication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.MapSeed;
import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation.Type.GET;
import static org.assertj.core.api.Assertions.assertThat;

public class RecipeDeserializationTest {
    @Test
    public void metadataIsIgnored()
            throws JsonProcessingException {
        String underTest = """
                {
                    "metadata": {
                        "startInstant": "",
                        "totalDuration": 120
                    },
                    "mapSeeds": [
                        {"mapName": "A","size": 103,"averageValueBytes": 1024}
                    ],
                    "batchDuration": 60.000000000,
                    "batches": [
                        {
                            "operations": [
                                {
                                    "mapName": "B",
                                    "type": "GET",
                                    "count": 76
                                }
                            ]
                        }
                    ]
                }
                """;

        assertThat(ReplicationRecipe.createObjectMapper().readValue(underTest, ReplicationRecipe.class))
                .isEqualTo(new ReplicationRecipe(
                        List.of(new MapSeed("A", 103, 1024)),
                        Duration.ofMinutes(1),
                        List.of(new Batch(List.of(new MapOperation("B", GET, 76))))
                ));

    }
}

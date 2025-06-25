package com.hazelcast.simulator.tests.diagnosticreplication;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.diagnosticreplication.WorkerDistribution.initialiseBatches;
import static com.hazelcast.simulator.tests.diagnosticreplication.WorkerDistribution.initialiseMapKeyDomains;

public class DiagnosticReplicationTest
        extends HazelcastTest {

    private static final String COORDINATOR_NAME = "coordinator";
    private static final String RECIPE_PATH = "upload/recipe.json";

    // We probably want to balance the operations across all workers so we don't have some workers doing all the removes for example
    // Is it worthwhile tracking the size of the mas across the entire run instead of just the start?
    // The main challenge here is how to choose which key to perform an operation on and tracking the map keys so we don't e.g.
    // have all gets on empty values. Ideally this key hit rate would be tunable at different levels of granularity.

    // Is controlling the hit rate even possible without expensive synchronisation?
    // We start with contiguous key ranges.
    // We could assign the workers their own key ranges and track which gets were removed so they can be added again and avoid
    // gets on removed keys, problem if maps start with small numbers of keys
    // It would probably be better to restrict workers to the non-contiguous set by modulus and we can guarantee that workers
    // won't step on each others keys and should be quite cheap to manage

    // Basically keep a per worker stack of populated keys where puts/sets either change existing value or add to stack, ratio
    // could be tuned. Removes just delete from head of the stack.

    // Initialised during setup
    private ConcurrentMap<String, MapState> mapState;
    private List<Batch> batches;

    // We need the workers to start their run as closely together as possible for best replication so we use a latch
    private ICountDownLatch coordinationLatch;

    @Prepare
    public void prepareOperations() {
        ReplicationRecipe globalRecipe = loadGlobalReplicationRecipe();
        mapState = initialiseMapKeyDomains(testContext.getWorkerIndex(), globalRecipe);
        batches = initialiseBatches(testContext.getWorkerIndex(), globalRecipe);
        prepareCoordinationLatch();
    }

    private void prepareCoordinationLatch() {
        coordinationLatch = targetInstance.getCPSubsystem().getCountDownLatch(COORDINATOR_NAME);
        coordinationLatch.trySetCount(testContext.getWorkerIndex().workerCount());
    }

    private ReplicationRecipe loadGlobalReplicationRecipe() {
        Path recipePath = Path.of(RECIPE_PATH);
        if (!Files.isRegularFile(recipePath)) {
            throw new IllegalStateException("No recipe file found at " + RECIPE_PATH);
        }
        try {
            return ReplicationRecipe.createObjectMapper().readValue(recipePath.toFile(), ReplicationRecipe.class);
        } catch (JsonParseException e) {
            throw new RuntimeException("Recipe content is not valid json", e);
        } catch (JsonMappingException e) {
            throw new RuntimeException("Recipe content is valid json but malformed", e);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read contents of the recipe file", e);
        }
    }

    @Run
    public void runTest() {
        try {
            if (!awaitWorkersReady()) {
                throw new IllegalStateException("Could not synchronise the test start!");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

    }

    private boolean awaitWorkersReady()
            throws InterruptedException {
        coordinationLatch.countDown();
        return coordinationLatch.await(600, TimeUnit.SECONDS);
    }
}

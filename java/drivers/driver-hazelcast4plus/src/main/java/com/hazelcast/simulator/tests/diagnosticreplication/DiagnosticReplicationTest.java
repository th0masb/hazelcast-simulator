package com.hazelcast.simulator.tests.diagnosticreplication;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.tests.diagnosticreplication.StateDistribution.initBatches;
import static com.hazelcast.simulator.tests.diagnosticreplication.StateDistribution.initMapStates;
import static com.hazelcast.simulator.worker.loadsupport.Streamer.DEFAULT_CONCURRENCY_LEVEL;

public class DiagnosticReplicationTest
        extends HazelcastTest {

    private static final String COORDINATOR_LATCH_NAME = "coordinator";
    private static final String DEFAULT_RECIPE_PATH = "upload/recipe.json";
    private static final int DEFAULT_SYNC_TIMEOUT_SECS = 300;

    private static final Logger LOGGER = LogManager.getLogger(DiagnosticReplicationTest.class);

    public int workerSyncTimeoutSecs = DEFAULT_SYNC_TIMEOUT_SECS;
    public String recipePath = DEFAULT_RECIPE_PATH;

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

    // TODO
    //  - Populate the maps with initial data
    //  - Implement the actual test

    @Prepare
    public void prepareInitialState() {
        LOGGER.info("Loading global replication recipe from {}", DEFAULT_RECIPE_PATH);
        ReplicationRecipe globalRecipe = loadGlobalReplicationRecipe();
        LOGGER.info("Initialising the map state for {} seeds", globalRecipe.mapSeeds().size());
        mapState = initMapStates(testContext.getWorkerIndex(), globalRecipe.mapSeeds());
        LOGGER.info("Extracting our operations from {} global batches", globalRecipe.batches());
        batches = initBatches(testContext.getWorkerIndex(), globalRecipe.batches());
        initCoordinationLatch();
        populateMaps();
    }

    @Prepare(global = true)
    public void sendTestDurationToUser() {
        ReplicationRecipe globalRecipe = loadGlobalReplicationRecipe();
        long expectedRuntimeMins = (globalRecipe.batchDuration().toSeconds() * globalRecipe.batches().size()) / 60;
        testContext.echoCoordinator(
                "SimulationDetails { mapCount=%s, batchCount=%s, batchDurationSecs=%s, expectedRunTimeMins=%s }",
                globalRecipe.mapSeeds().size(), globalRecipe.batches().size(), globalRecipe.batchDuration().toSeconds(),
                expectedRuntimeMins);
    }

    private void populateMaps() {
        int workerCount = testContext.getWorkerIndex().workerCount();
        for (var entry : mapState.entrySet()) {
            IMap<Long, byte[]> m = targetInstance.getMap(entry.getKey());
            Streamer<Long, byte[]> streamer = StreamerFactory.getInstance(m,
                    Math.max(1, DEFAULT_CONCURRENCY_LEVEL / workerCount));
            int valueSize = entry.getValue().getValueSizeBytes();
            entry.getValue().streamKeys().forEach(key -> streamer.pushEntry(key, randomByteArray(valueSize)));
        }
    }

    private byte[] randomByteArray(int length) {
        byte[] result = new byte[length];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    private void initCoordinationLatch() {
        coordinationLatch = targetInstance.getCPSubsystem().getCountDownLatch(COORDINATOR_LATCH_NAME);
        coordinationLatch.trySetCount(testContext.getWorkerIndex().workerCount());
    }

    private ReplicationRecipe loadGlobalReplicationRecipe() {
        File recipeFile = new File(recipePath);
        if (!recipeFile.isFile()) {
            throw new IllegalStateException("No recipe file found at " + recipePath);
        }
        try {
            return ReplicationRecipe.createObjectMapper().readValue(recipeFile, ReplicationRecipe.class);
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
            LOGGER.warn("Test interrupted while synchronising run start");
            return;
        }

        LOGGER.info("Workers synchronised, beginning first simulation batch");
    }

    private boolean awaitWorkersReady()
            throws InterruptedException {
        coordinationLatch.countDown();
        return coordinationLatch.await(workerSyncTimeoutSecs, TimeUnit.SECONDS);
    }
}

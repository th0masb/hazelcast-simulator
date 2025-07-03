package com.hazelcast.simulator.tests.diagnosticreplication;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.tests.diagnosticreplication.OperationQueue.Operation;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch.MapOperation;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.simulator.tests.diagnosticreplication.StateDistribution.initBatches;
import static com.hazelcast.simulator.tests.diagnosticreplication.StateDistribution.initOwnedKeys;
import static com.hazelcast.simulator.worker.loadsupport.Streamer.DEFAULT_CONCURRENCY_LEVEL;
import static java.lang.String.format;

/**
 * A test which executes a preexisting {@link ReplicationRecipe} derived from some set of diagnostics to reproduce
 * the same workload. The default location for the recipe file is "upload/recipe.json" relative to the simulation
 * directory. Any file in the "upload" subdirectory is copied to the worker hosts by the simulator and will be
 * available to tests.
 * <p>
 * The test is split into a sequence of discrete operations batches. Batch i should be executed concurrently on all
 * workers, the loadgenerators are periodically synchronized using the CP subsystem to keep them aligned.
 */
public class DiagnosticReplicationTest
        extends HazelcastTest {

    private static final String SYNC_LATCH_NAME = "synchronizer";
    private static final int EXECUTOR_SHUTDOWN_WAIT_SECS = 30;

    private static final Logger LOGGER = LogManager.getLogger(DiagnosticReplicationTest.class);

    /**
     * Relative path to the recipe file which defines the test
     */
    public String recipePath = "upload/recipe.json";

    /**
     * Seconds to wait when synchronizing workers, if any worker waits longer than this an exception will be thrown and the
     * test terminated.
     */
    public int workerSyncTimeoutSecs = 300;

    /**
     * Workers are synchronized when batch index i satisfies i % syncFrequency == 0
     */
    public int syncFrequency = 5;

    /**
     * Probability a get operation will access a populated key
     */
    public int getHitPercentage = 100;

    /**
     * Probability a put/set operation will access a populated key
     */
    public int putHitPercentage = 100;

    /**
     * Number of threads used to process operation responses
     */
    public int threadCount = 10;

    /**
     * Max number of operations in flight at once for a single loadgenerator
     */
    public int operationConcurrency = 20;

    /**
     * Scaling factor applied to the operation count for each batch to allow artificially higher/lower loads
     */
    public double operationVolumeScale = 1.0;

    private ConcurrentMap<String, OwnedKeys> ownedMapKeys;
    private List<Batch> batches;
    private Duration targetBatchDuration;
    // We need the workers to start their run as closely together as possible for best replication so we use a latch
    private ICountDownLatch syncLatch;

    @Prepare
    public void prepareInitialState() {
        LOGGER.info("Loading global replication recipe from {}", recipePath);
        ReplicationRecipe globalRecipe = loadGlobalReplicationRecipe();
        LOGGER.info("Initialising the map state for {} seeds", globalRecipe.mapSeeds().size());
        ownedMapKeys = initOwnedKeys(testContext.getWorkerIndex(), globalRecipe.mapSeeds());
        LOGGER.info("Extracting our operations from {} global batches", globalRecipe.batches().size());
        batches = initBatches(testContext.getWorkerIndex(), globalRecipe.batches()).stream().map(this::scaleOperations).toList();
        initProbes(batches);
        targetBatchDuration = globalRecipe.batchDuration();
        initSyncLatch();
        populateOwnedKeys();
    }

    private Batch scaleOperations(Batch input) {
        return new Batch(input.operations().stream().map(op -> new MapOperation(op.mapName(), op.type(),
                Math.toIntExact(Math.round(op.count() * operationVolumeScale)))).collect(Collectors.toSet()));
    }

    private void initProbes(List<Batch> batches) {
        batches.stream().flatMap(batch -> batch.operations().stream()).distinct()
               .forEach(op -> testContext.getLatencyProbe(probeName(op.mapName(), op.type()), true));
    }

    private String probeName(String mapName, MapOperation.Type type) {
        return mapName + "-" + type;
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

    private void populateOwnedKeys() {
        int workerCount = testContext.getWorkerIndex().workerCount();
        for (var entry : ownedMapKeys.entrySet()) {
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

    private void initSyncLatch() {
        syncLatch = targetInstance.getCPSubsystem().getCountDownLatch(SYNC_LATCH_NAME);
        syncLatch.trySetCount(testContext.getWorkerIndex().workerCount());
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
    public void runReplication() {
        LOGGER.info("Beginning test execution");
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(threadCount);
        try {
            BatchExecutor batchExecutor = new BatchExecutor(executor, this::startOperation, this::handleLatency);
            for (int batchIndex = 0; batchIndex < batches.size(); batchIndex++) {
                if (batchIndex % syncFrequency == 0) {
                    syncWorkers(batchIndex);
                    LOGGER.info("Workers synchronised at batchIndex={}", batchIndex);
                }
                LOGGER.info("Starting operations in batch {}", batchIndex);
                long batchStart = System.nanoTime();
                batchExecutor.executeBatch(batches.get(batchIndex), targetBatchDuration, operationConcurrency);
                Duration batchDuration = Duration.ofNanos(System.nanoTime() - batchStart);
                long timeDriftMillis = batchDuration.toMillis() - targetBatchDuration.toMillis();
                LOGGER.info("Batch {} complete with time drift of {} ms", batchIndex, timeDriftMillis);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Test interrupted prematurely");
        } finally {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(EXECUTOR_SHUTDOWN_WAIT_SECS, TimeUnit.SECONDS)) {
                    LOGGER.warn("Failed to shutdown test executor after {} seconds", EXECUTOR_SHUTDOWN_WAIT_SECS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void handleLatency(Operation op, Duration latency) {
        testContext.getLatencyProbe(probeName(op.mapName(), op.type())).recordValue(latency.toNanos());
    }

    private ActiveOperation startOperation(Operation op) {
        IMap<Long, byte[]> m = targetInstance.getMap(op.mapName());
        int valueSize = ownedMapKeys.get(op.mapName()).getValueSizeBytes();
        long key = chooseKey(op);
        byte[] value = randomByteArray(valueSize);
        long startTime = System.nanoTime();
        return new ActiveOperation(startTime, switch (op.type()) {
            case REMOVE -> m.removeAsync(key);
            case GET -> m.getAsync(key);
            case PUT -> m.putAsync(key, value);
            case SET -> m.setAsync(key, value);
        });
    }

    private long chooseKey(Operation op) {
        Random rng = ThreadLocalRandom.current();
        OwnedKeys state = ownedMapKeys.get(op.mapName());
        boolean isEmpty = state.size() == 0;
        return switch (op.type()) {
            // If there are no keys then the remove will just look at the empty 0 key
            case REMOVE -> isEmpty ? state.zeroKey() : state.deleteLargestKey();
            case GET -> {
                boolean isHit = rng.nextInt(100) < getHitPercentage;
                yield isEmpty || !isHit ? rng.nextLong() : state.getRandomDomainKey();
            }
            case PUT, SET -> {
                boolean isHit = rng.nextInt(100) < putHitPercentage;
                yield isEmpty || !isHit ? state.addNextEmptyKey() : state.getRandomDomainKey();
            }
        };
    }

    private void syncWorkers(int index)
            throws InterruptedException {
        syncLatch.trySetCount(testContext.getWorkerIndex().workerCount());
        syncLatch.countDown();
        if (!syncLatch.await(workerSyncTimeoutSecs, TimeUnit.SECONDS)) {
            throw new RuntimeException(
                    format("Worker sync timeout at batch %s from worker %s", index, testContext.getWorkerIndex().index()));
        }
    }
}

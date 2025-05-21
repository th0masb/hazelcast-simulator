/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.simulator.tests.map;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Pipelining;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.assignKeyToIndex;
import static com.hazelcast.simulator.utils.GeneratorUtils.generateByteArrays;
import static java.lang.Thread.currentThread;
import static java.util.stream.Collectors.groupingBy;

public class LongByteArrayMapTest extends HazelcastTest {

    // properties
    public int keyDomain = 10000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;
    public int pipelineDepth = 10;
    public int pipelineIterations = 100;
    public int getAllSize = 5;
    public int mapCount = 1;
    /**
     * The fixed keys to be used in the test. If set to 0, all
     * keys are random. Currently the only {@code get} operation uses it.
     * It should be less than the {@code keyDomain}.
     */
    public int fixedKeyDomain = 0;
    /**
     * The probability of using a fixed key from the {@code fixedKeyDomain}.
     * It is used if {@code fixedKeyPercentage} is set to a value greater than 0.
     */
    public int fixedKeyProbability = 0;

    private byte[][] values;
    private long[] fixedKeys;
    private final List<List<IMap<Long, byte[]>>> maps = new ArrayList<>();
    private final Executor callerRuns = Runnable::run;
    private final Random random = new Random();

    // Tracks which thread is assigned which client by its index
    private final Map<Thread, Integer> clientIndexForThread = new ConcurrentHashMap<>();

    @Setup
    public void setUp() {
        for (HazelcastInstance instance : getTargetInstances()) {
            List<IMap<Long, byte[]>> mapsForInstance = new ArrayList<>();
            maps.add(mapsForInstance);
            for (int i = 0; i < mapCount; i++) {
                String mapName = (mapCount == 1) ? name : name + "_" + i;
                mapsForInstance.add(instance.getMap(mapName));
            }
        }
        values = generateByteArrays(valueCount, minValueLength, maxValueLength);
    }

    @Prepare
    public void prepareFixedKeys() {
        // We want to balance the fixed keys across cluster members
        if (fixedKeyDomain > 0) {
            // Fetch the initialised partition table
            Set<Partition> partitions = targetInstance.getPartitionService().getPartitions();
            while (partitions.isEmpty()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    return;
                }
                partitions = targetInstance.getPartitionService().getPartitions();
            }

            int partitionCount = partitions.size();
            if (partitionCount <= fixedKeyDomain) {
                fixedKeys = LongStream.range(0, fixedKeyDomain).toArray();
            } else {
                fixedKeys = new long[fixedKeyDomain];
                Map<Member, Integer> memberCounts = new HashMap<>();
                partitions.stream().map(Partition::getOwner).filter(Objects::nonNull).forEach(m -> memberCounts.putIfAbsent(m, 0));
                int keysPerMember = fixedKeyDomain / memberCounts.size();
                int i = 0;
                long keyCandidate = 0;
                while (i < fixedKeys.length) {
                    Partition partitionForKey = targetInstance.getPartitionService().getPartition(keyCandidate);
                    int currentCount = memberCounts.get(partitionForKey.getOwner());
                    if (currentCount < keysPerMember) {
                        memberCounts.put(partitionForKey.getOwner(), currentCount + 1);
                        fixedKeys[i++] = keyCandidate;
                    }
                    if (keyCandidate++ == 100_000) {
                        throw new IllegalStateException("Cannot fill fixed keys");
                    }
                }
            }
        }
    }

    @Prepare(global = true)
    public void prepare() {
        // We only need to use one instance to prepare the maps
        for (IMap<Long, byte[]> map : maps.get(0)) {
            Streamer<Long, byte[]> streamer = StreamerFactory.getInstance(map);
            for (long key = 0; key < keyDomain; key++) {
                byte[] value = values[random.nextInt(valueCount)];
                streamer.pushEntry(key, value);
            }
            streamer.await();
        }
    }

    private IMap<Long, byte[]> getRandomMap() {
        List<IMap<Long, byte[]>> mapsToSelectFrom;
        if (maps.size() == 1) {
            mapsToSelectFrom = maps.get(0);
        } else {
            Integer clientIndex = clientIndexForThread.get(currentThread());
            mapsToSelectFrom = maps.get(clientIndex == null ? putClientForCurrentThread() : clientIndex);
        }
        return mapsToSelectFrom.get(random.nextInt(mapCount));
    }

    private synchronized int putClientForCurrentThread() {
        return assignKeyToIndex(getTargetInstances().size(), currentThread(), clientIndexForThread);
    }

    @TimeStep(prob = -1)
    public byte[] get(ThreadState state) {
        var map = getRandomMap();
        boolean useFixedKey = fixedKeyDomain > 0 && state.randomInt(100) < fixedKeyProbability;
        if (useFixedKey) {
            return map.get(fixedKeys[state.randomInt(fixedKeyDomain)]);
        } else {
            return map.get(state.randomKey());
        }
    }

    @TimeStep(prob = -1)
    public Map<Long, byte[]> getAll(ThreadState state) {
        Set<Long> keys = new HashSet<>();
        for (int k = 0; k < getAllSize; k++) {
            keys.add(state.randomKey());
        }
        return getRandomMap().getAll(keys);
    }

    @TimeStep(prob = 0)
    public CompletableFuture getAsync(ThreadState state) {
        return getRandomMap().getAsync(state.randomKey()).toCompletableFuture();
    }

    @TimeStep(prob = 0.1)
    public byte[] put(ThreadState state) {
        return getRandomMap().put(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0.0)
    public CompletableFuture putAsync(ThreadState state) {
        return getRandomMap().putAsync(state.randomKey(), state.randomValue()).toCompletableFuture();
    }

    @TimeStep(prob = 0)
    public void set(ThreadState state) {
        getRandomMap().set(state.randomKey(), state.randomValue());
    }

    @TimeStep(prob = 0)
    public CompletableFuture setAsync(ThreadState state) {
        return getRandomMap().setAsync(state.randomKey(), state.randomValue()).toCompletableFuture();
    }

    @TimeStep(prob = 0)
    public void pipelinedGet(final ThreadState state, @StartNanos final long startNanos, final LatencyProbe probe) throws Exception {
        if (state.pipeline == null) {
            state.pipeline = new Pipelining<>(pipelineDepth);
        }

        CompletableFuture<byte[]> f = getRandomMap().getAsync(state.randomKey()).toCompletableFuture();
        f.whenCompleteAsync((bytes, throwable) -> probe.done(startNanos), callerRuns);
        state.pipeline.add(f);
        state.i++;
        if (state.i == pipelineIterations) {
            state.i = 0;
            state.pipeline.results();
            state.pipeline = null;
        }
    }

    public class ThreadState extends BaseThreadState {
        public static final int HIGHEST_PROBABILITY = 100;
        private Pipelining<byte[]> pipeline;
        private int i;

        private long fixedKeyOrRandom() {
            if (fixedKeyDomain > 0 && fixedKeyDomain < keyDomain && fixedKeyProbability > 0 &&
                    randomInt(HIGHEST_PROBABILITY) < fixedKeyProbability) {
                return randomLong(fixedKeyDomain);
            }
            return randomKey();
        }

        private long randomKey() {
            return randomLong(keyDomain);
        }

        private byte[] randomValue() {
            return values[randomInt(values.length)];
        }
    }

    @Teardown
    public void tearDown() {
        maps.stream().flatMap(Collection::stream).forEach(IMap::destroy);
    }
}

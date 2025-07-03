package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.worker.WorkerIndex;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

/**
 * The set of populated keys for a map owned by a single worker. Each worker is guaranteed to have
 * mutually disjoint keysets so they can correctly maintain which keys are populated without having
 * to synchronize with other workers. This is done by mapping each worker with index i to the set of
 * integers n with
 * <pre>
 * n % workerCount == i
 * </pre>
 */
class OwnedKeys {

    private final WorkerIndex index;
    private final int valueSizeBytes;

    private long size;

    OwnedKeys(WorkerIndex index, int valueSizeBytes, long initialSize) {
        this.index = index;
        this.valueSizeBytes = valueSizeBytes;
        this.size = initialSize;
    }

    long getRandomDomainKey() {
        if (size == 0) {
            throw new RuntimeException("Key state is empty!");
        }
        return transformMultipleToKey(ThreadLocalRandom.current().nextLong(size));
    }

    long addNextEmptyKey() {
        return transformMultipleToKey(size++);
    }

    long deleteLargestKey() {
        if (size == 0) {
            throw new RuntimeException("Key state is empty!");
        }
        return transformMultipleToKey(--size);
    }

    long zeroKey() {
        return transformMultipleToKey(0);
    }

    long size() {
        return size;
    }

    int getValueSizeBytes() {
        return valueSizeBytes;
    }

    private long transformMultipleToKey(long multiple) {
        return multiple * index.workerCount() + index.index();
    }

    LongStream streamKeys() {
        return LongStream.range(0, size).map(this::transformMultipleToKey);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OwnedKeys ownedKeys = (OwnedKeys) o;
        return valueSizeBytes == ownedKeys.valueSizeBytes && size == ownedKeys.size && Objects.equals(index, ownedKeys.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, valueSizeBytes, size);
    }

    @Override
    public String toString() {
        return "MapState{" + "index=" + index + ", valueSizeBytes=" + valueSizeBytes + ", size=" + size + '}';
    }
}

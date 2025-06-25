package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.worker.WorkerIndex;

import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

class MapState {
    private final WorkerIndex index;
    private final int valueSizeBytes;

    private long size;

    MapState(WorkerIndex index, int valueSizeBytes, long initialSize) {
        this.index = index;
        this.valueSizeBytes = valueSizeBytes;
        this.size = initialSize;
    }

    long getRandomDomainKey() {
        Random rng = ThreadLocalRandom.current();
        return size == 0 ? rng.nextLong() : transformMultipleToKey(rng.nextLong(size));
    }

    long addNextEmptyKey() {
        return transformMultipleToKey(size++);
    }

    //        long deleteLargestKey() {
    //            return transformMultipleToKey(--size);
    //        }

    long size() {
        return size;
    }

    int getValueSizeBytes() {
        return valueSizeBytes;
    }

    private long transformMultipleToKey(long multiple) {
        return multiple * index.workerCount() + index.index();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MapState mapState = (MapState) o;
        return valueSizeBytes == mapState.valueSizeBytes && size == mapState.size && Objects.equals(index, mapState.index);
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

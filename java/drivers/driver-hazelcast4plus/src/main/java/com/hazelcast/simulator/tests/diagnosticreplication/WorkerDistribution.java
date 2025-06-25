package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;
import com.hazelcast.simulator.worker.WorkerIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static java.util.Comparator.comparing;

class WorkerDistribution {
    private WorkerDistribution() {
    }

    static ConcurrentMap<String, MapState> initialiseMapKeyDomains(WorkerIndex worker, ReplicationRecipe globalRecipe) {
        ConcurrentMap<String, MapState> mapState = new ConcurrentHashMap<>();
        for (ReplicationRecipe.MapSeed mapSeed : globalRecipe.mapSeeds()) {
            // The map size is divided equally amongst the workers
            long globalMapSize = mapSeed.size();
            long quotient = globalMapSize / worker.workerCount();
            long remainder = globalMapSize % worker.workerCount();
            long localSize = quotient + remainder >= worker.index() ? 1 : 0;
            mapState.put(mapSeed.mapName(), new MapState(worker, mapSeed.averageValueBytes(), localSize));
        }
        return mapState;
    }

    static List<Batch> initialiseBatches(WorkerIndex worker, ReplicationRecipe globalRecipe) {
        List<Batch> batches = new ArrayList<>();
        for (Batch globalBatch : globalRecipe.batches()) {
            List<Batch.MapOperation> operations = new ArrayList<>(globalBatch.operations());
            // Sort order doesn't matter the only thing which matters is the sorting is uniform across workers
            operations.sort(comparing(op -> format("%s-%s", op.mapName(), op.type())));
            Batch localBatch = new Batch(new ArrayList<>());
            int nextWorkerOwedExtraOp = 0;
            for (Batch.MapOperation op : operations) {
                // Total count of this op which needs dividing between workers
                int globalCount = op.count();
                // Every worker gets at least this value
                int quotient = globalCount / worker.workerCount();
                // Remainder needs to be distributed fairly across the workers
                int remainder = globalCount % worker.workerCount();

                boolean getsExtraOp = (nextWorkerOwedExtraOp + worker.index()) % worker.workerCount() < remainder;
                int localSize = quotient + (getsExtraOp ? 1 : 0);
                localBatch.operations().add(new Batch.MapOperation(op.mapName(), op.type(), localSize));
                nextWorkerOwedExtraOp = (nextWorkerOwedExtraOp + remainder) % worker.workerCount();
            }
            batches.add(localBatch);
        }
        return batches;
    }
}

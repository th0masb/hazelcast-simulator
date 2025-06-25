package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;
import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.MapSeed;
import com.hazelcast.simulator.worker.WorkerIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;
import static java.util.Comparator.comparing;

class StateDistribution {
    private StateDistribution() {
    }

    /**
     * Partitions the map seeds in the global recipe evenly amongst all workers
     * @param worker The worker to compute the map states for
     * @param mapSeeds The global description of the initial map states
     */
    static ConcurrentMap<String, MapState> initialiseMapStates(WorkerIndex worker, List<MapSeed> mapSeeds) {
        ConcurrentMap<String, MapState> mapState = new ConcurrentHashMap<>();
        for (MapSeed mapSeed : mapSeeds) {
            // The map size is divided equally amongst the workers
            long globalMapSize = mapSeed.size();
            long quotient = globalMapSize / worker.workerCount();
            long remainder = globalMapSize % worker.workerCount();
            long localSize = quotient + (remainder > worker.index() ? 1 : 0);
            mapState.put(mapSeed.mapName(), new MapState(worker, mapSeed.averageValueBytes(), localSize));
        }
        return mapState;
    }

    /**
     * Partitions the batch operations in the global recipe evenly amongst all workers
     * @param worker The worker to compute the map states for
     * @param globalBatches The batches describing the global operations
     */
    static List<Batch> initialiseBatches(WorkerIndex worker, List<Batch> globalBatches) {
        List<Batch> batches = new ArrayList<>();
        for (Batch globalBatch : globalBatches) {
            List<Batch.MapOperation> operations = new ArrayList<>(globalBatch.operations());
            // The thing which matters is the order is uniform across workers
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

                boolean getsExtraOp = shouldGetExtraOp(worker, remainder, nextWorkerOwedExtraOp);
                int localSize = quotient + (getsExtraOp ? 1 : 0);
                nextWorkerOwedExtraOp = (nextWorkerOwedExtraOp + remainder) % worker.workerCount();
                if (localSize > 0) {
                    localBatch.operations().add(new Batch.MapOperation(op.mapName(), op.type(), localSize));
                }
            }
            batches.add(localBatch);
        }
        return batches;
    }

    private static boolean shouldGetExtraOp(WorkerIndex worker, int remainder, int nextWorkerOwedExtraOp) {
        boolean getsExtraOp = false;
        if (remainder > 0) {
            int lastWorkerOwedExtraOp = (nextWorkerOwedExtraOp + (remainder - 1)) % worker.workerCount();
            int index = worker.index();
            // First case handles no wrap around, second case handles wrap around
            getsExtraOp = (nextWorkerOwedExtraOp <= index && index <= lastWorkerOwedExtraOp)
                    || (lastWorkerOwedExtraOp < nextWorkerOwedExtraOp && index <= lastWorkerOwedExtraOp);
        }
        return getsExtraOp;
    }
}

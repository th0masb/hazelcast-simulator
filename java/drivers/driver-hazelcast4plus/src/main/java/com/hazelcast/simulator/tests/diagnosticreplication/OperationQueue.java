package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.simulator.tests.diagnosticreplication.ReplicationRecipe.Batch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static java.util.stream.Collectors.toCollection;

public class OperationQueue {

    private final List<OperationCount> operations;
    private volatile int remainingOperations;

    public record Operation(String mapName, Batch.MapOperation.Type type) {
    }

    private static class OperationCount {
        final Operation operation;
        int count;

        public OperationCount(Batch.MapOperation source) {
            this.operation = new Operation(source.mapName(), source.type());
            this.count = source.count();
        }
    }

    public OperationQueue(Batch batch) {
        operations = batch.operations().stream().map(OperationCount::new).collect(toCollection(ArrayList::new));
        remainingOperations = operations.stream().mapToInt(op -> op.count).sum();
    }

    // The current approach is selecting operations with uniform distribution
    public Operation next() {
        if (operations.isEmpty()) {
            return null;
        }
        int opIndex = ThreadLocalRandom.current().nextInt(remainingOperations);
        int typeIndex = -1;
        int accumulation = 0;
        for (int i = 0; i < operations.size(); i++) {
            accumulation += operations.get(i).count;
            if (opIndex < accumulation) {
                typeIndex = i;
                break;
            }
        }
        OperationCount op = operations.get(typeIndex);
        op.count--;
        remainingOperations--;
        if (op.count == 0) {
            operations.remove(typeIndex);
        }
        return op.operation;
    }

    public int getRemainingOperations() {
        return remainingOperations;
    }
}

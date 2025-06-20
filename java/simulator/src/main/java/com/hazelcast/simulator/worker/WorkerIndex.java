package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;

public record WorkerIndex(int index, int workerCount) {
    public WorkerIndex(WorkerParameters params) {
        this(params.intGet(WorkerParam.TYPE_INDEX), params.intGet(WorkerParam.TYPE_COUNT));
    }
}

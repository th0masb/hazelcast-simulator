package com.hazelcast.simulator.worker;

public enum WorkerParam {
    TYPE("WORKER_TYPE"),
    TYPE_COUNT("WORKER_TYPE_COUNT"),
    TYPE_INDEX("WORKER_TYPE_INDEX"),
    ADDRESS("WORKER_ADDRESS");

    public final String key;

    WorkerParam(String key) {
        this.key = key;
    }
}

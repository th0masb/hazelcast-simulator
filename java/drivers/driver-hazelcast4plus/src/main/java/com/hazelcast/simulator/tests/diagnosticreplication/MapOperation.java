package com.hazelcast.simulator.tests.diagnosticreplication;

public record MapOperation(String mapName, Type type) {
    public enum Type {
        GET,
        PUT,
        SET,
        REMOVE,
    }
}

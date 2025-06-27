package com.hazelcast.simulator.tests.diagnosticreplication;

import java.util.concurrent.CompletionStage;

public record ActiveOperation(long start, CompletionStage<?> operation) {
}

package com.hazelcast.simulator.tests.diagnosticreplication;

import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;

import java.util.concurrent.TimeUnit;

public class DiagnosticReplicationTest extends HazelcastTest {

    private ICountDownLatch coordinationLatch;
    private ReplicationRecipe recipe;

    @Prepare
    public void prepareOperations() {
        // Here we parse the input file and use our worker index to determine the
        // operations we will need to run in the time chunks. We then create our
        // latch


        coordinationLatch = targetInstance.getCPSubsystem().getCountDownLatch("coordinator");
        coordinationLatch.trySetCount(testContext.getWorkerIndex().workerCount());
    }

    @Run
    public void runTest() {
        try {
            if (!awaitWorkersReady()) {
                throw new IllegalStateException("Could not synchronise the test start!");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

    }

    private boolean awaitWorkersReady()
            throws InterruptedException {
        coordinationLatch.countDown();
        return coordinationLatch.await(600, TimeUnit.SECONDS);
    }
}

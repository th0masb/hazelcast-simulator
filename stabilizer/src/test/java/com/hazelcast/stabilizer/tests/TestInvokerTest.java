package com.hazelcast.stabilizer.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.stabilizer.tests.annotations.Run;
import com.hazelcast.stabilizer.tests.annotations.Setup;
import com.hazelcast.stabilizer.tests.annotations.Verify;
import com.hazelcast.stabilizer.tests.annotations.Warmup;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TestInvokerTest {
    // =================== setup ========================

    @Test
    public void testRun() throws Throwable {
        DummyTest dummyTest = new DummyTest();
        TestInvoker invoker = new TestInvoker(dummyTest,new DummyTestContext());
        invoker.run();

        assertTrue(dummyTest.runCalled);
    }

    @Test(expected = IllegalTestException.class)
    public void runMissing() throws Throwable {
        RunMissingTest test = new RunMissingTest();
        new TestInvoker(test,new DummyTestContext());
    }

    static class RunMissingTest {

        @Setup
        void setup(TestContext context) {
        }
    }

    // =================== setup ========================

    @Test
    public void testSetup() throws Throwable {
        DummyTestContext testContext = new DummyTestContext();
        DummyTest test = new DummyTest();
        TestInvoker invoker = new TestInvoker(test,testContext);
        invoker.run();
        invoker.setup();

        assertTrue(test.setupCalled);
        assertSame(testContext, test.context);
    }

    @Test(expected = IllegalTestException.class)
    public void setupMissing() throws Throwable {
        SetupMissingTest test = new SetupMissingTest();
        new TestInvoker(test,new DummyTestContext());
    }


    static class SetupMissingTest {

        @Run
        void run() {
        }
    }

    // =================== setup ========================


    static class FullTest{

        @Setup
        void setup(TestContext testContext){

        }

        @Warmup(global = false)
        void localWarmup(){

        }

        @Warmup(global = true)
        void globalWarmup(){

        }

        @Run
        void run(){

        }

        @Verify(global = false)
        void localVerify(){

        }

        @Verify(global = false)
        void globalVerify(){

        }

        @Verify(global = false)
        void localTeardown(){

        }

        @Verify(global = true)
        void globalTeardown(){

        }
    }

    static class DummyTest {
        boolean runCalled;
        boolean setupCalled;
        TestContext context;

        @Setup
        void setup(TestContext context) {
            this.context = context;
            this.setupCalled = true;
        }

        @Run
        void run() {
            runCalled = true;
        }
    }

    static class DummyTestContext implements TestContext {
        @Override
        public HazelcastInstance getTargetInstance() {
            return null;
        }

        @Override
        public String getTestId() {
            return null;
        }

        @Override
        public boolean isStopped() {
            return false;
        }
    }
}

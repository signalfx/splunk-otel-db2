package com.splunk.otel.db2;

import com.splunk.otel.db2.config.Db2ReceiverConfig;
import com.splunk.otel.db2.config.OtlpExporterConfig;
import com.splunk.otel.db2.pipeline.Db2Pipeline;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Concurrency is asserted with a barrier rather than wall-clock timing. An earlier version timed
 * connects to a black-holed address, which a runner that answers ENETUNREACH immediately would let
 * a serial implementation pass — a test that cannot fail. A barrier cannot be satisfied serially at
 * all, so it holds regardless of the runner's network or load.
 */
class MainStartupTest {

    private static final int BARRIER_TIMEOUT_SECONDS = 10;

    private static Db2ReceiverConfig receiver(String name) {
        Db2ReceiverConfig rc = new Db2ReceiverConfig();
        rc.setComponentName(name);
        rc.setHostname("db2-" + name + ".example.com");
        rc.setHostport(50000);
        rc.setUsername("user");
        rc.setPassword("pass");
        rc.setDatabase("TESTDB");
        return rc;
    }

    private static List<Db2ReceiverConfig> receivers(int count) {
        List<Db2ReceiverConfig> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(receiver("db" + i));
        }
        return list;
    }

    private static OtlpExporterConfig otlp() {
        OtlpExporterConfig otlp = new OtlpExporterConfig();
        otlp.setEndpoint("http://127.0.0.1:4317");
        return otlp;
    }

    /**
     * Every receiver must be building at the same moment. A serial implementation parks the first
     * task on the barrier forever, which trips the timeout and fails — so this cannot pass unless
     * startup genuinely overlaps.
     */
    @Test
    void everyPipelineIsBuiltConcurrently() throws Exception {
        int count = 4;
        CyclicBarrier allBuilding = new CyclicBarrier(count);

        List<Db2Pipeline> pipelines = Main.startAll(receivers(count), otlp(), (rc, otlpConfig) -> {
            try {
                allBuilding.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                throw new IllegalStateException(
                    "startup is serialised: only some receivers reached the barrier", e);
            } catch (InterruptedException | BrokenBarrierException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return mock(Db2Pipeline.class);
        });

        assertThat(pipelines).hasSize(count);
    }

    /**
     * One bad receiver among several: startup aborts, and the pipelines that did come up are
     * stopped rather than left holding JDBC connections and live schedulers.
     */
    @Test
    void aFailedReceiverRollsBackTheOnesThatStarted() {
        int count = 4;
        List<Db2Pipeline> created = new java.util.concurrent.CopyOnWriteArrayList<>();

        assertThatThrownBy(() ->
            Main.startAll(receivers(count), otlp(), (rc, otlpConfig) -> {
                if ("db2".equals(rc.getComponentName())) {
                    throw new IllegalStateException("boom: " + rc.getComponentName());
                }
                Db2Pipeline p = mock(Db2Pipeline.class);
                created.add(p);
                return p;
            }))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("boom: db2");

        assertThat(created).as("the three healthy receivers were built").hasSize(count - 1);
        for (Db2Pipeline p : created) {
            verify(p).stop();
        }
    }

    // The failure must surface even when it happens in start() rather than construction, and the
    // half-started pipeline must be stopped too.
    @Test
    void aPipelineThatThrowsFromStartIsAlsoRolledBack() {
        Db2Pipeline failing = mock(Db2Pipeline.class);
        org.mockito.Mockito.doThrow(new IllegalStateException("start failed")).when(failing).start();

        assertThatThrownBy(() -> Main.startAll(receivers(1), otlp(), (rc, otlpConfig) -> failing))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("start failed");

        verify(failing).stop();
    }

    /**
     * A worker whose construction outlives its own cancellation must still be rolled back.
     * {@code cancel(true)} only interrupts, and the JDBC connect this stands in for does not answer
     * interruption — so without draining the pool the worker adds and starts its pipeline after
     * rollback has finished, and nothing ever stops it.
     */
    @Test
    void aWorkerThatOutlivesCancellationIsStillRolledBack() throws Exception {
        CountDownLatch slowFactoryEntered = new CountDownLatch(1);
        CountDownLatch releaseSlowFactory = new CountDownLatch(1);
        Db2Pipeline slow = mock(Db2Pipeline.class);
        Db2Pipeline fast = mock(Db2Pipeline.class);
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread starter = new Thread(() -> {
            try {
                Main.startAll(receivers(2), otlp(), (rc, otlpConfig) -> {
                    if (!"db1".equals(rc.getComponentName())) {
                        return fast;
                    }
                    slowFactoryEntered.countDown();
                    // Deliberately ignores interruption, the way DriverManager.getConnection does.
                    while (true) {
                        try {
                            releaseSlowFactory.await();
                            return slow;
                        } catch (InterruptedException ignored) {
                            // keep going: this is the whole point of the test
                        }
                    }
                });
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "startAll-under-test");
        starter.start();

        assertThat(slowFactoryEntered.await(10, TimeUnit.SECONDS))
            .as("the slow factory should have been entered").isTrue();
        starter.interrupt();

        // The observable that separates the two implementations, with no reliance on who wins a
        // race: startAll cannot legitimately return while a worker is still inside the factory,
        // because that worker is about to add and start a pipeline nobody will stop. Draining the
        // pool makes returning early impossible, so the thread must still be alive here. Without
        // the drain it returns in microseconds — these are mocks — and this assertion fails.
        starter.join(TimeUnit.SECONDS.toMillis(2));
        boolean returnedWhileWorkerStillConstructing = !starter.isAlive();

        releaseSlowFactory.countDown();
        starter.join(TimeUnit.SECONDS.toMillis(30));

        assertThat(returnedWhileWorkerStillConstructing)
            .as("startAll returned before its cancelled worker finished; that worker's pipeline "
                + "is started after rollback and leaks")
            .isFalse();
        assertThat(starter.isAlive()).as("startAll should have returned once released").isFalse();
        assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
        verify(slow).stop();
        verify(fast).stop();
    }

    // The barrier above proves construction overlaps; this proves start() does too, which is where
    // the blocking version probe runs.
    @Test
    void everyPipelineIsStartedConcurrently() throws Exception {
        int count = 4;
        CyclicBarrier allStarting = new CyclicBarrier(count);

        List<Db2Pipeline> pipelines = Main.startAll(receivers(count), otlp(), (rc, otlpConfig) -> {
            Db2Pipeline p = mock(Db2Pipeline.class);
            org.mockito.Mockito.doAnswer(invocation -> {
                allStarting.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                return null;
            }).when(p).start();
            return p;
        });

        assertThat(pipelines).hasSize(count);
    }

    // Rollback must overlap too: stop() can block for connect_timeout, so a serial unwind
    // reintroduces the delay the parallel start removed.
    @Test
    void rollbackStopsPipelinesConcurrently() {
        int healthy = 3;
        CyclicBarrier allStopping = new CyclicBarrier(healthy);

        assertThatThrownBy(() ->
            Main.startAll(receivers(healthy + 1), otlp(), (rc, otlpConfig) -> {
                if ("db3".equals(rc.getComponentName())) {
                    throw new IllegalStateException("boom");
                }
                Db2Pipeline p = mock(Db2Pipeline.class);
                org.mockito.Mockito.doAnswer(invocation -> {
                    allStopping.await(BARRIER_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    return null;
                }).when(p).stop();
                return p;
            }))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void stoppingAnEmptyListIsANoOp() {
        Main.stopAll(List.of(), 1);
    }

    // A single receiver still goes through the same path; nothing is stopped on success.
    @Test
    void aSingleHealthyReceiverStartsAndIsNotStopped() throws Exception {
        Db2Pipeline healthy = mock(Db2Pipeline.class);

        List<Db2Pipeline> pipelines = Main.startAll(receivers(1), otlp(), (rc, otlpConfig) -> healthy);

        assertThat(pipelines).containsExactly(healthy);
        verify(healthy).start();
        verify(healthy, never()).stop();
    }
}

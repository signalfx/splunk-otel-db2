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
import static org.mockito.Mockito.atLeastOnce;
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
        CountDownLatch fastFactoryDone = new CountDownLatch(1);
        CountDownLatch slowFactoryEntered = new CountDownLatch(1);
        CountDownLatch releaseSlowFactory = new CountDownLatch(1);
        Db2Pipeline slow = mock(Db2Pipeline.class);
        Db2Pipeline fast = mock(Db2Pipeline.class);
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread starter = new Thread(() -> {
            try {
                Main.startAll(receivers(2), otlp(), (rc, otlpConfig) -> {
                    if (!"db1".equals(rc.getComponentName())) {
                        fastFactoryDone.countDown();
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

        boolean returnedWhileWorkerStillConstructing;
        try {
            // Both workers must be past the point of interest before interrupting, otherwise the
            // fast one may not yet be registered and the assertions below race scheduler timing.
            assertThat(fastFactoryDone.await(10, TimeUnit.SECONDS))
                .as("the fast factory should have completed").isTrue();
            assertThat(slowFactoryEntered.await(10, TimeUnit.SECONDS))
                .as("the slow factory should have been entered").isTrue();
            starter.interrupt();

            // The observable that separates the two implementations, with no reliance on who wins a
            // race: startAll cannot legitimately return while a worker is still inside the factory,
            // because that worker is about to start a pipeline rollback has stopped looking for.
            // Draining makes returning early impossible, so the thread must still be alive here.
            // Without it, it returns in microseconds — these are mocks — and this fails.
            starter.join(TimeUnit.SECONDS.toMillis(2));
            returnedWhileWorkerStillConstructing = !starter.isAlive();
        } finally {
            // In a finally so a failed assertion above cannot leave this thread parked forever.
            releaseSlowFactory.countDown();
            starter.join(TimeUnit.SECONDS.toMillis(30));
        }

        assertThat(returnedWhileWorkerStillConstructing)
            .as("startAll returned before its cancelled worker finished; that worker's pipeline "
                + "is started after rollback and leaks")
            .isFalse();
        assertThat(starter.isAlive()).as("startAll should have returned once released").isFalse();
        assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
        // atLeastOnce, not once: a worker that sees the abort flag after start() stops its own
        // pipeline, and rollback may stop the same one. Db2Pipeline.stop() is idempotent, so the
        // contract is "gets stopped", not "stopped exactly once".
        verify(slow, atLeastOnce()).stop();
        verify(fast, atLeastOnce()).stop();
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

    /**
     * Rollback must overlap too: {@code stop()} can block for {@code connect_timeout}, so a serial
     * unwind reintroduces the delay the parallel start removed.
     *
     * <p>Asserting only the startup exception was not enough — {@code stopAll} swallows whatever
     * {@code stop()} throws, so a timed-out barrier left no trace and the test passed under serial
     * rollback. The barrier's own broken state is the observable that cannot be swallowed.
     */
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

        assertThat(allStopping.isBroken())
            .as("a stop() timed out on the barrier, so rollback stopped pipelines one at a time")
            .isFalse();
        assertThat(allStopping.getNumberWaiting())
            .as("every stop() should have passed through the barrier")
            .isZero();
    }

    /**
     * The guarantee that does not depend on the drain succeeding. No timeout can outlast a driver
     * that ignores its own, so the drain is expired deliberately here: the worker finishes
     * afterwards and must dispose of its own pipeline rather than start one that rollback has
     * already stopped looking for.
     *
     * <p>Only reachable through the interrupt path. On the failure path {@code startAll} drains
     * every future before rolling back, so no worker can outlive it there.
     */
    @Test
    void aWorkerOutlivingAnExpiredDrainStopsItsOwnPipeline() throws Exception {
        CountDownLatch lateFactoryEntered = new CountDownLatch(1);
        CountDownLatch lateStopped = new CountDownLatch(1);
        Db2Pipeline late = mock(Db2Pipeline.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            lateStopped.countDown();
            return null;
        }).when(late).stop();
        AtomicReference<Throwable> thrown = new AtomicReference<>();

        Thread starter = new Thread(() -> {
            try {
                // One-second drain: the worker below stays busy for longer, so the wait expires
                // while it is still inside the factory.
                Main.startAll(receivers(1), otlp(), (rc, otlpConfig) -> {
                    lateFactoryEntered.countDown();
                    long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
                    while (System.nanoTime() < until) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(50);
                        } catch (InterruptedException ignored) {
                            // Ignores interruption, the way DriverManager.getConnection does.
                        }
                    }
                    return late;
                }, 1);
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "startAll-expired-drain");
        starter.start();

        assertThat(lateFactoryEntered.await(10, TimeUnit.SECONDS)).isTrue();
        starter.interrupt();
        starter.join(TimeUnit.SECONDS.toMillis(20));

        assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
        assertThat(lateStopped.await(15, TimeUnit.SECONDS))
            .as("the worker finished after rollback gave up, so it must stop its own pipeline")
            .isTrue();
        verify(late, never()).start();
    }

    /**
     * The in-progress-start case, which blocking inside the factory does not reach: there the
     * pre-start abort branch handles it and {@code start()} is never called. Here {@code start()}
     * itself is still running when rollback gives up, so only the post-start abort check can stop
     * the pipeline.
     *
     * <p>Because the pipeline is published only after {@code start()} returns, rollback cannot stop
     * it mid-start and spend the one-shot guard — the worker owns it until then.
     */
    @Test
    void aWorkerBlockedInsideStartPastAnExpiredDrainStopsItsOwnPipeline() throws Exception {
        CountDownLatch startEntered = new CountDownLatch(1);
        CountDownLatch releaseStart = new CountDownLatch(1);
        CountDownLatch pipelineStopped = new CountDownLatch(1);
        Db2Pipeline blocked = mock(Db2Pipeline.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            startEntered.countDown();
            // Ignores interruption, so the drain below cannot cut it short.
            while (true) {
                try {
                    releaseStart.await();
                    return null;
                } catch (InterruptedException ignored) {
                    // keep going: this is the point of the test
                }
            }
        }).when(blocked).start();
        org.mockito.Mockito.doAnswer(invocation -> {
            pipelineStopped.countDown();
            return null;
        }).when(blocked).stop();

        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread starter = new Thread(() -> {
            try {
                Main.startAll(receivers(1), otlp(), (rc, otlpConfig) -> blocked, 1);
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "startAll-blocked-start");
        starter.start();

        try {
            assertThat(startEntered.await(10, TimeUnit.SECONDS))
                .as("start() should have been entered").isTrue();
            starter.interrupt();

            // The one-second drain expires while start() is still blocked, so startAll returns
            // before the worker does — which is exactly the window the post-start check covers.
            starter.join(TimeUnit.SECONDS.toMillis(15));
            assertThat(starter.isAlive())
                .as("startAll should have returned once the drain expired").isFalse();
            assertThat(thrown.get()).isInstanceOf(InterruptedException.class);
        } finally {
            releaseStart.countDown();
        }

        assertThat(pipelineStopped.await(15, TimeUnit.SECONDS))
            .as("start() completed after rollback, so the worker must stop its own pipeline")
            .isTrue();
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

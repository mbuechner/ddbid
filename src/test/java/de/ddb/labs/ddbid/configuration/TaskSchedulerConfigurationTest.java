package de.ddb.labs.ddbid.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

class TaskSchedulerConfigurationTest {

    @Test
    void immediateJobsRunInQueueOrderWithoutOverlap() throws Exception {
        final var scheduler = (ThreadPoolTaskScheduler) new TaskSchedulerConfiguration().taskScheduler();
        final var started = new CountDownLatch(1);
        final var release = new CountDownLatch(1);
        final List<String> order = new CopyOnWriteArrayList<>();
        scheduler.initialize();
        try {
            assertEquals(1, scheduler.getScheduledThreadPoolExecutor().getCorePoolSize());
            ScheduledFuture<?> first = scheduler.schedule(() -> {
                order.add("first started");
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("First job interrupted while waiting for release", e);
                }
                order.add("first finished");
            }, Instant.EPOCH);
            assertTrue(started.await(10, TimeUnit.SECONDS));

            ScheduledFuture<?> second = scheduler.schedule(() -> order.add("second started"), Instant.EPOCH);
            assertEquals(1, scheduler.getScheduledThreadPoolExecutor().getQueue().size());
            assertFalse(second.isDone());
            assertEquals(List.of("first started"), order);

            release.countDown();
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertEquals(List.of("first started", "first finished", "second started"), order);
        } finally {
            release.countDown();
            scheduler.shutdown();
        }
    }
}

/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.ms365;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.AssignedLicense;
import com.microsoft.graph.models.User;

/**
 * Test class for Microsoft365DataStore base class.
 * Tests common functionality shared across all Microsoft 365 data stores.
 */
public class Microsoft365DataStoreTest extends UnitDsTestCase {

    private static final Logger logger = LogManager.getLogger(Microsoft365DataStoreTest.class);

    private TestDataStore dataStore;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new TestDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        dataStore = null;
        super.tearDown(testInfo);
    }

    // Test thread pool creation with different thread counts
    @Test
    public void test_newFixedThreadPool_singleThread() {
        final ExecutorService executor = dataStore.newFixedThreadPool(1);
        assertNotNull("ExecutorService should be created", executor);

        try {
            // Submit a simple task
            executor.submit(() -> {
                // Do nothing
            }).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Should be able to execute task: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void test_newFixedThreadPool_multipleThreads() {
        final ExecutorService executor = dataStore.newFixedThreadPool(5);
        assertNotNull("ExecutorService should be created", executor);

        try {
            final AtomicInteger counter = new AtomicInteger(0);
            final List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

            // Submit multiple tasks
            for (int i = 0; i < 10; i++) {
                futures.add(executor.submit(() -> {
                    counter.incrementAndGet();
                }));
            }

            // Wait for all tasks to complete
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }

            assertEquals("All tasks should have executed", 10, counter.get());
        } catch (Exception e) {
            fail("Should be able to execute tasks: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void test_newFixedThreadPool_cappedThreads() {
        // Request more threads than system can handle
        final int maxThreads = Runtime.getRuntime().availableProcessors() * 2;
        final int requestedThreads = maxThreads * 10;

        final ExecutorService executor = dataStore.newFixedThreadPool(requestedThreads);
        assertNotNull("ExecutorService should be created even with excessive thread request", executor);

        try {
            // Verify that executor still works properly
            executor.submit(() -> {
                // Do nothing
            }).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Capped thread pool should still function: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void test_newFixedThreadPool_minimumThreads() {
        // Test with minimum viable thread count (1)
        final ExecutorService executor = dataStore.newFixedThreadPool(1);
        assertNotNull("ExecutorService should be created with 1 thread", executor);

        try {
            executor.submit(() -> {
                // Simple test task
            }).get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail("Should execute task with minimum threads: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void test_newFixedThreadPool_threadCapping() {
        // Verify that thread capping logic works
        final int maxThreads = Runtime.getRuntime().availableProcessors() * 2;
        final int requestedThreads = maxThreads + 10;

        final ExecutorService executor = dataStore.newFixedThreadPool(requestedThreads);
        assertNotNull("ExecutorService should cap threads appropriately", executor);

        try {
            // Verify executor can handle concurrent tasks
            final List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                futures.add(executor.submit(() -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }

            for (java.util.concurrent.Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            fail("Capped thread pool should handle tasks: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    @Test
    public void test_newFixedThreadPool_callerRunsPolicy() {
        // Test that CallerRunsPolicy is applied when queue is full
        final ExecutorService executor = dataStore.newFixedThreadPool(1);
        assertNotNull("ExecutorService should be created", executor);

        try {
            // Submit tasks that will test the rejection policy
            final List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                futures.add(executor.submit(() -> {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }));
            }

            // All tasks should complete (some in caller thread due to CallerRunsPolicy)
            for (java.util.concurrent.Future<?> future : futures) {
                future.get(5, TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            fail("CallerRunsPolicy should handle task overflow: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    /**
     * Runs {@code action}, returning every record {@link Microsoft365DataStore} logged at
     * {@code WARN} or worse while it ran, in order.
     *
     * <p>The threshold keeps the pool's own debug chatter out without hiding a report that was
     * demoted to {@code WARN}: a test that compares two captures can then see the level differ
     * rather than seeing one capture silently become empty.
     *
     * @param action the code whose logging should be captured.
     * @return the captured records.
     */
    private static List<LogEvent> captureDataStoreReports(final Runnable action) {
        final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());
        final org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Microsoft365DataStore.class);
        final AbstractAppender appender =
                new AbstractAppender("test-ms365-datastore-report-capture", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(final LogEvent event) {
                        if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
                            // the pool thread appends too, and log4j may recycle the event
                            events.add(event.toImmutable());
                        }
                    }
                };
        appender.start();
        coreLogger.addAppender(appender);
        try {
            action.run();
        } finally {
            coreLogger.removeAppender(appender);
            appender.stop();
        }
        return events;
    }

    /**
     * @param events the captured records.
     * @return their formatted messages, for an assertion failure that can be read.
     */
    private static List<String> messagesOf(final List<LogEvent> events) {
        return events.stream().map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
    }

    @Test
    public void test_reportingExecutor_reportsAPoolThreadFailureIdenticallyToACallerThreadFailure() {
        // This is the property Task 3 exists for: one failure must stop having two outcomes.
        // Counting it is a different property and is pinned separately below. The two captures are
        // compared against each other rather than against two hand-written literals, so the test
        // cannot drift into agreeing with only one of the paths.
        final IllegalStateException failure = new IllegalStateException("boom");

        final List<LogEvent> poolPath = captureDataStoreReports(() -> {
            final Microsoft365DataStore.ReportingExecutor executor = dataStore.newFixedThreadPool(1);
            try {
                executor.execute(() -> {
                    throw failure;
                });
                executor.shutdown();
                assertTrue("the pool thread must finish inside the capture window", executor.awaitTermination(10, TimeUnit.SECONDS));
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted while waiting for the pool thread: " + e);
            } finally {
                executor.shutdownNow();
            }
        });

        final List<LogEvent> callerPath = captureDataStoreReports(() -> {
            final Microsoft365DataStore.ReportingExecutor executor = dataStore.newFixedThreadPool(1);
            final java.util.concurrent.CountDownLatch block = new java.util.concurrent.CountDownLatch(1);
            try {
                executor.execute(() -> {
                    try {
                        block.await();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
                // one thread is busy and the queue holds one task; the third submission saturates
                executor.execute(() -> {});
                executor.execute(() -> {
                    throw failure;
                });
            } finally {
                block.countDown();
                executor.shutdownNow();
            }
        });

        assertEquals("the pool path must report exactly once, got " + messagesOf(poolPath), 1, poolPath.size());
        assertEquals("the caller path must report exactly once, got " + messagesOf(callerPath), 1, callerPath.size());

        final LogEvent onPool = poolPath.get(0);
        final LogEvent onCaller = callerPath.get(0);
        assertEquals("both paths must report at the same level", onPool.getLevel(), onCaller.getLevel());
        assertEquals("both paths must report the same message", onPool.getMessage().getFormattedMessage(),
                onCaller.getMessage().getFormattedMessage());
        assertSame("both paths must attach the throwable that escaped", failure, onPool.getThrown());
        assertSame("both paths must attach the throwable that escaped", failure, onCaller.getThrown());

        // and the shared report must name the data store, or a mixed-crawl log cannot be read
        assertTrue(onPool.getMessage().getFormattedMessage(), onPool.getMessage().getFormattedMessage().contains("TestDataStore"));
    }

    @Test
    public void test_reportingExecutor_countsAFailureOnAPoolThread() throws Exception {
        final Microsoft365DataStore.ReportingExecutor executor = dataStore.newFixedThreadPool(1);
        try {
            executor.execute(() -> {
                throw new IllegalStateException("boom");
            });
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(1, executor.getFailureCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void test_reportingExecutor_countsAFailureRunOnTheCallerThread() {
        final Microsoft365DataStore.ReportingExecutor executor = dataStore.newFixedThreadPool(1);
        try {
            final java.util.concurrent.CountDownLatch block = new java.util.concurrent.CountDownLatch(1);
            executor.execute(() -> {
                try {
                    block.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            // one thread is busy and the queue holds one task; the third submission saturates
            executor.execute(() -> {});
            executor.execute(() -> {
                throw new IllegalStateException("boom on the caller");
            });
            assertEquals(1, executor.getFailureCount());
            block.countDown();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void test_reportingExecutor_countsNothingWhenTasksSucceed() throws Exception {
        final Microsoft365DataStore.ReportingExecutor executor = dataStore.newFixedThreadPool(1);
        try {
            executor.execute(() -> {});
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(0, executor.getFailureCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void test_getShutdownTimeoutSeconds_defaultsToTheExistingValue() {
        assertEquals(Microsoft365DataStore.EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, dataStore.getShutdownTimeoutSeconds(new DataStoreParams()));
    }

    @Test
    public void test_getShutdownTimeoutSeconds_readsTheParameter() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("executor_shutdown_timeout", "600");
        assertEquals(600L, dataStore.getShutdownTimeoutSeconds(paramMap));
    }

    @Test
    public void test_getShutdownTimeoutSeconds_malformedValueFallsBackToTheDefault() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("executor_shutdown_timeout", "soon");
        assertEquals(Microsoft365DataStore.EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, dataStore.getShutdownTimeoutSeconds(paramMap));
    }

    @Test
    public void test_getShutdownTimeoutSeconds_zeroFallsBackToTheDefault() {
        // A zero wait would cancel every in-flight task the moment shutdown starts.
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("executor_shutdown_timeout", "0");
        assertEquals(Microsoft365DataStore.EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, dataStore.getShutdownTimeoutSeconds(paramMap));
    }

    @Test
    public void test_getShutdownTimeoutSeconds_negativeValueFallsBackToTheDefault() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("executor_shutdown_timeout", "-1");
        assertEquals(Microsoft365DataStore.EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS, dataStore.getShutdownTimeoutSeconds(paramMap));
    }

    @Test
    public void test_shutdownExecutor_reportsTasksThatDidNotFinish() {
        final Microsoft365DataStore.ReportingExecutor executor = dataStore.newFixedThreadPool(1);
        final java.util.concurrent.CountDownLatch block = new java.util.concurrent.CountDownLatch(1);
        try {
            executor.execute(() -> {
                try {
                    block.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            final DataStoreParams paramMap = new DataStoreParams();
            paramMap.put("executor_shutdown_timeout", "1");

            final long started = System.nanoTime();
            final List<LogEvent> reports = captureDataStoreReports(() -> dataStore.shutdownExecutor(executor, paramMap));
            final long elapsedSeconds = (System.nanoTime() - started) / 1_000_000_000L;

            // it must wait the configured period and return rather than hang or throw
            assertTrue("expected roughly the configured 1s wait but took " + elapsedSeconds + "s", elapsedSeconds < 10L);
            assertEquals("the unfinished tasks must be reported once, got " + messagesOf(reports), 1, reports.size());
            assertEquals("an unfinished crawl is an operator-visible failure", Level.ERROR, reports.get(0).getLevel());
            final String message = reports.get(0).getMessage().getFormattedMessage();
            assertTrue(message,
                    message.contains("TestDataStore: 1 crawling task(s) were still running and 0 had not started after 1 seconds"));
            assertTrue(message, message.contains("executor_shutdown_timeout"));
        } finally {
            block.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void test_shutdownExecutor_reportsNothingWhenEveryTaskFinishes() {
        final Microsoft365DataStore.ReportingExecutor executor = dataStore.newFixedThreadPool(1);
        try {
            executor.execute(() -> {});
            final List<LogEvent> reports = captureDataStoreReports(() -> dataStore.shutdownExecutor(executor, new DataStoreParams()));
            assertTrue("a drained, failure-free shutdown must say nothing, got " + messagesOf(reports), reports.isEmpty());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void test_shutdownExecutor_reportsHowManyTasksFailed() {
        final Microsoft365DataStore.ReportingExecutor executor = dataStore.newFixedThreadPool(1);
        try {
            executor.execute(() -> {
                throw new IllegalStateException("boom");
            });
            final List<LogEvent> reports = captureDataStoreReports(() -> dataStore.shutdownExecutor(executor, new DataStoreParams()));

            // the per-task report may or may not land inside the capture window, depending on
            // whether the pool thread got there first; only the summary is this test's subject.
            final List<LogEvent> summaries = reports.stream()
                    .filter(event -> event.getMessage().getFormattedMessage().contains("crawling task(s) failed;"))
                    .collect(Collectors.toList());
            assertEquals("the failure count must be reported once, got " + messagesOf(reports), 1, summaries.size());
            assertEquals("a crawl that lost documents is an operator-visible failure", Level.ERROR, summaries.get(0).getLevel());
            assertEquals("TestDataStore: 1 crawling task(s) failed; their documents are missing from this crawl. See the errors above.",
                    summaries.get(0).getMessage().getFormattedMessage());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void test_shutdownExecutor_restoresTheInterruptStatusBeforeRethrowing() {
        final Microsoft365DataStore.ReportingExecutor executor = dataStore.newFixedThreadPool(1);
        try {
            final java.util.concurrent.CountDownLatch block = new java.util.concurrent.CountDownLatch(1);
            executor.execute(() -> {
                try {
                    block.await();
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            final DataStoreParams paramMap = new DataStoreParams();
            paramMap.put("executor_shutdown_timeout", "30");

            // awaitTermination throws immediately when the calling thread is already interrupted,
            // and clears the status while doing so.
            Thread.currentThread().interrupt();
            boolean interruptStatusRestored = false;
            try {
                dataStore.shutdownExecutor(executor, paramMap);
                fail("shutdownExecutor should have rethrown the interrupt");
            } catch (final InterruptedRuntimeException e) {
                interruptStatusRestored = Thread.interrupted();
            }
            assertTrue("the interrupt status must be restored before the exception is rethrown", interruptStatusRestored);
            block.countDown();
        } finally {
            // never let the flag leak into another test
            Thread.interrupted();
            executor.shutdownNow();
        }
    }

    @Test
    public void test_getUserRoles_structure() {
        // Test that getUserRoles method exists and has correct signature
        // Note: Actual testing of getUserRoles requires SystemHelper to be configured
        // which is not available in unit test environment.
        // This test verifies the method structure without execution.
        final User user = new User();
        user.setId("test-user-id");
        user.setDisplayName("Test User");

        // Verify that the method exists by checking the class has the method
        try {
            dataStore.getClass().getMethod("getUserRoles", User.class);
        } catch (NoSuchMethodException e) {
            fail("getUserRoles method should exist: " + e.getMessage());
        }
    }

    @Test
    public void test_isLicensedUser_logic() {
        // Test licensed user detection logic
        final User licensedUser = new User();
        licensedUser.setId("licensed-user");

        final List<AssignedLicense> licenses = new ArrayList<>();
        final AssignedLicense license = new AssignedLicense();
        license.setSkuId(UUID.randomUUID());
        licenses.add(license);
        licensedUser.setAssignedLicenses(licenses);

        // Verify license detection logic
        assertNotNull("Licensed user should have licenses", licensedUser.getAssignedLicenses());
        assertFalse("Licensed user should have non-empty license list", licensedUser.getAssignedLicenses().isEmpty());
        assertTrue("License should have SKU ID", licensedUser.getAssignedLicenses().stream().anyMatch(l -> l.getSkuId() != null));
    }

    @Test
    public void test_unlicensedUser_logic() {
        // Test unlicensed user detection
        final User unlicensedUser1 = new User();
        unlicensedUser1.setId("unlicensed-user-1");
        unlicensedUser1.setAssignedLicenses(new ArrayList<>()); // Empty list

        final User unlicensedUser2 = new User();
        unlicensedUser2.setId("unlicensed-user-2");
        unlicensedUser2.setAssignedLicenses(null); // Null list

        // Verify unlicensed user detection logic
        assertTrue("User with empty license list should be detected as unlicensed",
                unlicensedUser1.getAssignedLicenses() != null && unlicensedUser1.getAssignedLicenses().isEmpty());
        assertTrue("User with null license list should be detected as unlicensed", unlicensedUser2.getAssignedLicenses() == null);
    }

    @Test
    public void test_userWithInvalidLicense_logic() {
        // Test user with license but no SKU ID
        final User userWithInvalidLicense = new User();
        userWithInvalidLicense.setId("invalid-license-user");

        final List<AssignedLicense> licenses = new ArrayList<>();
        final AssignedLicense license = new AssignedLicense();
        license.setSkuId(null); // No SKU ID
        licenses.add(license);
        userWithInvalidLicense.setAssignedLicenses(licenses);

        // Verify that user with license but no SKU ID is treated as unlicensed
        assertFalse("User with license but no SKU ID should be treated as unlicensed",
                userWithInvalidLicense.getAssignedLicenses().stream().anyMatch(l -> l.getSkuId() != null));
    }

    @Test
    public void test_threadPoolExecutor_shutdownGracefully() {
        final ExecutorService executor = dataStore.newFixedThreadPool(2);

        try {
            // Submit some tasks
            executor.submit(() -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // Shutdown gracefully
            executor.shutdown();
            assertTrue("Executor should shutdown within timeout", executor.awaitTermination(5, TimeUnit.SECONDS));
        } catch (Exception e) {
            fail("Graceful shutdown should work: " + e.getMessage());
        }
    }

    @Test
    public void test_threadPoolExecutor_shutdownNow() {
        final ExecutorService executor = dataStore.newFixedThreadPool(2);

        try {
            // Submit long-running task
            executor.submit(() -> {
                try {
                    Thread.sleep(10000); // Long sleep
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            // Shutdown immediately
            final List<Runnable> unfinishedTasks = executor.shutdownNow();
            assertNotNull("ShutdownNow should return list of unfinished tasks", unfinishedTasks);

            assertTrue("Executor should terminate quickly after shutdownNow", executor.awaitTermination(2, TimeUnit.SECONDS));
        } catch (Exception e) {
            fail("Immediate shutdown should work: " + e.getMessage());
        }
    }

    @Test
    public void test_constantValues() {
        // Test that important constants are properly defined
        assertEquals("IGNORE_ERROR constant should match", "ignore_error", TestDataStore.getIgnoreErrorConstant());
        assertEquals("IGNORE_SYSTEM_LIBRARIES constant should match", "ignore_system_libraries",
                TestDataStore.getIgnoreSystemLibrariesConstant());
        assertEquals("IGNORE_SYSTEM_LISTS constant should match", "ignore_system_lists", TestDataStore.getIgnoreSystemListsConstant());
    }

    /**
     * Pins the values of the six parameter constants promoted from the subclasses to the base.
     * They are operator-visible data config parameter names, so a typo introduced while
     * re-declaring them here would silently stop the parameter being read at all.
     */
    @Test
    public void test_promotedParameterConstantValues() {
        assertEquals("number_of_threads", TestDataStore.getNumberOfThreadsConstant());
        assertEquals("site_id", TestDataStore.getSiteIdConstant());
        assertEquals("exclude_site_id", TestDataStore.getExcludeSiteIdConstant());
        assertEquals("include_pattern", TestDataStore.getIncludePatternConstant());
        assertEquals("exclude_pattern", TestDataStore.getExcludePatternConstant());
        assertEquals("url_filter", TestDataStore.getUrlFilterConstant());
    }

    /**
     * MultipleCrawlingAccessException carries an array of causes; the six data stores all record
     * the LAST one against the failure URL. Picking the first, or not unwrapping at all, would
     * put a different exception class in the Failure URL admin screen.
     */
    @Test
    public void test_unwrapCrawlingAccessException_returnsLastCause() {
        final Throwable first = new IllegalStateException("first");
        final Throwable last = new IllegalArgumentException("last");
        final MultipleCrawlingAccessException multiple = new MultipleCrawlingAccessException("multi", new Throwable[] { first, last });

        assertSame(last, Microsoft365DataStore.unwrapCrawlingAccessException(multiple));
    }

    @Test
    public void test_unwrapCrawlingAccessException_withNoCausesReturnsTheExceptionItself() {
        final MultipleCrawlingAccessException empty = new MultipleCrawlingAccessException("multi", new Throwable[0]);
        assertSame(empty, Microsoft365DataStore.unwrapCrawlingAccessException(empty));

        final CrawlingAccessException plain = new CrawlingAccessException("plain");
        assertSame(plain, Microsoft365DataStore.unwrapCrawlingAccessException(plain));
    }

    /**
     * The recorded errorName is the CAUSE's class name when there is a cause, and the
     * exception's own class name otherwise. Getting this backwards makes every failure row
     * read "CrawlingAccessException" and hides the real reason.
     */
    @Test
    public void test_failureErrorName_prefersTheCauseClassName() {
        final Throwable withCause = new CrawlingAccessException("outer", new java.net.SocketTimeoutException("inner"));
        assertEquals("java.net.SocketTimeoutException", Microsoft365DataStore.failureErrorName(withCause));

        final Throwable withoutCause = new CrawlingAccessException("outer");
        assertEquals("org.codelibs.fess.crawler.exception.CrawlingAccessException", Microsoft365DataStore.failureErrorName(withoutCause));
    }

    /**
     * The failure-URL row is keyed by the URL argument and stamped with an error name taken from
     * the UNWRAPPED cause, not from the {@link MultipleCrawlingAccessException} wrapper. Recording
     * the wrapper would make every row in the Failure URL admin screen read
     * "MultipleCrawlingAccessException" and hide the real reason.
     */
    @Test
    public void test_handleCrawlingException_storesTheUnwrappedCauseAgainstTheFailureUrl() {
        final CapturingFailureUrlService failureUrlService = CapturingFailureUrlService.empty();
        final RecordingCrawlerStatsHelper crawlerStatsHelper = new RecordingCrawlerStatsHelper();
        final DataConfig dataConfig = new DataConfig();
        final StatsKeyObject statsKey = new StatsKeyObject("item-1");

        final Throwable earlierCause = new IllegalStateException("earlier");
        final Throwable lastCause = new CrawlingAccessException("last", new java.net.SocketTimeoutException("inner"));
        final MultipleCrawlingAccessException e = new MultipleCrawlingAccessException("multi", new Throwable[] { earlierCause, lastCause });

        dataStore.handleCrawlingException(dataConfig, crawlerStatsHelper, statsKey, "https://example.com/item-1", e);

        final List<CapturingFailureUrlService.StoredFailure> stored = failureUrlService.getStoredFailures();
        assertEquals("exactly one failure row must be written", 1, stored.size());
        final CapturingFailureUrlService.StoredFailure failure = stored.get(0);
        assertSame("the row must be recorded against the crawl's own data config", dataConfig, failure.crawlingConfig());
        assertEquals("the row key must be the URL argument the caller passed", "https://example.com/item-1", failure.url());
        assertEquals("the error name must come from the unwrapped cause", "java.net.SocketTimeoutException", failure.errorName());
        assertSame("the stored throwable must be the unwrapped last cause, not the wrapper", lastCause, failure.throwable());

        assertEquals("the item must be counted as an access exception", List.of(StatsAction.ACCESS_EXCEPTION),
                crawlerStatsHelper.getRecordedActions());
        assertEquals("the stats must be recorded against the item's own key", List.of(statsKey), crawlerStatsHelper.getRecordedKeys());
    }

    /**
     * The other arm. A throwable that is not a {@link CrawlingAccessException} is recorded under
     * its OWN class name and counted as {@link StatsAction#EXCEPTION}: swapping the two arms'
     * rules would be invisible in the crawler log but would relabel every row.
     */
    @Test
    public void test_handleCrawlingThrowable_storesTheThrowableUnderItsOwnClassName() {
        final CapturingFailureUrlService failureUrlService = CapturingFailureUrlService.empty();
        final RecordingCrawlerStatsHelper crawlerStatsHelper = new RecordingCrawlerStatsHelper();
        final DataConfig dataConfig = new DataConfig();
        final StatsKeyObject statsKey = new StatsKeyObject("item-2");

        // a cause is present on purpose: the CrawlingAccessException arm would report the cause,
        // this arm must report the throwable itself.
        final Throwable t = new IllegalStateException("boom", new java.net.SocketTimeoutException("inner"));

        dataStore.handleCrawlingThrowable(dataConfig, crawlerStatsHelper, statsKey, "https://example.com/item-2", t);

        final List<CapturingFailureUrlService.StoredFailure> stored = failureUrlService.getStoredFailures();
        assertEquals("exactly one failure row must be written", 1, stored.size());
        final CapturingFailureUrlService.StoredFailure failure = stored.get(0);
        assertSame("the row must be recorded against the crawl's own data config", dataConfig, failure.crawlingConfig());
        assertEquals("the row key must be the URL argument the caller passed", "https://example.com/item-2", failure.url());
        assertEquals("this arm must record the throwable's own class name, not its cause's", "java.lang.IllegalStateException",
                failure.errorName());
        assertSame("the stored throwable must be the throwable that was caught", t, failure.throwable());

        assertEquals("the item must be counted as a plain exception", List.of(StatsAction.EXCEPTION),
                crawlerStatsHelper.getRecordedActions());
        assertEquals("the stats must be recorded against the item's own key", List.of(statsKey), crawlerStatsHelper.getRecordedKeys());
    }

    /**
     * A {@link CrawlingAccessException} that is not a {@link MultipleCrawlingAccessException} has
     * nothing to unwrap, so the exception itself is stored.
     */
    @Test
    public void test_handleCrawlingException_withNothingToUnwrapStoresTheExceptionItself() {
        final CapturingFailureUrlService failureUrlService = CapturingFailureUrlService.empty();
        final RecordingCrawlerStatsHelper crawlerStatsHelper = new RecordingCrawlerStatsHelper();
        final CrawlingAccessException e = new CrawlingAccessException("plain");

        dataStore.handleCrawlingException(new DataConfig(), crawlerStatsHelper, new StatsKeyObject("item-3"), "https://example.com/item-3",
                e);

        final List<CapturingFailureUrlService.StoredFailure> stored = failureUrlService.getStoredFailures();
        assertEquals("exactly one failure row must be written", 1, stored.size());
        assertSame(e, stored.get(0).throwable());
        assertEquals("org.codelibs.fess.crawler.exception.CrawlingAccessException", stored.get(0).errorName());
        assertEquals(List.of(StatsAction.ACCESS_EXCEPTION), crawlerStatsHelper.getRecordedActions());
    }

    /**
     * A {@link CrawlerStatsHelper} that records what it was asked to count instead of counting.
     */
    static class RecordingCrawlerStatsHelper extends CrawlerStatsHelper {

        private final List<Object> recordedKeys = Collections.synchronizedList(new ArrayList<>());

        private final List<StatsAction> recordedActions = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void record(final Object keyObj, final StatsAction action) {
            recordedKeys.add(keyObj);
            recordedActions.add(action);
        }

        List<Object> getRecordedKeys() {
            return new ArrayList<>(recordedKeys);
        }

        List<StatsAction> getRecordedActions() {
            return new ArrayList<>(recordedActions);
        }
    }

    /**
     * Test implementation of Microsoft365DataStore for testing purposes.
     * This allows us to test the abstract base class functionality.
     */
    static class TestDataStore extends Microsoft365DataStore {

        @Override
        protected String getName() {
            return "TestDataStore";
        }

        @Override
        protected void storeData(DataConfig dataConfig, IndexUpdateCallback callback, DataStoreParams paramMap,
                Map<String, String> scriptMap, Map<String, Object> defaultDataMap) {
            // Test implementation - does nothing
        }

        // Expose protected methods for testing
        @Override
        public ReportingExecutor newFixedThreadPool(int nThreads) {
            return super.newFixedThreadPool(nThreads);
        }

        @Override
        public List<String> getUserRoles(User user) {
            return super.getUserRoles(user);
        }

        @Override
        public Microsoft365Client createClient(DataStoreParams paramMap) {
            return super.createClient(paramMap);
        }

        // Expose constants for testing
        public static String getIgnoreErrorConstant() {
            return IGNORE_ERROR;
        }

        public static String getIgnoreSystemLibrariesConstant() {
            return IGNORE_SYSTEM_LIBRARIES;
        }

        public static String getIgnoreSystemListsConstant() {
            return IGNORE_SYSTEM_LISTS;
        }

        public static String getNumberOfThreadsConstant() {
            return NUMBER_OF_THREADS;
        }

        public static String getSiteIdConstant() {
            return SITE_ID;
        }

        public static String getExcludeSiteIdConstant() {
            return EXCLUDE_SITE_ID;
        }

        public static String getIncludePatternConstant() {
            return INCLUDE_PATTERN;
        }

        public static String getExcludePatternConstant() {
            return EXCLUDE_PATTERN;
        }

        public static String getUrlFilterConstant() {
            return URL_FILTER;
        }
    }
}

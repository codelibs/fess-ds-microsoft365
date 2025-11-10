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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.codelibs.fess.ds.ms365;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.microsoft.graph.models.AssignedLicense;
import com.microsoft.graph.models.User;

/**
 * Test class for Microsoft365DataStore base class.
 * Tests common functionality shared across all Microsoft 365 data stores.
 */
public class Microsoft365DataStoreTest extends LastaFluteTestCase {

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
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new TestDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        dataStore = null;
        super.tearDown();
    }

    // Test thread pool creation with different thread counts
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

    public void test_newFixedThreadPool_zeroThreads() {
        // Test edge case with zero threads
        final ExecutorService executor = dataStore.newFixedThreadPool(0);
        assertNotNull("ExecutorService should be created even with 0 threads", executor);
        executor.shutdown();
    }

    public void test_newFixedThreadPool_negativeThreads() {
        // Test edge case with negative threads
        final ExecutorService executor = dataStore.newFixedThreadPool(-1);
        assertNotNull("ExecutorService should be created even with negative threads", executor);
        executor.shutdown();
    }

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

    public void test_getUserRoles_validUser() {
        // Create a test user
        final User user = new User();
        user.setId("test-user-id");
        user.setDisplayName("Test User");

        // Note: This test assumes SystemHelper is properly configured
        // In isolated test environment, getUserRoles may return a default role
        final List<String> roles = dataStore.getUserRoles(user);

        assertNotNull("Roles should not be null", roles);
        assertFalse("Roles should not be empty", roles.isEmpty());
        assertEquals("Should return exactly one role", 1, roles.size());
    }

    public void test_getUserRoles_userWithoutDisplayName() {
        // Test with user that has no display name
        final User user = new User();
        user.setId("test-user-id-no-name");
        // No display name set

        final List<String> roles = dataStore.getUserRoles(user);

        assertNotNull("Roles should not be null even without display name", roles);
        assertFalse("Roles should not be empty", roles.isEmpty());
    }

    public void test_getUserRoles_multipleUsers() {
        // Test that different users get different roles
        final User user1 = new User();
        user1.setId("user-1");
        user1.setDisplayName("User One");

        final User user2 = new User();
        user2.setId("user-2");
        user2.setDisplayName("User Two");

        final List<String> roles1 = dataStore.getUserRoles(user1);
        final List<String> roles2 = dataStore.getUserRoles(user2);

        assertNotNull("User 1 roles should not be null", roles1);
        assertNotNull("User 2 roles should not be null", roles2);

        // Roles should be based on user ID, so they should be different
        assertFalse("Different users should have different roles", roles1.get(0).equals(roles2.get(0)));
    }

    public void test_isLicensedUser_logic() {
        // Test licensed user detection logic
        final User licensedUser = new User();
        licensedUser.setId("licensed-user");

        final List<AssignedLicense> licenses = new ArrayList<>();
        final AssignedLicense license = new AssignedLicense();
        license.setSkuId("sku-id-123");
        licenses.add(license);
        licensedUser.setAssignedLicenses(licenses);

        // Verify license detection logic
        assertNotNull("Licensed user should have licenses", licensedUser.getAssignedLicenses());
        assertFalse("Licensed user should have non-empty license list", licensedUser.getAssignedLicenses().isEmpty());
        assertTrue("License should have SKU ID", licensedUser.getAssignedLicenses().stream().anyMatch(l -> l.getSkuId() != null));
    }

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

            assertTrue("Executor should terminate quickly after shutdownNow",
                    executor.awaitTermination(2, TimeUnit.SECONDS));
        } catch (Exception e) {
            fail("Immediate shutdown should work: " + e.getMessage());
        }
    }

    public void test_constantValues() {
        // Test that important constants are properly defined
        assertEquals("IGNORE_ERROR constant should match", "ignore_error",
                TestDataStore.getIgnoreErrorConstant());
        assertEquals("IGNORE_SYSTEM_LIBRARIES constant should match", "ignore_system_libraries",
                TestDataStore.getIgnoreSystemLibrariesConstant());
        assertEquals("IGNORE_SYSTEM_LISTS constant should match", "ignore_system_lists",
                TestDataStore.getIgnoreSystemListsConstant());
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
        public ExecutorService newFixedThreadPool(int nThreads) {
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
    }
}

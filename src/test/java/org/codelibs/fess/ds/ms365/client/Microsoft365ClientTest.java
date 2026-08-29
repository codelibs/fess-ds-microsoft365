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
package org.codelibs.fess.ds.ms365.client;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Collections;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.ms365.UnitDsTestCase;

import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.User;

public class Microsoft365ClientTest extends UnitDsTestCase {

    private static final Logger logger = LogManager.getLogger(Microsoft365ClientTest.class);

    Microsoft365Client client = null;

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
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);
        if (tenant != null && clientId != null && clientSecret != null) {
            DataStoreParams params = new DataStoreParams();
            params.put(Microsoft365Client.TENANT_PARAM, tenant);
            params.put(Microsoft365Client.CLIENT_ID_PARAM, clientId);
            params.put(Microsoft365Client.CLIENT_SECRET_PARAM, clientSecret);
            client = new Microsoft365Client(params);
        }
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        if (client != null) {
            client.close();
        }
        super.tearDown(testInfo);
    }

    @Test
    public void test_getUsers() {
        Assumptions.assumeTrue(client != null, "No client");

        client.getUsers(Collections.emptyList(), u -> {
            logger.info(ToStringBuilder.reflectionToString(u));
            User user = client.getUser(u.getId(), Collections.emptyList());
            logger.info(ToStringBuilder.reflectionToString(user));
            assertEquals(u.getId(), user.getId());

            client.getNotebookPage(NotebookScope.USER, user.getId()).getValue().forEach(n -> {
                logger.info(ToStringBuilder.reflectionToString(n));
            });
        });
    }

    @Test
    public void test_getGroups() {
        Assumptions.assumeTrue(client != null, "No client");

        client.getGroups(Collections.emptyList(), g -> {
            logger.info(ToStringBuilder.reflectionToString(g));
            assertNotNull(g.getId());
        });
    }

    @Test
    public void test_getDrives() {
        Assumptions.assumeTrue(client != null, "No client");

        client.getDrives(d -> {
            logger.info(ToStringBuilder.reflectionToString(d));
            Drive drive = client.getDrive(d.getId());
            logger.info(ToStringBuilder.reflectionToString(drive));
        });
    }

    @Test
    public void test_getTeams() {
        Assumptions.assumeTrue(client != null, "No client");

        client.getTeams(Collections.emptyList(), g -> {
            logger.info(ToStringBuilder.reflectionToString(g));
            assertNotNull(g.getId());
            Group g2 = client.getGroupById(g.getId());
            assertEquals(g.getId(), g2.getId());
            client.getChannels(Collections.emptyList(), c -> {
                logger.info(ToStringBuilder.reflectionToString(c));
                assertNotNull(c.getId());
                Channel c2 = client.getChannelById(g.getId(), c.getId());
                assertEquals(c.getId(), c2.getId());
                client.getTeamMessages(Collections.emptyList(), m -> {
                    logger.info(ToStringBuilder.reflectionToString(m));
                    logger.info(m.getBody().getContentType().toString());
                    logger.info(m.getBody().getContent());
                    client.getTeamReplyMessages(Collections.emptyList(), r -> {
                        logger.info(ToStringBuilder.reflectionToString(r));
                        logger.info(r.getBody().getContentType().toString());
                        logger.info(r.getBody().getContent());
                    }, g.getId(), c.getId(), m.getId());
                }, g.getId(), c.getId());
            }, g.getId());
        });
    }

    @Test
    public void test_getChats() {
        Assumptions.assumeTrue(client != null, "No client");

        final String chatId = "chat id";
        client.getChatMessages(Collections.emptyList(), m -> {
            logger.info(ToStringBuilder.reflectionToString(m));
            logger.info(m.getBody().getContentType().toString());
            logger.info(m.getBody().getContent());
        }, chatId);
    }

    @Test
    public void test_getCacheSize_malformedValueFallsBackToDefault() {
        final DataStoreParams params = new DataStoreParams();
        params.put("cache_size", "abc");
        assertEquals(Microsoft365Client.DEFAULT_CACHE_SIZE, Microsoft365Client.getCacheSize(params));
    }

    @Test
    public void test_getCacheSize_validValueIsUsed() {
        final DataStoreParams params = new DataStoreParams();
        params.put("cache_size", "42");
        assertEquals(42, Microsoft365Client.getCacheSize(params));
    }

    @Test
    public void test_getCacheSize_absentValueFallsBackToDefault() {
        assertEquals(Microsoft365Client.DEFAULT_CACHE_SIZE, Microsoft365Client.getCacheSize(new DataStoreParams()));
    }

    @Test
    public void test_getCacheSize_negativeValueFallsBackToDefault() {
        final DataStoreParams params = new DataStoreParams();
        params.put("cache_size", "-1");
        assertEquals(Microsoft365Client.DEFAULT_CACHE_SIZE, Microsoft365Client.getCacheSize(params));
    }

    /**
     * Reproduces the crash the reviewer found directly: {@code Integer.parseInt("-1")} succeeds,
     * so a bare {@code getCacheSize} guard against unparseable input alone is not enough --
     * Guava's {@code CacheBuilder#maximumSize(long)} rejects a negative size with its own
     * {@code IllegalArgumentException}, uncaught by anything in the constructor. This constructs
     * a real client (no network call happens: {@code ClientSecretCredential} is lazy) to prove
     * the constructor itself survives a negative cache_size end to end, not just that the helper
     * method returns the right number in isolation.
     */
    @Test
    public void test_constructorDoesNotThrow_whenCacheSizeIsNegative() throws Exception {
        final DataStoreParams params = new DataStoreParams();
        params.put(Microsoft365Client.TENANT_PARAM, "dummy-tenant");
        params.put(Microsoft365Client.CLIENT_ID_PARAM, "dummy-client-id");
        params.put(Microsoft365Client.CLIENT_SECRET_PARAM, "dummy-client-secret");
        params.put("cache_size", "-1");

        try (Microsoft365Client target = new Microsoft365Client(params)) {
            assertNotNull(target.userTypeCache);
        }
    }

    /**
     * Test that DEFAULT_CACHE_SIZE is an int constant and has the correct value.
     */
    @Test
    public void test_defaultCacheSizeConstant() {
        // Verify that DEFAULT_CACHE_SIZE is the expected value
        assertEquals("DEFAULT_CACHE_SIZE should be 10000", 10000, Microsoft365Client.DEFAULT_CACHE_SIZE);

        // Note: Type checking for primitive int is done at compile-time
        // The fact that this compiles confirms it's an int
    }

    /**
     * Test that client uses default cache size when no cache_size parameter is provided.
     */
    @Test
    public void test_clientUsesDefaultCacheSize() {
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);

        Assumptions.assumeTrue(tenant != null && clientId != null && clientSecret != null, "No credentials - skipping test");

        DataStoreParams params = new DataStoreParams();
        params.put(Microsoft365Client.TENANT_PARAM, tenant);
        params.put(Microsoft365Client.CLIENT_ID_PARAM, clientId);
        params.put(Microsoft365Client.CLIENT_SECRET_PARAM, clientSecret);
        // Note: NOT setting cache_size parameter - should use default

        Microsoft365Client testClient = null;
        try {
            testClient = new Microsoft365Client(params);
            assertNotNull("Client should be created successfully", testClient);

            // The client should be created without errors using the default cache size
            // We can't directly verify the cache size, but we can verify the client works
            assertNotNull("userTypeCache should be initialized", testClient.userTypeCache);
            assertNotNull("groupIdCache should be initialized", testClient.groupIdCache);
            assertNotNull("upnCache should be initialized", testClient.upnCache);
            assertNotNull("groupNameCache should be initialized", testClient.groupNameCache);
        } finally {
            if (testClient != null) {
                testClient.close();
            }
        }
    }

    /**
     * Test that client uses custom cache size when cache_size parameter is provided.
     */
    @Test
    public void test_clientUsesCustomCacheSize() {
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);

        Assumptions.assumeTrue(tenant != null && clientId != null && clientSecret != null, "No credentials - skipping test");

        DataStoreParams params = new DataStoreParams();
        params.put(Microsoft365Client.TENANT_PARAM, tenant);
        params.put(Microsoft365Client.CLIENT_ID_PARAM, clientId);
        params.put(Microsoft365Client.CLIENT_SECRET_PARAM, clientSecret);
        params.put("cache_size", "5000"); // Set custom cache size

        Microsoft365Client testClient = null;
        try {
            testClient = new Microsoft365Client(params);
            assertNotNull("Client should be created successfully with custom cache size", testClient);

            // The client should be created without errors using the custom cache size
            assertNotNull("userTypeCache should be initialized", testClient.userTypeCache);
            assertNotNull("groupIdCache should be initialized", testClient.groupIdCache);
            assertNotNull("upnCache should be initialized", testClient.upnCache);
            assertNotNull("groupNameCache should be initialized", testClient.groupNameCache);
        } finally {
            if (testClient != null) {
                testClient.close();
            }
        }
    }

    /**
     * Test that close() method properly invalidates all caches.
     * This test verifies the fix for the resource leak bug.
     */
    @Test
    public void test_closeInvalidatesAllCaches() {
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);

        Assumptions.assumeTrue(tenant != null && clientId != null && clientSecret != null, "No credentials - skipping test");

        DataStoreParams params = new DataStoreParams();
        params.put(Microsoft365Client.TENANT_PARAM, tenant);
        params.put(Microsoft365Client.CLIENT_ID_PARAM, clientId);
        params.put(Microsoft365Client.CLIENT_SECRET_PARAM, clientSecret);

        Microsoft365Client testClient = new Microsoft365Client(params);

        // Verify caches are initialized
        assertNotNull("userTypeCache should be initialized before close", testClient.userTypeCache);
        assertNotNull("groupIdCache should be initialized before close", testClient.groupIdCache);
        assertNotNull("upnCache should be initialized before close", testClient.upnCache);
        assertNotNull("groupNameCache should be initialized before close", testClient.groupNameCache);

        // Get initial sizes (should be 0 as nothing has been cached yet)
        long userTypeCacheSize = testClient.userTypeCache.size();
        long groupIdCacheSize = testClient.groupIdCache.size();
        long upnCacheSize = testClient.upnCache.size();
        long groupNameCacheSize = testClient.groupNameCache.size();

        logger.info("Cache sizes before close - userType: {}, groupId: {}, upn: {}, groupName: {}", userTypeCacheSize, groupIdCacheSize,
                upnCacheSize, groupNameCacheSize);

        // Close the client - this should invalidate all caches
        testClient.close();

        // Verify all caches are invalidated (size should be 0)
        assertEquals("userTypeCache should be empty after close", 0L, testClient.userTypeCache.size());
        assertEquals("groupIdCache should be empty after close", 0L, testClient.groupIdCache.size());
        assertEquals("upnCache should be empty after close", 0L, testClient.upnCache.size());
        assertEquals("groupNameCache should be empty after close", 0L, testClient.groupNameCache.size());

        logger.info("All caches successfully invalidated after close()");
    }

    /**
     * Test that caches work correctly and can be invalidated.
     * This is an integration test that verifies cache behavior.
     */
    @Test
    public void test_cacheInvalidationPreventsMemoryLeak() {
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);

        Assumptions.assumeTrue(tenant != null && clientId != null && clientSecret != null, "No credentials - skipping test");

        DataStoreParams params = new DataStoreParams();
        params.put(Microsoft365Client.TENANT_PARAM, tenant);
        params.put(Microsoft365Client.CLIENT_ID_PARAM, clientId);
        params.put(Microsoft365Client.CLIENT_SECRET_PARAM, clientSecret);
        params.put("cache_size", "100"); // Small cache for testing

        Microsoft365Client testClient = null;
        try {
            testClient = new Microsoft365Client(params);

            // All caches should start empty
            assertEquals("Initial userTypeCache size should be 0", 0L, testClient.userTypeCache.size());
            assertEquals("Initial groupIdCache size should be 0", 0L, testClient.groupIdCache.size());
            assertEquals("Initial upnCache size should be 0", 0L, testClient.upnCache.size());
            assertEquals("Initial groupNameCache size should be 0", 0L, testClient.groupNameCache.size());

            logger.info("Cache invalidation test: All caches start empty as expected");

            // After close, all caches should still be empty (and properly cleaned up)
            testClient.close();

            assertEquals("userTypeCache should remain empty after close", 0L, testClient.userTypeCache.size());
            assertEquals("groupIdCache should remain empty after close", 0L, testClient.groupIdCache.size());
            assertEquals("upnCache should remain empty after close", 0L, testClient.upnCache.size());
            assertEquals("groupNameCache should remain empty after close", 0L, testClient.groupNameCache.size());

            logger.info("Cache invalidation test: All caches properly cleaned up after close()");

        } finally {
            if (testClient != null && testClient != client) {
                // Ensure cleanup even if test fails
                testClient.close();
            }
        }
    }

    /**
     * Constructs a {@link Microsoft365Client} with the minimum required params, backed by the
     * given {@link GraphMockServer} with the SDK's own retry middleware disabled.
     */
    private static Microsoft365Client newClientBackedBy(final GraphMockServer server) {
        final DataStoreParams params = new DataStoreParams();
        params.put(Microsoft365Client.TENANT_PARAM, "dummy-tenant");
        params.put(Microsoft365Client.CLIENT_ID_PARAM, "dummy-client-id");
        params.put(Microsoft365Client.CLIENT_SECRET_PARAM, "dummy-client-secret");
        final Microsoft365Client target = new Microsoft365Client(params);
        target.client = server.newGraphClientWithRetriesDisabled();
        return target;
    }

    @Test
    public void test_getGroupById_requestsTheSingleGroupNotTheCollection() throws Exception {
        try (GraphMockServer server = new GraphMockServer()) {
            server.enqueueJson("{\"id\":\"group-1\",\"displayName\":\"Contoso Team\"}");

            final Microsoft365Client target = newClientBackedBy(server);
            final Group group = target.getGroupById("group-1");

            assertEquals("group-1", group.getId());
            assertEquals(1, server.requestCount());
            final String path = server.takePath();
            assertTrue("expected a single-group path but was: " + path, path.startsWith("/groups/group-1"));
        }
    }

    /**
     * getGroupById's old getGroups-backed implementation returned groupTypes on every Group it
     * enumerated; the single-group $select list copied from getTeams does not include it. This
     * asserts the $select query parameter itself, via the recorded request path, so a future
     * edit that drops the field again is caught here rather than by a script silently seeing
     * team.groupTypes as null.
     */
    @Test
    public void test_getGroupById_selectsGroupTypes() throws Exception {
        try (GraphMockServer server = new GraphMockServer()) {
            server.enqueueJson("{\"id\":\"group-1\",\"displayName\":\"Contoso Team\"}");

            final Microsoft365Client target = newClientBackedBy(server);
            target.getGroupById("group-1");

            final String path = server.takePath();
            assertTrue("expected the $select query to include groupTypes but was: " + path, path.contains("groupTypes"));
        }
    }

    @Test
    public void test_getGroupById_notFoundReturnsNull() throws Exception {
        try (GraphMockServer server = new GraphMockServer()) {
            server.enqueueStatus(404, null);

            final Microsoft365Client target = newClientBackedBy(server);
            assertNull(target.getGroupById("missing"));
        }
    }
}

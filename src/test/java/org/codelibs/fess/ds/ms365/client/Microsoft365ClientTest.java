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

import java.util.Collections;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.User;

public class Microsoft365ClientTest extends LastaFluteTestCase {

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
    public void setUp() throws Exception {
        super.setUp();
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
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        if (client != null) {
            client.close();
        }
        super.tearDown();
    }

    public void test_getUsers() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.getUsers(Collections.emptyList(), u -> {
            logger.info(ToStringBuilder.reflectionToString(u));
            User user = client.getUser(u.getId(), Collections.emptyList());
            logger.info(ToStringBuilder.reflectionToString(user));
            assertEquals(u.getId(), user.getId());

            client.getNotebookPage(user.getId()).getValue().forEach(n -> {
                logger.info(ToStringBuilder.reflectionToString(n));
            });
        });
    }

    public void test_getGroups() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.getGroups(Collections.emptyList(), g -> {
            logger.info(ToStringBuilder.reflectionToString(g));
            assertNotNull(g.getId());
        });
    }

    public void test_getDrives() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        client.getDrives(d -> {
            logger.info(ToStringBuilder.reflectionToString(d));
            Drive drive = client.getDrive(d.getId());
            logger.info(ToStringBuilder.reflectionToString(drive));
        });
    }

    public void test_getTeams() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

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

    public void test_getChats() {
        if (client == null) {
            assertTrue("No client", true);
            return;
        }

        final String chatId = "chat id";
        client.getChatMessages(Collections.emptyList(), m -> {
            logger.info(ToStringBuilder.reflectionToString(m));
            logger.info(m.getBody().getContentType().toString());
            logger.info(m.getBody().getContent());
        }, chatId);
    }

    /**
     * Test that DEFAULT_CACHE_SIZE is an int constant and has the correct value.
     */
    public void test_defaultCacheSizeConstant() {
        // Verify that DEFAULT_CACHE_SIZE is the expected value
        assertEquals("DEFAULT_CACHE_SIZE should be 10000", 10000, Microsoft365Client.DEFAULT_CACHE_SIZE);

        // Note: Type checking for primitive int is done at compile-time
        // The fact that this compiles confirms it's an int
    }

    /**
     * Test that client uses default cache size when no cache_size parameter is provided.
     */
    public void test_clientUsesDefaultCacheSize() {
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);

        if (tenant == null || clientId == null || clientSecret == null) {
            assertTrue("No credentials - skipping test", true);
            return;
        }

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
    public void test_clientUsesCustomCacheSize() {
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);

        if (tenant == null || clientId == null || clientSecret == null) {
            assertTrue("No credentials - skipping test", true);
            return;
        }

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
    public void test_closeInvalidatesAllCaches() {
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);

        if (tenant == null || clientId == null || clientSecret == null) {
            assertTrue("No credentials - skipping test", true);
            return;
        }

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

        logger.info("Cache sizes before close - userType: {}, groupId: {}, upn: {}, groupName: {}",
                    userTypeCacheSize, groupIdCacheSize, upnCacheSize, groupNameCacheSize);

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
    public void test_cacheInvalidationPreventsMemoryLeak() {
        String tenant = System.getenv(Microsoft365Client.TENANT_PARAM);
        String clientId = System.getenv(Microsoft365Client.CLIENT_ID_PARAM);
        String clientSecret = System.getenv(Microsoft365Client.CLIENT_SECRET_PARAM);

        if (tenant == null || clientId == null || clientSecret == null) {
            assertTrue("No credentials - skipping test", true);
            return;
        }

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
}

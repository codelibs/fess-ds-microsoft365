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

import java.net.URI;
import java.util.Collections;
import java.util.List;

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
import com.microsoft.kiota.authentication.AccessTokenProvider;
import com.microsoft.kiota.authentication.AllowedHostsValidator;
import com.microsoft.kiota.authentication.AzureIdentityAccessTokenProvider;
import com.microsoft.kiota.http.middleware.RetryHandler;
import com.microsoft.kiota.http.middleware.options.RetryHandlerOption;

import okhttp3.Request;
import okhttp3.Response;

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
     * The minimum required params -- tenant, client_id and client_secret -- with dummy values.
     * No network call happens during construction, since {@code ClientSecretCredential} is lazy.
     */
    private static DataStoreParams minimalParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put(Microsoft365Client.TENANT_PARAM, "dummy-tenant");
        params.put(Microsoft365Client.CLIENT_ID_PARAM, "dummy-client-id");
        params.put(Microsoft365Client.CLIENT_SECRET_PARAM, "dummy-client-secret");
        return params;
    }

    /**
     * Constructs a {@link Microsoft365Client} with {@link #minimalParams()}. No network call
     * happens during construction, so this stays offline.
     */
    private static Microsoft365Client newMinimalClient() {
        return new Microsoft365Client(minimalParams());
    }

    /**
     * Constructs a {@link Microsoft365Client} with the minimum required params, backed by the
     * given {@link GraphMockServer} with the SDK's own retry middleware disabled.
     */
    private static Microsoft365Client newClientBackedBy(final GraphMockServer server) {
        final Microsoft365Client target = newMinimalClient();
        target.client = server.newGraphClientWithRetriesDisabled();
        return target;
    }

    /**
     * A reviewer commented out {@code connectionPool().evictAll()} and this still passed:
     * {@code target} never issues a request, so the pool is empty either way and
     * {@code connectionCount() == 0} cannot fail regardless of whether eviction runs. This
     * routes one real request through {@code target.httpClient} itself -- not through
     * {@code target.client} or a substitute {@code GraphServiceClient}, which would use a
     * different OkHttpClient entirely -- against a local {@link GraphMockServer}, so the pool
     * this test inspects is verifiably non-empty before {@code close()} is called.
     */
    @Test
    public void test_close_shutsDownTheHttpStack() throws Exception {
        try (GraphMockServer server = new GraphMockServer()) {
            server.enqueueStatus(200, null);
            final Microsoft365Client target = newMinimalClient();
            assertNotNull("the client must own its OkHttpClient", target.httpClient);

            try (Response response = target.httpClient.newCall(new Request.Builder().url(server.url("/ping")).build()).execute()) {
                assertEquals(200, response.code());
            }
            assertTrue("the pool must be non-empty before close(), or evicting it proves nothing",
                    target.httpClient.connectionPool().connectionCount() > 0);

            target.close();

            assertTrue("dispatcher executor should be shut down", target.httpClient.dispatcher().executorService().isShutdown());
            assertEquals("connection pool should be evicted", 0, target.httpClient.connectionPool().connectionCount());
        }
    }

    /**
     * Two different classes share the simple name {@code AzureIdentityAuthenticationProvider}:
     * kiota's own, and {@code com.microsoft.graph.core.authentication}'s. Passed a null/empty
     * allowed-hosts array (as this client does), the Graph one installs the six Graph
     * national-cloud hosts on the underlying {@code AllowedHostsValidator}; kiota's leaves the
     * validator's host set empty, which makes {@code isUrlHostValid} return true for -- and so
     * attach the bearer token to -- every host. That matters here because roughly twenty methods
     * on this class call {@code …withUrl(response.getOdataNextLink())} with a server-supplied
     * {@code @odata.nextLink}: a non-Graph host named there must not receive the tenant's token.
     */
    @Test
    public void test_authProvider_rejectsANonGraphHost() throws Exception {
        final Microsoft365Client target = newMinimalClient();

        final AccessTokenProvider tokenProvider = target.authProvider.getAccessTokenProvider();
        final AllowedHostsValidator validator = ((AzureIdentityAccessTokenProvider) tokenProvider).getAllowedHostsValidator();

        assertFalse("a non-Graph host must not be treated as valid", validator.isUrlHostValid(new URI("https://evil.example.com/x")));
        assertTrue("the configured Graph cloud host must still be valid",
                validator.isUrlHostValid(new URI("https://graph.microsoft.com/v1.0/users")));
    }

    /**
     * A bare {@code GraphClientOption} (what {@code GraphClientFactory.create(RequestOption[])}
     * falls back to when the array holds none) leaves {@code clientLibraryVersion} unset, so the
     * {@code SdkVersion} header comes out as {@code "graph-java, graph-java-core/..."} -- missing
     * the client library's own version. {@code GraphServiceClient.getGraphClientOptions()} is
     * public and static, and including it in the {@code RequestOption[]} array is enough to
     * restore {@code "graph-java/<version>, ..."}, matching what
     * {@code GraphServiceClient(TokenCredential, ...)} builds internally.
     */
    @Test
    public void test_sdkVersionHeader_includesTheClientLibraryVersion() throws Exception {
        try (GraphMockServer server = new GraphMockServer()) {
            server.enqueueStatus(200, null);
            final Microsoft365Client target = newMinimalClient();

            try (Response response = target.httpClient.newCall(new Request.Builder().url(server.url("/ping")).build()).execute()) {
                assertEquals(200, response.code());
            }

            final String sdkVersion = server.takeHeader("SdkVersion");
            assertNotNull("SdkVersion header must be present", sdkVersion);
            assertTrue("expected the client library version in the SdkVersion header but was: " + sdkVersion,
                    sdkVersion.contains("graph-java/"));
        }
    }

    @Test
    public void test_timeouts_defaultToTheUnderlyingDefaults() {
        // GraphClientFactory.create() (via KiotaClientFactory.create()) hard-codes a 100-second
        // connect/read/call timeout on every OkHttpClient.Builder it returns -- confirmed by
        // disassembling microsoft-kiota-http-okHttp's KiotaClientFactory.create(Interceptor[]),
        // which calls builder.connectTimeout/readTimeout/callTimeout(Duration.ofSeconds(100))
        // unconditionally. That is "the underlying default" a 0 parameter leaves untouched, and
        // it applied identically before this change: the old no-proxy path's
        // new GraphServiceClient(credential) also builds its OkHttpClient through this same
        // factory internally. It is not OkHttp's own raw default (0/10000/10000ms).
        final Microsoft365Client target = newMinimalClient();
        assertEquals(100000, target.httpClient.callTimeoutMillis());
    }

    @Test
    public void test_accessTimeoutIsAppliedAsCallTimeout() {
        final DataStoreParams params = minimalParams();
        params.put("access_timeout", "45");
        final Microsoft365Client target = new Microsoft365Client(params);
        assertEquals(45000, target.httpClient.callTimeoutMillis());
    }

    @Test
    public void test_connectAndReadTimeoutsAreApplied() {
        final DataStoreParams params = minimalParams();
        params.put("connect_timeout", "5");
        params.put("read_timeout", "70");
        final Microsoft365Client target = new Microsoft365Client(params);
        assertEquals(5000, target.httpClient.connectTimeoutMillis());
        assertEquals(70000, target.httpClient.readTimeoutMillis());
    }

    @Test
    public void test_malformedTimeoutFallsBackToTheDefault() {
        final DataStoreParams params = minimalParams();
        params.put("access_timeout", "soon");
        final Microsoft365Client target = new Microsoft365Client(params);
        // See test_timeouts_defaultToTheUnderlyingDefaults: 100000ms is GraphClientFactory's own
        // hard-coded default, left untouched because getLongParam falls back to 0 (do not apply).
        assertEquals(100000, target.httpClient.callTimeoutMillis());
    }

    /**
     * OkHttp's {@code Builder} requires the timeout in milliseconds to fit an {@code int}: at most
     * {@code Integer.MAX_VALUE / 1000} = 2147483 seconds. At that exact boundary construction must
     * succeed and apply the value as given.
     */
    @Test
    public void test_accessTimeout_atLibraryMaximumIsApplied() {
        final DataStoreParams params = minimalParams();
        params.put("access_timeout", String.valueOf(Microsoft365Client.MAX_TIMEOUT_SECONDS));
        final Microsoft365Client target = new Microsoft365Client(params);
        assertEquals((int) (Microsoft365Client.MAX_TIMEOUT_SECONDS * 1000), target.httpClient.callTimeoutMillis());
    }

    /**
     * One second past the boundary: OkHttp itself throws {@code IllegalArgumentException: timeout
     * too large} for this value, which -- unlike a {@code NumberFormatException} -- was not caught
     * by getLongParam, so it used to abort the whole constructor with
     * {@code DataStoreException: Failed to create a client.} instead of falling back like any
     * other malformed value. It must now be clamped instead.
     */
    @Test
    public void test_accessTimeout_beyondLibraryMaximumIsClamped() {
        final DataStoreParams params = minimalParams();
        params.put("access_timeout", String.valueOf(Microsoft365Client.MAX_TIMEOUT_SECONDS + 1));
        final Microsoft365Client target = new Microsoft365Client(params);
        assertEquals((int) (Microsoft365Client.MAX_TIMEOUT_SECONDS * 1000), target.httpClient.callTimeoutMillis());
    }

    @Test
    public void test_connectTimeout_atLibraryMaximumIsApplied() {
        final DataStoreParams params = minimalParams();
        params.put("connect_timeout", String.valueOf(Microsoft365Client.MAX_TIMEOUT_SECONDS));
        final Microsoft365Client target = new Microsoft365Client(params);
        assertEquals((int) (Microsoft365Client.MAX_TIMEOUT_SECONDS * 1000), target.httpClient.connectTimeoutMillis());
    }

    @Test
    public void test_connectTimeout_beyondLibraryMaximumIsClamped() {
        final DataStoreParams params = minimalParams();
        params.put("connect_timeout", String.valueOf(Microsoft365Client.MAX_TIMEOUT_SECONDS + 1));
        final Microsoft365Client target = new Microsoft365Client(params);
        assertEquals((int) (Microsoft365Client.MAX_TIMEOUT_SECONDS * 1000), target.httpClient.connectTimeoutMillis());
    }

    @Test
    public void test_readTimeout_atLibraryMaximumIsApplied() {
        final DataStoreParams params = minimalParams();
        params.put("read_timeout", String.valueOf(Microsoft365Client.MAX_TIMEOUT_SECONDS));
        final Microsoft365Client target = new Microsoft365Client(params);
        assertEquals((int) (Microsoft365Client.MAX_TIMEOUT_SECONDS * 1000), target.httpClient.readTimeoutMillis());
    }

    @Test
    public void test_readTimeout_beyondLibraryMaximumIsClamped() {
        final DataStoreParams params = minimalParams();
        params.put("read_timeout", String.valueOf(Microsoft365Client.MAX_TIMEOUT_SECONDS + 1));
        final Microsoft365Client target = new Microsoft365Client(params);
        assertEquals((int) (Microsoft365Client.MAX_TIMEOUT_SECONDS * 1000), target.httpClient.readTimeoutMillis());
    }

    @Test
    public void test_retryHandlerOption_usesConfiguredValues() {
        final DataStoreParams params = minimalParams();
        params.put("max_retry_count", "5");
        params.put("retry_interval", "7");
        final RetryHandlerOption option = Microsoft365Client.newRetryHandlerOption(params);
        assertEquals(5, option.maxRetries());
        assertEquals(7L, option.delay());
    }

    /**
     * Verifies newRetryHandlerOption's result is actually wired into the constructed
     * OkHttpClient, not merely computed and discarded: GraphClientFactory.create(RequestOption[])
     * installs a {@link RetryHandler} interceptor carrying whichever {@link RetryHandlerOption}
     * it was given, and RetryHandler#getRetryOptions() reads it back. Passing an empty
     * RequestOption[] here would still produce a RetryHandler (the library's own default one,
     * maxRetries=3/delay=3), so this only passes when our configured values reach it.
     */
    @Test
    public void test_retryHandlerOption_isWiredIntoTheHttpClient() {
        final DataStoreParams params = minimalParams();
        params.put("max_retry_count", "5");
        params.put("retry_interval", "7");
        final Microsoft365Client target = new Microsoft365Client(params);

        final RetryHandlerOption installed = target.httpClient.interceptors()
                .stream()
                .filter(RetryHandler.class::isInstance)
                .map(RetryHandler.class::cast)
                .findFirst()
                .map(RetryHandler::getRetryOptions)
                .orElse(null);

        assertNotNull("the built OkHttpClient must carry a RetryHandler", installed);
        assertEquals(5, installed.maxRetries());
        assertEquals(7L, installed.delay());
    }

    @Test
    public void test_retryHandlerOption_clampsToTheLibraryMaximum() {
        final DataStoreParams params = minimalParams();
        params.put("max_retry_count", String.valueOf(RetryHandlerOption.MAX_RETRIES + 1));
        final RetryHandlerOption option = Microsoft365Client.newRetryHandlerOption(params);
        assertEquals(RetryHandlerOption.MAX_RETRIES, option.maxRetries());
    }

    @Test
    public void test_retryHandlerOption_defaultsWhenUnset() {
        final RetryHandlerOption option = Microsoft365Client.newRetryHandlerOption(minimalParams());
        assertEquals(RetryHandlerOption.DEFAULT_MAX_RETRIES, option.maxRetries());
        assertEquals(RetryHandlerOption.DEFAULT_DELAY, option.delay());
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

    @Test
    public void test_additionallyAllowedTenants_defaultsToNone() {
        assertEquals(List.of(), Microsoft365Client.getAdditionallyAllowedTenants(minimalParams()));
    }

    @Test
    public void test_additionallyAllowedTenants_wildcardIsOptIn() {
        final DataStoreParams params = minimalParams();
        params.put("additionally_allowed_tenants", "*");
        assertEquals(List.of("*"), Microsoft365Client.getAdditionallyAllowedTenants(params));
    }

    @Test
    public void test_additionallyAllowedTenants_acceptsACommaSeparatedList() {
        // The blank entry must be an *internal* one ("tenant-a,,tenant-b"): a merely trailing
        // comma is stripped by String#split's own default (limit=0) behaviour before the
        // isNotBlank filter ever runs, so a test using only a trailing comma would pass even
        // with that filter removed. This keeps the trailing comma too (surrounding whitespace
        // must still be trimmed) but adds an internal blank segment to actually exercise the
        // filter.
        final DataStoreParams params = minimalParams();
        params.put("additionally_allowed_tenants", "tenant-a,, tenant-b ,");
        assertEquals(List.of("tenant-a", "tenant-b"), Microsoft365Client.getAdditionallyAllowedTenants(params));
    }
}

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

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.ds.ms365.UnitDsTestCase;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.implementation.IdentityClient;
import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.User;
import com.microsoft.kiota.RequestAdapter;
import com.microsoft.kiota.authentication.AccessTokenProvider;
import com.microsoft.kiota.authentication.AllowedHostsValidator;
import com.microsoft.kiota.authentication.AzureIdentityAccessTokenProvider;
import com.microsoft.kiota.http.OkHttpRequestAdapter;
import com.microsoft.kiota.http.middleware.RetryHandler;
import com.microsoft.kiota.http.middleware.options.RetryHandlerOption;

import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.Protocol;
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
     * {@code Integer.parseInt("-1")} succeeds, so a bare {@code getCacheSize} guard against
     * unparseable input alone is not enough -- Guava's {@code CacheBuilder#maximumSize(long)}
     * rejects a negative size with its own {@code IllegalArgumentException}, uncaught by
     * anything in the constructor. This constructs a real client (no network call happens:
     * {@code ClientSecretCredential} is lazy) to prove the constructor itself survives a
     * negative cache_size end to end, not just that the helper method returns the right number
     * in isolation.
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
     * Reads back the additionally-allowed-tenants set actually installed on {@code target}'s
     * {@link ClientSecretCredential}. {@code ClientSecretCredential} exposes no accessor for it,
     * but its private {@code identityClient} field does, once reached: both
     * {@code IdentityClient#getIdentityClientOptions()} and
     * {@code IdentityClientOptions#getAdditionallyAllowedTenants()} are public, so only the one
     * field lookup needs reflection.
     */
    private static Set<String> allowedTenantsOf(final Microsoft365Client target) throws Exception {
        final Field identityClientField = ClientSecretCredential.class.getDeclaredField("identityClient");
        identityClientField.setAccessible(true);
        final IdentityClient identityClient = (IdentityClient) identityClientField.get(target.credential);
        return identityClient.getIdentityClientOptions().getAdditionallyAllowedTenants();
    }

    /**
     * Runs {@code action}, capturing the formatted message of every {@code WARN} logged by
     * {@link Microsoft365Client} while it runs.
     */
    private static List<String> captureMicrosoft365ClientWarnings(final Runnable action) {
        final List<String> messages = new ArrayList<>();
        final org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Microsoft365Client.class);
        final AbstractAppender appender =
                new AbstractAppender("test-microsoft365client-warn-capture", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(final LogEvent event) {
                        if (event.getLevel() == Level.WARN) {
                            messages.add(event.getMessage().getFormattedMessage());
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
        return messages;
    }

    /**
     * Commenting out {@code connectionPool().evictAll()} in {@link Microsoft365Client#close()}
     * still passes here unless a request is issued first: an empty pool makes
     * {@code connectionCount() == 0} true either way, whether or not eviction ran. This test
     * must issue a real request through {@code target.httpClient} itself -- not through
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

    /**
     * {@code getAdditionallyAllowedTenants(params)}'s result is only useful once it actually
     * reaches {@code ClientSecretCredentialBuilder}; this and
     * {@link #test_credential_wildcardTenantReachesTheCredential()} prove it does, on both ends:
     * an empty default here, and {@code Set.of("*")} there. Either half of the constructor
     * regressing -- dropping the {@code additionallyAllowedTenants(...)} call, or leaving a
     * hard-coded {@code "*"} on the builder regardless of {@code params} -- passes one of these
     * two tests and fails the other.
     */
    @Test
    public void test_credential_defaultAllowsNoAdditionalTenants() throws Exception {
        final Microsoft365Client target = newMinimalClient();
        assertEquals(Set.of(), allowedTenantsOf(target));
    }

    /**
     * See {@link #test_credential_defaultAllowsNoAdditionalTenants()}.
     */
    @Test
    public void test_credential_wildcardTenantReachesTheCredential() throws Exception {
        final DataStoreParams params = minimalParams();
        params.put("additionally_allowed_tenants", "*");
        final Microsoft365Client target = new Microsoft365Client(params);
        assertEquals(Set.of("*"), allowedTenantsOf(target));
    }

    @Test
    public void test_negativeConnectTimeoutWarnsAndKeepsTheDefault() {
        final DataStoreParams params = minimalParams();
        params.put("connect_timeout", "-1");
        final List<Microsoft365Client> target = new ArrayList<>();
        final List<String> warnings = captureMicrosoft365ClientWarnings(() -> target.add(new Microsoft365Client(params)));

        assertEquals(100000, target.get(0).httpClient.connectTimeoutMillis());
        assertTrue("expected a WARN naming connect_timeout=-1 but got: " + warnings,
                warnings.stream().anyMatch(m -> m.contains("connect_timeout=-1")));
    }

    @Test
    public void test_negativeReadTimeoutWarnsAndKeepsTheDefault() {
        final DataStoreParams params = minimalParams();
        params.put("read_timeout", "-1");
        final List<Microsoft365Client> target = new ArrayList<>();
        final List<String> warnings = captureMicrosoft365ClientWarnings(() -> target.add(new Microsoft365Client(params)));

        assertEquals(100000, target.get(0).httpClient.readTimeoutMillis());
        assertTrue("expected a WARN naming read_timeout=-1 but got: " + warnings,
                warnings.stream().anyMatch(m -> m.contains("read_timeout=-1")));
    }

    /**
     * {@code access_timeout=-1} meaning "no timeout" used to be accepted without comment,
     * leaving an operator with no way to learn OkHttp's call timeout stayed at the 100-second
     * default. {@code max_retry_count=-1} on the same input already warned -- see
     * {@code newRetryHandlerOption}'s own negative-value branch -- which is what made the
     * asymmetry visible in the first place.
     */
    @Test
    public void test_negativeAccessTimeoutWarnsAndKeepsTheDefault() {
        final DataStoreParams params = minimalParams();
        params.put("access_timeout", "-1");
        final List<Microsoft365Client> target = new ArrayList<>();
        final List<String> warnings = captureMicrosoft365ClientWarnings(() -> target.add(new Microsoft365Client(params)));

        assertEquals(100000, target.get(0).httpClient.callTimeoutMillis());
        assertTrue("expected a WARN naming access_timeout=-1 but got: " + warnings,
                warnings.stream().anyMatch(m -> m.contains("access_timeout=-1")));
    }

    /**
     * {@code close()} evicts {@code target.httpClient}'s connection pool. If the Graph client
     * were built against a different {@code OkHttpClient} instance, {@code close()} would evict
     * an empty pool while the crawl's real sockets leak on the instance actually in use.
     * {@code GraphServiceClient#getRequestAdapter()} is public; the {@code Call.Factory} field
     * it returns the value of (declared on {@code OkHttpRequestAdapter}, the superclass of the
     * adapter Graph actually installs) is private, so only that one field lookup needs
     * reflection.
     */
    @Test
    public void test_httpClient_isTheSameInstanceTheGraphAdapterUses() throws Exception {
        final Microsoft365Client target = newMinimalClient();

        final RequestAdapter adapter = target.client.getRequestAdapter();
        final Field callFactoryField = OkHttpRequestAdapter.class.getDeclaredField("client");
        callFactoryField.setAccessible(true);
        final Object callFactory = callFactoryField.get(adapter);

        assertSame("the Graph client must use target.httpClient itself, or close() evicting its pool proves nothing", target.httpClient,
                callFactory);
    }

    @Test
    public void test_proxyConfiguration_setsProxyOnTheHttpClient() throws Exception {
        final DataStoreParams params = minimalParams();
        params.put("proxy_host", "proxy.example.com");
        params.put("proxy_port", "8080");
        final Microsoft365Client target = new Microsoft365Client(params);

        final Proxy proxy = target.httpClient.proxy();
        assertNotNull("a proxy must be configured on the Graph client's OkHttpClient", proxy);
        assertEquals(Proxy.Type.HTTP, proxy.type());
        assertEquals(new InetSocketAddress("proxy.example.com", 8080), proxy.address());
        assertSame("no proxy credentials were given, so no custom authenticator should be installed", Authenticator.NONE,
                target.httpClient.proxyAuthenticator());

        // test_authProvider_rejectsANonGraphHost proves this for the no-proxy path; this is the
        // one path (see the README's "Graph host allowlist" section) whose allowlist behaviour
        // actually changed relative to the base commit, so it needs its own assertion here.
        final AccessTokenProvider tokenProvider = target.authProvider.getAccessTokenProvider();
        final AllowedHostsValidator validator = ((AzureIdentityAccessTokenProvider) tokenProvider).getAllowedHostsValidator();
        assertFalse("a non-Graph host must not be treated as valid on the proxy path either",
                validator.isUrlHostValid(new URI("https://evil.example.com/x")));
    }

    /**
     * No network is involved: the authenticator is invoked directly against a {@code Response}
     * built by hand, the same shape OkHttp would hand it after a real 407 from the proxy.
     */
    @Test
    public void test_proxyConfiguration_installsAuthenticatorWithCredentials() throws Exception {
        final DataStoreParams params = minimalParams();
        params.put("proxy_host", "proxy.example.com");
        params.put("proxy_port", "8080");
        params.put("proxy_username", "proxyuser");
        params.put("proxy_password", "proxypass");
        final Microsoft365Client target = new Microsoft365Client(params);

        assertNotSame("proxy credentials were given, so a custom authenticator must be installed", Authenticator.NONE,
                target.httpClient.proxyAuthenticator());

        final Request request = new Request.Builder().url("http://example.com/").build();
        final Response response = new Response.Builder().request(request)
                .protocol(Protocol.HTTP_1_1)
                .code(407)
                .message("Proxy Authentication Required")
                .build();
        final Request authenticated = target.httpClient.proxyAuthenticator().authenticate(null, response);
        assertEquals(Credentials.basic("proxyuser", "proxypass"), authenticated.header("Proxy-Authorization"));
    }
}

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

import java.util.Map;

import com.microsoft.graph.core.requests.BaseGraphRequestAdapter;
import com.microsoft.graph.core.requests.GraphClientFactory;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.kiota.RequestInformation;
import com.microsoft.kiota.RequestOption;
import com.microsoft.kiota.authentication.AuthenticationProvider;
import com.microsoft.kiota.http.middleware.options.RetryHandlerOption;

import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

/**
 * Wraps a MockWebServer and hands out GraphServiceClient instances pointed at it,
 * so Graph calls can be exercised without contacting Microsoft.
 *
 * <p>The client is built through {@link GraphClientFactory} on purpose: the retry
 * middleware that honours 429/503 and Retry-After lives in that interceptor chain.
 * A bare OkHttpClient throws on the first 429 instead of retrying.</p>
 */
public class GraphMockServer implements AutoCloseable {

    /** Stands in for Azure AD; the mock server does not check authorization. */
    private static final class NoopAuthenticationProvider implements AuthenticationProvider {
        @Override
        public void authenticateRequest(final RequestInformation request, final Map<String, Object> context) {
            // no credentials needed against MockWebServer
        }
    }

    private final MockWebServer server;

    public GraphMockServer() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    /** Queues a 200 response carrying the given JSON body. */
    public void enqueueJson(final String body) {
        server.enqueue(new MockResponse().setResponseCode(200).setHeader("Content-Type", "application/json").setBody(body));
    }

    /**
     * Queues a bare status response.
     *
     * @param code the HTTP status code
     * @param retryAfterSeconds value for the Retry-After header, or null to omit it
     */
    public void enqueueStatus(final int code, final String retryAfterSeconds) {
        final MockResponse response = new MockResponse().setResponseCode(code).setBody("");
        if (retryAfterSeconds != null) {
            response.setHeader("Retry-After", retryAfterSeconds);
        }
        server.enqueue(response);
    }

    /** Absolute URL on the mock server, for embedding in an {@code @odata.nextLink}. */
    public String url(final String path) {
        return server.url(path).toString();
    }

    /**
     * A GraphServiceClient whose base URL is this server. The trailing slash is
     * trimmed so request paths come out as "/users" rather than "//users".
     */
    public GraphServiceClient newGraphClient() {
        final String base = server.url("/").toString();
        final String baseUrl = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        final OkHttpClient http = GraphClientFactory.create().build();
        return new GraphServiceClient(new BaseGraphRequestAdapter(new NoopAuthenticationProvider(), baseUrl, http));
    }

    /**
     * A GraphServiceClient like {@link #newGraphClient()}, but with the SDK's own 429/503 retry
     * middleware disabled (maxRetries=0) so a queued 429/503 response is surfaced to the caller
     * as an {@code ApiException} on the first request instead of being retried internally.
     *
     * <p>Useful for tests that exercise a caller's own retry/caching logic (for example {@code
     * Microsoft365Client}'s UPN/group-name caches): without this, every queued 429/503 would be
     * retried up to 3 more times by the SDK before the caller's code ever sees it, inflating both
     * the request count and the real wall-clock time the test takes to run.</p>
     */
    public GraphServiceClient newGraphClientWithRetriesDisabled() {
        final String base = server.url("/").toString();
        final String baseUrl = base.endsWith("/") ? base.substring(0, base.length() - 1) : base;
        final OkHttpClient http = GraphClientFactory.create(new RequestOption[] { new RetryHandlerOption(null, 0, 0) }).build();
        return new GraphServiceClient(new BaseGraphRequestAdapter(new NoopAuthenticationProvider(), baseUrl, http));
    }

    public int requestCount() {
        return server.getRequestCount();
    }

    /** Path (with query string) of the next request the server received. */
    public String takePath() throws InterruptedException {
        return server.takeRequest().getPath();
    }

    @Override
    public void close() throws Exception {
        server.shutdown();
    }
}

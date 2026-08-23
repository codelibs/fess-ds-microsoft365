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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

/**
 * Covers the principal-name caches. These resolve the object IDs in an ACL to the
 * UPN or group name that Fess roles are usually keyed by; when they silently return
 * null, documents become unreachable for users whose roles use those names.
 */
public class Microsoft365ClientCacheTest {

    /** The mock server does not authenticate and ClientSecretCredential is lazy, so this stays offline. */
    private static DataStoreParams dummyParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put("tenant", "dummy-tenant");
        params.put("client_id", "dummy-client-id");
        params.put("client_secret", "dummy-client-secret");
        return params;
    }

    @Test
    public void test_tryResolveUserPrincipalName_resolvesFromGraph() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{\"id\":\"oid-1\",\"userPrincipalName\":\"alice@example.com\",\"mail\":\"alice@example.com\"}");
            client.client = mock.newGraphClient();

            assertEquals("alice@example.com", client.tryResolveUserPrincipalName("oid-1"));
            assertEquals(1, mock.requestCount(), "the loader must actually call Graph");
        }
    }

    @Test
    public void test_tryResolveUserPrincipalName_cachesResult() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{\"id\":\"oid-1\",\"userPrincipalName\":\"alice@example.com\"}");
            client.client = mock.newGraphClient();

            assertEquals("alice@example.com", client.tryResolveUserPrincipalName("oid-1"));
            assertEquals("alice@example.com", client.tryResolveUserPrincipalName("oid-1"));
            assertEquals(1, mock.requestCount(), "the second call must be served from the cache");
        }
    }

    @Test
    public void test_tryResolveUserPrincipalName_cachesUnresolved() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            // 404: the object ID does not name a user. Graph SDK turns this into an ApiException,
            // the loader returns "unresolved", and that outcome must be cached too -- otherwise
            // every ACL entry for a deleted principal re-queries Graph on every document.
            mock.enqueueStatus(404, null);
            client.client = mock.newGraphClient();

            assertNull(client.tryResolveUserPrincipalName("oid-missing"));
            assertNull(client.tryResolveUserPrincipalName("oid-missing"));
            assertEquals(1, mock.requestCount(), "an unresolved id must not be re-queried");
        }
    }

    @Test
    public void test_tryResolveUserPrincipalName_shortCircuitsOnEmail() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            client.client = mock.newGraphClient();

            assertEquals("bob@example.com", client.tryResolveUserPrincipalName("bob@example.com"));
            assertEquals(0, mock.requestCount(), "a value that is already a UPN needs no lookup");
        }
    }

    @Test
    public void test_tryResolveUserPrincipalName_blankIsNull() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            client.client = mock.newGraphClient();

            assertNull(client.tryResolveUserPrincipalName(null));
            assertNull(client.tryResolveUserPrincipalName(""));
            assertEquals(0, mock.requestCount());
        }
    }

    @Test
    public void test_tryResolveGroupName_resolvesFromGraph() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            // doResolveGroupName prefers mail over mailNickname over displayName, so with a
            // mail present the resolved name is the mail address, not the displayName.
            mock.enqueueJson("{\"id\":\"gid-1\",\"displayName\":\"Sales\",\"mail\":\"sales@example.com\"}");
            client.client = mock.newGraphClient();

            assertEquals("sales@example.com", client.tryResolveGroupName("gid-1"));
            assertEquals(1, mock.requestCount(), "the loader must actually call Graph");
        }
    }

    @Test
    public void test_tryResolveGroupName_cachesUnresolved() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(404, null);
            client.client = mock.newGraphClient();

            assertNull(client.tryResolveGroupName("gid-missing"));
            assertNull(client.tryResolveGroupName("gid-missing"));
            assertEquals(1, mock.requestCount(), "an unresolved id must not be re-queried");
        }
    }
}

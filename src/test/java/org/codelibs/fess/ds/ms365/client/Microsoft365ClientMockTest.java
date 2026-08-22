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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

import com.microsoft.graph.models.User;

/**
 * Exercises Microsoft365Client against a mock Graph endpoint. These paths --
 * pagination and throttling -- have no other test coverage, and a break in them
 * loses documents silently.
 */
public class Microsoft365ClientMockTest {

    /** Credentials are never used: the mock server does not authenticate, and
     *  ClientSecretCredential acquires tokens lazily, so construction is offline. */
    private static DataStoreParams dummyParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put("tenant", "dummy-tenant");
        params.put("client_id", "dummy-client-id");
        params.put("client_secret", "dummy-client-secret");
        return params;
    }

    @Test
    public void test_getUsers_followsNextLink() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            final String nextLink = mock.url("/users?$skiptoken=PAGE2");
            mock.enqueueJson("{\"@odata.nextLink\":\"" + nextLink + "\",\"value\":[{\"id\":\"u1\",\"displayName\":\"Alice\"}]}");
            mock.enqueueJson("{\"value\":[{\"id\":\"u2\",\"displayName\":\"Bob\"}]}");

            client.client = mock.newGraphClient();

            final List<User> users = new ArrayList<>();
            client.getUsers(Collections.emptyList(), users::add);

            assertEquals(2, users.size(), "second page must be collected");
            assertEquals("Alice", users.get(0).getDisplayName());
            assertEquals("Bob", users.get(1).getDisplayName());
            assertEquals(2, mock.requestCount());
            assertTrue(mock.takePath().startsWith("/users"));
            assertEquals("/users?$skiptoken=PAGE2", mock.takePath());
        }
    }

    @Test
    public void test_getUsers_retriesOnThrottling() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(429, "1");
            mock.enqueueJson("{\"value\":[{\"id\":\"u1\",\"displayName\":\"Alice\"}]}");

            client.client = mock.newGraphClient();

            final List<User> users = new ArrayList<>();
            client.getUsers(Collections.emptyList(), users::add);

            assertEquals(1, users.size(), "the retried request must be consumed");
            assertEquals(2, mock.requestCount(), "429 must be retried, not surfaced");
        }
    }

    @Test
    public void test_getUsers_retriesOnServiceUnavailable() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(503, "1");
            mock.enqueueJson("{\"value\":[{\"id\":\"u1\",\"displayName\":\"Alice\"}]}");

            client.client = mock.newGraphClient();

            final List<User> users = new ArrayList<>();
            client.getUsers(Collections.emptyList(), users::add);

            assertEquals(1, users.size());
            assertEquals(2, mock.requestCount(), "503 must be retried");
        }
    }
}

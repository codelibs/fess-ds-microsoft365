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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.User;
import com.microsoft.graph.models.UserCollectionResponse;

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

    /**
     * Pins where the SDK actually puts {@code resourceProvisioningOptions}, because a wrong answer
     * here is what silently emptied Teams crawls: {@code Group} declares the property, so Kiota
     * registers a field deserializer for it and routes the value through
     * {@code setResourceProvisioningOptions}. {@code additionalData} only ever receives properties
     * the model does <em>not</em> declare, so reading the option list from that map can never
     * succeed. Should a future SDK drop the typed property, this test goes red and points at
     * {@code isTeamAllowedByProvisioningOptions} before Teams indexing quietly returns to zero
     * documents.
     */
    @Test
    public void test_graphSdk_deserializesResourceProvisioningOptionsAsATypedProperty() throws Exception {
        try (GraphMockServer mock = new GraphMockServer()) {
            mock.enqueueJson("{\"id\":\"g1\",\"displayName\":\"Group One\",\"resourceProvisioningOptions\":[\"Team\"]}");

            final Group group = mock.newGraphClient().groups().byGroupId("g1").get();

            assertEquals(List.of("Team"), group.getResourceProvisioningOptions(),
                    "the typed accessor is where the deserialized value lands");
            assertFalse(group.getAdditionalData().containsKey("resourceProvisioningOptions"),
                    "a declared property never reaches additionalData");
        }
    }

    /**
     * The regression this whole fix exists for: a group whose {@code resourceProvisioningOptions}
     * contains "Team" must be handed to the consumer. Reading the list from {@code additionalData}
     * made {@code getTeams} accept nothing at all, so Teams crawling indexed zero documents while
     * logging only one DEBUG line per team.
     */
    @Test
    public void test_getTeams_acceptsGroupWhoseOptionsContainTeam() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            // strict FIFO: the /teams listing first, then one /groups/{id} lookup per team
            mock.enqueueJson("{\"value\":[{\"id\":\"t1\",\"displayName\":\"Team One\"}]}");
            mock.enqueueJson("{\"id\":\"t1\",\"displayName\":\"Team One\",\"resourceProvisioningOptions\":[\"Team\"]}");

            client.client = mock.newGraphClient();

            final List<Group> groups = new ArrayList<>();
            client.getTeams(Collections.emptyList(), groups::add);

            assertEquals(1, groups.size(), "a group carrying \"Team\" must reach the consumer");
            assertEquals("Team One", groups.get(0).getDisplayName());
        }
    }

    /**
     * The only rejection the gate performs: the list is present and says this group backs some
     * other workload (an Exchange-only group), so it is not a Team.
     */
    @Test
    public void test_getTeams_skipsGroupWhoseOptionsArePresentWithoutTeam() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{\"value\":[{\"id\":\"t1\",\"displayName\":\"Not A Team\"}]}");
            mock.enqueueJson("{\"id\":\"t1\",\"displayName\":\"Not A Team\",\"resourceProvisioningOptions\":[\"Exchange\"]}");

            client.client = mock.newGraphClient();

            final List<Group> groups = new ArrayList<>();
            client.getTeams(Collections.emptyList(), groups::add);

            assertTrue(groups.isEmpty(), "a present list without \"Team\" must be rejected");
        }
    }

    /**
     * Absent is not a rejection. The /teams endpoint was chosen precisely so that old teams whose
     * backing group never had {@code resourceProvisioningOptions} stamped are still crawled;
     * rejecting them here would defeat that choice.
     */
    @Test
    public void test_getTeams_acceptsGroupWithoutResourceProvisioningOptions() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{\"value\":[{\"id\":\"t1\",\"displayName\":\"Legacy Team\"}]}");
            mock.enqueueJson("{\"id\":\"t1\",\"displayName\":\"Legacy Team\"}");

            client.client = mock.newGraphClient();

            final List<Group> groups = new ArrayList<>();
            client.getTeams(Collections.emptyList(), groups::add);

            assertEquals(1, groups.size(), "an absent option list must not exclude a team");
            assertEquals("Legacy Team", groups.get(0).getDisplayName());
        }
    }

    /** An explicitly empty list carries no evidence against team-ness either. */
    @Test
    public void test_getTeams_acceptsGroupWithEmptyResourceProvisioningOptions() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{\"value\":[{\"id\":\"t1\",\"displayName\":\"Empty Options Team\"}]}");
            mock.enqueueJson("{\"id\":\"t1\",\"displayName\":\"Empty Options Team\",\"resourceProvisioningOptions\":[]}");

            client.client = mock.newGraphClient();

            final List<Group> groups = new ArrayList<>();
            client.getTeams(Collections.emptyList(), groups::add);

            assertEquals(1, groups.size(), "an empty option list must not exclude a team");
        }
    }

    /**
     * paginate must consume every page and stop exactly when the next link is absent. A helper
     * that stopped one page early would drop documents and drop roles from ACLs with no error
     * at all, which is why this is asserted directly rather than only through a mock server.
     */
    @Test
    public void test_paginate_consumesEveryPageAndStopsAtTheLastOne() {
        final UserCollectionResponse page1 = new UserCollectionResponse();
        page1.setValue(List.of(namedUser("u1"), namedUser("u2")));
        page1.setOdataNextLink("https://graph.example/next-2");

        final UserCollectionResponse page2 = new UserCollectionResponse();
        page2.setValue(List.of(namedUser("u3")));
        page2.setOdataNextLink(null);

        final List<String> requestedLinks = new ArrayList<>();
        final List<String> seen = new ArrayList<>();

        Microsoft365Client.paginate(page1, UserCollectionResponse::getValue, link -> {
            requestedLinks.add(link);
            return page2;
        }, user -> seen.add(user.getId()));

        assertEquals(List.of("u1", "u2", "u3"), seen);
        assertEquals(List.of("https://graph.example/next-2"), requestedLinks, "exactly one refetch, with the page-1 next link");
    }

    /**
     * Graph returns an empty string rather than omitting @odata.nextLink in some responses.
     * Treating "" as a link would refetch the same page forever.
     */
    @Test
    public void test_paginate_treatsAnEmptyNextLinkAsTheEnd() {
        final UserCollectionResponse only = new UserCollectionResponse();
        only.setValue(List.of(namedUser("u1")));
        only.setOdataNextLink("");

        final List<String> seen = new ArrayList<>();
        Microsoft365Client.paginate(only, UserCollectionResponse::getValue, link -> {
            throw new AssertionError("must not refetch on an empty next link");
        }, user -> seen.add(user.getId()));

        assertEquals(List.of("u1"), seen);
    }

    /**
     * A null first response, or a page whose value list is null, must terminate rather than
     * throw -- the hand-written loops all guarded on both.
     */
    @Test
    public void test_paginate_toleratesNullResponseAndNullValue() {
        final List<String> seen = new ArrayList<>();

        Microsoft365Client.paginate((UserCollectionResponse) null, UserCollectionResponse::getValue, link -> new UserCollectionResponse(),
                user -> seen.add(user.getId()));

        final UserCollectionResponse nullValue = new UserCollectionResponse();
        nullValue.setValue(null);
        Microsoft365Client.paginate(nullValue, UserCollectionResponse::getValue, link -> new UserCollectionResponse(),
                user -> seen.add(user.getId()));

        assertTrue(seen.isEmpty());
    }

    private static User namedUser(final String id) {
        final User user = new User();
        user.setId(id);
        return user;
    }
}

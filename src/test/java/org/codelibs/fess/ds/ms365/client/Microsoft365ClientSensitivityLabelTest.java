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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

import com.microsoft.graph.models.SensitivityLabel;
import com.microsoft.graph.models.SensitivityLabelAssignment;
import com.microsoft.graph.models.SensitivityLabelAssignmentMethod;

import okhttp3.mockwebserver.RecordedRequest;

/**
 * Exercises the two Graph calls behind sensitivity label support against a mock Graph endpoint:
 * reading the labels assigned to a drive item, and reading - through a cache - the definition of
 * a label. A transient failure cached as "not readable" would make every file carrying that label
 * fall back to the failure policy for the rest of the crawl.
 */
public class Microsoft365ClientSensitivityLabelTest {

    private static final String LABEL_ID = "0e7d7d2b-1c3e-4f5a-9b8c-1234567890ab";
    private static final String TENANT_ID = "11111111-2222-4333-8444-555555555555";

    /** The mock server does not authenticate and ClientSecretCredential is lazy, so this stays offline. */
    private static DataStoreParams dummyParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put("tenant", "dummy-tenant");
        params.put("client_id", "dummy-client-id");
        params.put("client_secret", "dummy-client-secret");
        return params;
    }

    private static String labelJson() {
        return "{\"id\":\"" + LABEL_ID + "\",\"name\":\"Confidential\",\"displayName\":\"Confidential - All Employees\","
                + "\"hasProtection\":true}";
    }

    // ===== extractSensitivityLabels =====

    /**
     * The SDK deserializes the action's response body directly as
     * {@code ExtractSensitivityLabelsResult}: {@code labels} sits at the top level of the body,
     * not inside an OData {@code value} wrapper.
     */
    @Test
    public void test_extractSensitivityLabels_parsesLabels() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(
                    "{\"@odata.context\":\"https://graph.microsoft.com/v1.0/$metadata#microsoft.graph.extractSensitivityLabelsResult\","
                            + "\"labels\":[{\"sensitivityLabelId\":\"" + LABEL_ID + "\",\"assignmentMethod\":\"standard\",\"tenantId\":\""
                            + TENANT_ID
                            + "\"},{\"sensitivityLabelId\":\"ffffffff-0000-4000-8000-000000000001\",\"assignmentMethod\":\"privileged\","
                            + "\"tenantId\":\"" + TENANT_ID + "\"}]}");
            client.client = mock.newGraphClient();

            final List<SensitivityLabelAssignment> labels = client.extractSensitivityLabels("drive-1", "item-1");

            assertEquals(2, labels.size());
            assertEquals(LABEL_ID, labels.get(0).getSensitivityLabelId());
            assertEquals(SensitivityLabelAssignmentMethod.Standard, labels.get(0).getAssignmentMethod());
            assertEquals(TENANT_ID, labels.get(0).getTenantId());
            assertEquals(SensitivityLabelAssignmentMethod.Privileged, labels.get(1).getAssignmentMethod());

            final RecordedRequest request = mock.takeRequest();
            assertEquals("POST", request.getMethod());
            assertEquals("/drives/drive-1/items/item-1/extractSensitivityLabels", request.getPath());
        }
    }

    @Test
    public void test_extractSensitivityLabels_unlabeledFileIsEmpty() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{\"labels\":[]}");
            mock.enqueueJson("{}");
            client.client = mock.newGraphClient();

            assertEquals(List.of(), client.extractSensitivityLabels("drive-1", "item-1"));
            assertEquals(List.of(), client.extractSensitivityLabels("drive-1", "item-1"), "a missing labels property is no labels");
        }
    }

    /**
     * Pins the response shape the SDK expects: a body that wraps the result in an OData
     * {@code value} property is not unwrapped, so its labels are not seen. Should Graph ever
     * answer in that shape, this test documents why every file would look unlabeled.
     */
    @Test
    public void test_extractSensitivityLabels_valueWrappedBodyIsNotUnwrapped() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{\"value\":{\"labels\":[{\"sensitivityLabelId\":\"" + LABEL_ID + "\",\"assignmentMethod\":\"standard\"}]}}");
            client.client = mock.newGraphClient();

            assertEquals(List.of(), client.extractSensitivityLabels("drive-1", "item-1"));
        }
    }

    // ===== getSensitivityLabel =====

    @Test
    public void test_getSensitivityLabel_returnsAndCachesDefinition() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(labelJson());
            client.client = mock.newGraphClient();

            final SensitivityLabel label = client.getSensitivityLabel(LABEL_ID);
            assertNotNull(label);
            assertEquals("Confidential", label.getName());
            assertEquals("Confidential - All Employees", label.getDisplayName());
            assertEquals(Boolean.TRUE, label.getHasProtection());

            final SensitivityLabel cached = client.getSensitivityLabel(LABEL_ID);
            assertEquals("Confidential", cached.getName());
            assertEquals(1, mock.requestCount(), "the second call must be served from the cache");

            final String path = URLDecoder.decode(mock.takePath(), StandardCharsets.UTF_8);
            assertTrue(path.startsWith("/security/dataSecurityAndGovernance/sensitivityLabels/" + LABEL_ID + "?"), path);
            for (final String field : new String[] { "id", "name", "displayName", "hasProtection" }) {
                assertTrue(path.matches(".*\\$select=([a-zA-Z]+,)*" + field + "(,[a-zA-Z]+)*(&.*)?"), field + " in " + path);
            }
        }
    }

    @Test
    public void test_getSensitivityLabel_blankIdMakesNoRequest() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            client.client = mock.newGraphClient();

            assertNull(client.getSensitivityLabel(null));
            assertNull(client.getSensitivityLabel(" "));
            assertEquals(0, mock.requestCount());
        }
    }

    @Test
    public void test_getSensitivityLabel_notFoundIsCached() throws Exception {
        assertNullAndCached(404);
    }

    @Test
    public void test_getSensitivityLabel_forbiddenIsCached() throws Exception {
        assertNullAndCached(403);
    }

    @Test
    public void test_getSensitivityLabel_unauthorizedIsCached() throws Exception {
        assertNullAndCached(401);
    }

    private static void assertNullAndCached(final int status) throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(status, null);
            mock.enqueueJson(labelJson());
            client.client = mock.newGraphClientWithRetriesDisabled();

            assertNull(client.getSensitivityLabel(LABEL_ID));
            assertNull(client.getSensitivityLabel(LABEL_ID), "a " + status + " must be remembered, not re-queried");
            assertEquals(1, mock.requestCount(), "a " + status + " must be cached");
        }
    }

    @Test
    public void test_getSensitivityLabel_serviceUnavailableIsNotCached() throws Exception {
        assertNullAndNotCached(503);
    }

    @Test
    public void test_getSensitivityLabel_serverErrorIsNotCached() throws Exception {
        assertNullAndNotCached(500);
    }

    @Test
    public void test_getSensitivityLabel_throttlingIsNotCached() throws Exception {
        assertNullAndNotCached(429);
    }

    private static void assertNullAndNotCached(final int status) throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(status, null);
            mock.enqueueJson(labelJson());
            client.client = mock.newGraphClientWithRetriesDisabled();

            assertNull(client.getSensitivityLabel(LABEL_ID));
            assertEquals(1, mock.requestCount());

            final SensitivityLabel label = client.getSensitivityLabel(LABEL_ID);
            assertNotNull(label, "a transient " + status + " must not be cached as unreadable");
            assertEquals("Confidential", label.getName());
            assertEquals(2, mock.requestCount(), "the second call must reach Graph again");
        }
    }

    @Test
    public void test_getSensitivityLabel_cachesPerLabelId() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(404, null);
            mock.enqueueJson(labelJson());
            client.client = mock.newGraphClientWithRetriesDisabled();

            assertNull(client.getSensitivityLabel("ffffffff-0000-4000-8000-000000000001"));
            assertNotNull(client.getSensitivityLabel(LABEL_ID), "another label's 404 must not be served for this one");
            assertEquals(2, mock.requestCount());
        }
    }
}

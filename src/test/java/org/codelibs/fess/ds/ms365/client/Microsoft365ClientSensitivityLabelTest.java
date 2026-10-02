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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import org.codelibs.fess.ds.ms365.client.Microsoft365Client.SensitivityLabelEntry;
import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

import com.microsoft.graph.models.SensitivityLabelAssignment;
import com.microsoft.graph.models.SensitivityLabelAssignmentMethod;
import com.microsoft.kiota.ApiException;

import okhttp3.mockwebserver.RecordedRequest;

/**
 * Exercises the two Graph calls behind sensitivity label support against a mock Graph endpoint:
 * reading the labels assigned to a drive item, and reading the tenant's label catalog - labels
 * and their sublabels, loaded once - that resolves a label ID to its definition and parent. A
 * transient failure cached as "not readable" would make every labeled file fall back to the
 * failure policy for the rest of the crawl.
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

    // ===== extractSensitivityLabels =====

    /** The shape the SDK's own model expects: {@code labels} at the top level of the body. */
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
            mock.enqueueJson("{\"value\":{\"labels\":[]}}");
            client.client = mock.newGraphClient();

            assertEquals(List.of(), client.extractSensitivityLabels("drive-1", "item-1"));
            assertEquals(List.of(), client.extractSensitivityLabels("drive-1", "item-1"), "an empty value-wrapped list is no labels");
        }
    }

    /**
     * Microsoft's reference shows the result wrapped in an OData {@code value} property, while the
     * SDK's own model expects it at the top level; both shapes must yield the labels.
     */
    @Test
    public void test_extractSensitivityLabels_unwrapsValueWrappedBody() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(
                    "{\"@odata.context\":\"https://graph.microsoft.com/v1.0/$metadata#microsoft.graph.extractSensitivityLabelsResult\","
                            + "\"value\":{\"labels\":[{\"sensitivityLabelId\":\"" + LABEL_ID
                            + "\",\"assignmentMethod\":\"standard\",\"tenantId\":\"" + TENANT_ID + "\"}]}}");
            client.client = mock.newGraphClient();

            final List<SensitivityLabelAssignment> labels = client.extractSensitivityLabels("drive-1", "item-1");
            assertEquals(1, labels.size());
            assertEquals(LABEL_ID, labels.get(0).getSensitivityLabelId());
            assertEquals(SensitivityLabelAssignmentMethod.Standard, labels.get(0).getAssignmentMethod());
            assertEquals(TENANT_ID, labels.get(0).getTenantId());

            final RecordedRequest request = mock.takeRequest();
            assertEquals("POST", request.getMethod());
            assertEquals("/drives/drive-1/items/item-1/extractSensitivityLabels", request.getPath());
        }
    }

    /** A response without labels is not an unlabeled file: reading it as one would bypass the label's rules. */
    @Test
    public void test_extractSensitivityLabels_missingLabelsPropertyThrows() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{}");
            mock.enqueueJson("{\"value\":{}}");
            mock.enqueueJson("{\"value\":null}");
            client.client = mock.newGraphClient();

            for (int i = 0; i < 3; i++) {
                final IllegalStateException e =
                        assertThrows(IllegalStateException.class, () -> client.extractSensitivityLabels("drive-1", "item-1"));
                assertTrue(e.getMessage().contains("item-1"), e.getMessage());
            }
            assertEquals(3, mock.requestCount());
        }
    }

    @Test
    public void test_extractSensitivityLabels_errorStatusThrowsApiException() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(403, null);
            client.client = mock.newGraphClientWithRetriesDisabled();

            final ApiException e = assertThrows(ApiException.class, () -> client.extractSensitivityLabels("drive-1", "item-1"));
            assertEquals(403, e.getResponseStatusCode());
        }
    }

    // ===== getSensitivityLabel =====

    private static final String PARENT_ID = "aaaaaaaa-0000-4000-8000-000000000001";
    private static final String PUBLIC_ID = "aaaaaaaa-0000-4000-8000-000000000002";
    private static final String SUBLABEL_ID = "BBBBBBBB-0000-4000-8000-000000000003";
    private static final String OTHER_SUBLABEL_ID = "bbbbbbbb-0000-4000-8000-000000000004";
    private static final String LABELS_PATH = "/security/dataSecurityAndGovernance/sensitivityLabels";

    private static String labelJson(final String id, final String name, final String displayName, final boolean hasProtection) {
        return "{\"id\":\"" + id + "\",\"name\":\"" + name + "\",\"displayName\":\"" + displayName + "\",\"hasProtection\":" + hasProtection
                + "}";
    }

    private static String page(final String nextLink, final String... labels) {
        return "{" + (nextLink != null ? "\"@odata.nextLink\":\"" + nextLink + "\"," : "") + "\"value\":[" + String.join(",", labels)
                + "]}";
    }

    private static String parentJson() {
        return labelJson(PARENT_ID, "Confidential", "Confidential", false);
    }

    private static String publicJson() {
        return labelJson(PUBLIC_ID, "Public", "Public", false);
    }

    private static String sublabelJson() {
        return labelJson(SUBLABEL_ID, "conf-all", "All Employees", true);
    }

    /** Queues a catalog of two top-level labels, the first with one sublabel. */
    private static void enqueueCatalog(final GraphMockServer mock) {
        mock.enqueueJson(page(null, parentJson(), publicJson()));
        mock.enqueueJson(page(null, sublabelJson()));
        mock.enqueueJson(page(null));
    }

    private static String decodedPath(final GraphMockServer mock) throws InterruptedException {
        return URLDecoder.decode(mock.takePath(), StandardCharsets.UTF_8);
    }

    private static void assertSelects(final String path) {
        for (final String field : new String[] { "id", "name", "displayName", "hasProtection" }) {
            assertTrue(path.matches(".*\\$select=([a-zA-Z]+,)*" + field + "(,[a-zA-Z]+)*(&.*)?"), field + " in " + path);
        }
    }

    @Test
    public void test_getSensitivityLabel_loadsCatalogOnceWithSublabels() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            enqueueCatalog(mock);
            client.client = mock.newGraphClient();

            final SensitivityLabelEntry sublabel = client.getSensitivityLabel(SUBLABEL_ID);
            assertNotNull(sublabel);
            assertEquals("conf-all", sublabel.label().getName());
            assertEquals("All Employees", sublabel.label().getDisplayName());
            assertEquals(Boolean.TRUE, sublabel.label().getHasProtection());
            assertNotNull(sublabel.parent(), "a sublabel must carry its parent");
            assertEquals(PARENT_ID, sublabel.parent().getId());
            assertEquals("Confidential", sublabel.parent().getName());

            final SensitivityLabelEntry parent = client.getSensitivityLabel(PARENT_ID);
            assertEquals("Confidential", parent.label().getName());
            assertNull(parent.parent());
            assertEquals("Public", client.getSensitivityLabel(PUBLIC_ID).label().getName());
            assertNull(client.getSensitivityLabel("ffffffff-0000-4000-8000-00000000dead"), "an unknown label has no definition");
            assertEquals(3, mock.requestCount(), "the catalog is loaded once: the list plus one sublabel list per top-level label");

            final String listPath = decodedPath(mock);
            assertTrue(listPath.startsWith(LABELS_PATH + "?"), listPath);
            assertSelects(listPath);
            final String sublabelPath = decodedPath(mock);
            assertTrue(sublabelPath.startsWith(LABELS_PATH + "/" + PARENT_ID + "/sublabels?"), sublabelPath);
            assertSelects(sublabelPath);
            assertTrue(decodedPath(mock).startsWith(LABELS_PATH + "/" + PUBLIC_ID + "/sublabels"));
        }
    }

    @Test
    public void test_getSensitivityLabel_lookupIgnoresCase() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            enqueueCatalog(mock);
            client.client = mock.newGraphClient();

            assertNotNull(client.getSensitivityLabel(SUBLABEL_ID.toLowerCase(Locale.ROOT)));
            assertNotNull(client.getSensitivityLabel(SUBLABEL_ID.toUpperCase(Locale.ROOT)));
            assertNotNull(client.getSensitivityLabel(PARENT_ID.toUpperCase(Locale.ROOT)));
        }
    }

    @Test
    public void test_getSensitivityLabel_followsNextLinks() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            final String listNext = mock.url(LABELS_PATH + "?$skiptoken=LIST2");
            final String subNext = mock.url(LABELS_PATH + "/" + PARENT_ID + "/sublabels?$skiptoken=SUB2");
            mock.enqueueJson(page(listNext, parentJson()));
            mock.enqueueJson(page(null, publicJson()));
            mock.enqueueJson(page(subNext, sublabelJson()));
            mock.enqueueJson(page(null, labelJson(OTHER_SUBLABEL_ID, "conf-partners", "Partners", true)));
            mock.enqueueJson(page(null));
            client.client = mock.newGraphClient();

            assertEquals(PARENT_ID, client.getSensitivityLabel(OTHER_SUBLABEL_ID).parent().getId(), "second sublabel page");
            assertNull(client.getSensitivityLabel(PUBLIC_ID).parent(), "second label page");
            assertEquals(5, mock.requestCount());
            mock.takePath();
            assertEquals(LABELS_PATH + "?$skiptoken=LIST2", decodedPath(mock));
            mock.takePath();
            assertEquals(LABELS_PATH + "/" + PARENT_ID + "/sublabels?$skiptoken=SUB2", decodedPath(mock));
        }
    }

    @Test
    public void test_getSensitivityLabel_sublabelOverwritesTopLevelEntry() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            // the list also returns the sublabel at the top level
            mock.enqueueJson(page(null, sublabelJson(), parentJson()));
            mock.enqueueJson(page(null));
            mock.enqueueJson(page(null, sublabelJson()));
            client.client = mock.newGraphClient();

            final SensitivityLabelEntry entry = client.getSensitivityLabel(SUBLABEL_ID);
            assertNotNull(entry.parent(), "the sublabel entry must win over the top-level one");
            assertEquals(PARENT_ID, entry.parent().getId());
            assertEquals(3, mock.requestCount());
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
    public void test_getSensitivityLabel_forbiddenIsCached() throws Exception {
        assertListFailureCached(403);
    }

    @Test
    public void test_getSensitivityLabel_unauthorizedIsCached() throws Exception {
        assertListFailureCached(401);
    }

    private static void assertListFailureCached(final int status) throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(status, null);
            enqueueCatalog(mock);
            client.client = mock.newGraphClientWithRetriesDisabled();

            assertNull(client.getSensitivityLabel(PARENT_ID));
            assertNull(client.getSensitivityLabel(SUBLABEL_ID), "a " + status + " must be remembered, not re-queried");
            assertEquals(1, mock.requestCount(), "a " + status + " must be cached as an empty catalog");
        }
    }

    @Test
    public void test_getSensitivityLabel_serviceUnavailableIsNotCached() throws Exception {
        assertListFailureNotCached(503);
    }

    @Test
    public void test_getSensitivityLabel_serverErrorIsNotCached() throws Exception {
        assertListFailureNotCached(500);
    }

    @Test
    public void test_getSensitivityLabel_throttlingIsNotCached() throws Exception {
        assertListFailureNotCached(429);
    }

    @Test
    public void test_getSensitivityLabel_notFoundListIsCachedAsEmpty() throws Exception {
        // A cloud that does not offer the endpoint answers 404 for good; reloading for every file
        // would add one request and one WARN per labeled file.
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(404, null);
            enqueueCatalog(mock);
            client.client = mock.newGraphClientWithRetriesDisabled();

            assertNull(client.getSensitivityLabel(SUBLABEL_ID));
            assertNull(client.getSensitivityLabel(SUBLABEL_ID));
            assertEquals(1, mock.requestCount(), "a persistent 4xx must not be retried");
        }
    }

    private static void assertListFailureNotCached(final int status) throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueStatus(status, null);
            enqueueCatalog(mock);
            client.client = mock.newGraphClientWithRetriesDisabled();

            assertNull(client.getSensitivityLabel(SUBLABEL_ID));
            assertEquals(1, mock.requestCount());

            final SensitivityLabelEntry entry = client.getSensitivityLabel(SUBLABEL_ID);
            assertNotNull(entry, "a transient " + status + " must not be cached as unreadable");
            assertEquals(PARENT_ID, entry.parent().getId());
            assertEquals(4, mock.requestCount(), "the second call must reload the catalog");
        }
    }

    @Test
    public void test_getSensitivityLabel_sublabelListErrorSkipsOnlyThatLabel() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(page(null, parentJson(), publicJson()));
            mock.enqueueStatus(500, null);
            mock.enqueueJson(page(null, labelJson(OTHER_SUBLABEL_ID, "public-ext", "External", false)));
            client.client = mock.newGraphClientWithRetriesDisabled();

            assertNull(client.getSensitivityLabel(SUBLABEL_ID), "the sublabels of the failed label are unknown");
            assertNotNull(client.getSensitivityLabel(PARENT_ID), "the failed label itself is still known");
            assertEquals(PUBLIC_ID, client.getSensitivityLabel(OTHER_SUBLABEL_ID).parent().getId(), "other labels' sublabels still load");
            assertEquals(3, mock.requestCount(), "the catalog is cached despite the skipped sublabel list");
        }
    }

    @Test
    public void test_getSensitivityLabel_sublabelListForbiddenEmptiesCatalog() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(page(null, parentJson(), publicJson()));
            mock.enqueueStatus(403, null);
            enqueueCatalog(mock);
            client.client = mock.newGraphClientWithRetriesDisabled();

            assertNull(client.getSensitivityLabel(PARENT_ID), "a 403 on a sublabel list empties the whole catalog");
            assertNull(client.getSensitivityLabel(PUBLIC_ID));
            assertEquals(2, mock.requestCount(), "the empty catalog is cached; the remaining sublabel list is not requested");
        }
    }

    @Test
    public void test_close_resetsCatalog() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            enqueueCatalog(mock);
            enqueueCatalog(mock);
            client.client = mock.newGraphClient();

            assertNotNull(client.getSensitivityLabel(PARENT_ID));
            assertNotNull(client.sensitivityLabelCatalog);
            client.close();
            assertNull(client.sensitivityLabelCatalog, "close() must forget the catalog");

            client.client = mock.newGraphClient();
            assertNotNull(client.getSensitivityLabel(PARENT_ID));
            assertEquals(6, mock.requestCount(), "the catalog is loaded again after close()");
        }
    }
}

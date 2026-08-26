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

import org.codelibs.fess.entity.DataStoreParams;
import org.junit.jupiter.api.Test;

/**
 * Covers which Graph path each notebook scope resolves to.
 *
 * <p>Site and group notebooks used to be requested by concatenating "sites/" or
 * "groups/" onto the id and passing it where a user id was expected, so every
 * request went to /users/sites%2F... and came back 404. The 404 was logged at
 * debug level, so both crawlers reported success with nothing indexed.</p>
 */
public class Microsoft365ClientOneNoteTest {

    private static DataStoreParams dummyParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put("tenant", "dummy-tenant");
        params.put("client_id", "dummy-client-id");
        params.put("client_secret", "dummy-client-secret");
        return params;
    }

    private static final String NOTEBOOKS_JSON = "{\"value\":[{\"id\":\"nb1\",\"displayName\":\"Notebook One\"}]}";

    @Test
    public void test_getNotebookPage_siteScopeUsesSitesPath() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(NOTEBOOKS_JSON);
            client.client = mock.newGraphClient();

            client.getNotebookPage(NotebookScope.SITE, "site-1");

            final String path = mock.takePath();
            assertTrue(path.startsWith("/sites/site-1/onenote/notebooks"), "site notebooks must come from /sites/, got " + path);
        }
    }

    @Test
    public void test_getNotebookPage_groupScopeUsesGroupsPath() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(NOTEBOOKS_JSON);
            client.client = mock.newGraphClient();

            client.getNotebookPage(NotebookScope.GROUP, "group-1");

            final String path = mock.takePath();
            assertTrue(path.startsWith("/groups/group-1/onenote/notebooks"), "group notebooks must come from /groups/, got " + path);
        }
    }

    @Test
    public void test_getNotebookPage_userScopeUsesUsersPath() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(NOTEBOOKS_JSON);
            client.client = mock.newGraphClient();

            client.getNotebookPage(NotebookScope.USER, "user-1");

            final String path = mock.takePath();
            assertTrue(path.startsWith("/users/user-1/onenote/notebooks"), "user notebooks must come from /users/, got " + path);
        }
    }

    @Test
    public void test_getNotebookPage_neverEncodesScopeIntoTheOwnerId() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(NOTEBOOKS_JSON);
            client.client = mock.newGraphClient();

            client.getNotebookPage(NotebookScope.SITE, "site-1");

            final String path = mock.takePath();
            assertEquals(-1, path.indexOf("sites%2F"), "the scope must not be smuggled into the id: " + path);
            assertEquals(-1, path.indexOf("/users/sites"), "site notebooks must not be requested as a user: " + path);
        }
    }

    @Test
    public void test_getNotebookContent_siteScopeUsesSitesPath() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            // sections for the notebook, then no pages for that section
            mock.enqueueJson("{\"value\":[{\"id\":\"sec1\",\"displayName\":\"Section One\"}]}");
            mock.enqueueJson("{\"value\":[]}");
            client.client = mock.newGraphClient();

            client.getNotebookContent(NotebookScope.SITE, "site-1", "nb1");

            final String sectionsPath = mock.takePath();
            assertTrue(sectionsPath.startsWith("/sites/site-1/onenote/notebooks/nb1/sections"),
                    "sections must follow the same scope as the notebook, got " + sectionsPath);
        }
    }
}

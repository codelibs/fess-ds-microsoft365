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

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.ms365.UnitDsTestCase;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Covers which Graph path each notebook scope resolves to.
 *
 * <p>Site and group notebooks used to be requested by concatenating "sites/" or
 * "groups/" onto the id and passing it where a user id was expected, so every
 * request went to /users/sites%2F... and came back 404. The 404 was logged at
 * debug level, so both crawlers reported success with nothing indexed.</p>
 *
 * <p>Listing notebooks was not the only place with this bug: sections, pages and
 * page content all branched the same way, so the full-chain tests below drive
 * {@link Microsoft365Client#getNotebookContent(NotebookScope, String, String)} all
 * the way through to the page-content request, not just the first hop.</p>
 *
 * <p>This extends {@link UnitDsTestCase} (rather than being a bare JUnit test, like
 * the sibling {@code Microsoft365ClientMockTest}) because {@code getPageContents}
 * routes the page body through {@code ComponentUtil.getExtractorFactory()}, which
 * needs a real DI container plus a registered {@code tikaExtractor} to resolve.
 * Note {@code UnitDsTestCase} redeclares JUnit4-style {@code assertTrue}/{@code
 * assertEquals}/{@code assertFalse} overloads (message first) that shadow the
 * statically-imported JUnit5 ones, so this class follows that same message-first
 * convention rather than importing {@code Assertions.assertTrue} et al.</p>
 */
public class Microsoft365ClientOneNoteTest extends UnitDsTestCase {

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        // getPageContents() runs the page body through the extractor factory; without a
        // real extractor registered, extraction itself throws before the scope-routing
        // under test ever gets a chance to matter.
        final TikaExtractor tikaExtractor = new TikaExtractor();
        tikaExtractor.init();
        ComponentUtil.register(tikaExtractor, "tikaExtractor");
    }

    @Override
    public void tearDown(final TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

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
            assertTrue("site notebooks must come from /sites/, got " + path, path.startsWith("/sites/site-1/onenote/notebooks"));
        }
    }

    @Test
    public void test_getNotebookPage_groupScopeUsesGroupsPath() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(NOTEBOOKS_JSON);
            client.client = mock.newGraphClient();

            client.getNotebookPage(NotebookScope.GROUP, "group-1");

            final String path = mock.takePath();
            assertTrue("group notebooks must come from /groups/, got " + path, path.startsWith("/groups/group-1/onenote/notebooks"));
        }
    }

    @Test
    public void test_getNotebookPage_userScopeUsesUsersPath() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(NOTEBOOKS_JSON);
            client.client = mock.newGraphClient();

            client.getNotebookPage(NotebookScope.USER, "user-1");

            final String path = mock.takePath();
            assertTrue("user notebooks must come from /users/, got " + path, path.startsWith("/users/user-1/onenote/notebooks"));
        }
    }

    @Test
    public void test_getNotebookPage_neverEncodesScopeIntoTheOwnerId() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson(NOTEBOOKS_JSON);
            client.client = mock.newGraphClient();

            client.getNotebookPage(NotebookScope.SITE, "site-1");

            final String path = mock.takePath();
            assertEquals("the scope must not be smuggled into the id: " + path, -1, path.indexOf("sites%2F"));
            assertEquals("site notebooks must not be requested as a user: " + path, -1, path.indexOf("/users/sites"));
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
            assertTrue("sections must follow the same scope as the notebook, got " + sectionsPath,
                    sectionsPath.startsWith("/sites/site-1/onenote/notebooks/nb1/sections"));
        }
    }

    /**
     * Drives {@code getNotebookContent} all the way through sections, pages and page
     * content for a SITE-scoped notebook, asserting every hop's request path. The
     * previous test above only reached the sections call (its second fixture answered
     * with zero pages), which is exactly the gap that let the SITE/GROUP branches of
     * {@code getPages} and {@code getPageContents} go untested.
     */
    @Test
    public void test_getNotebookContent_siteScopeFullChainUsesSitesPathThroughout() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{\"value\":[{\"id\":\"sec1\",\"displayName\":\"Section One\"}]}");
            mock.enqueueJson("{\"value\":[{\"id\":\"page1\",\"title\":\"Page One\"}]}");
            mock.enqueueContent("text/html", "<html><body>Site page body</body></html>");
            client.client = mock.newGraphClient();

            final String content = client.getNotebookContent(NotebookScope.SITE, "site-1", "nb1");

            assertEquals("sections, pages and page content must each be requested once", 3, mock.requestCount());

            final String sectionsPath = mock.takePath();
            assertTrue("sections must come from /sites/, got " + sectionsPath,
                    sectionsPath.startsWith("/sites/site-1/onenote/notebooks/nb1/sections"));

            final String pagesPath = mock.takePath();
            assertTrue("pages must come from /sites/, got " + pagesPath, pagesPath.startsWith("/sites/site-1/onenote/sections/sec1/pages"));
            assertFalse("site pages must not be requested as a user: " + pagesPath, pagesPath.startsWith("/users/"));

            final String contentPath = mock.takePath();
            assertTrue("page content must come from /sites/, got " + contentPath,
                    contentPath.startsWith("/sites/site-1/onenote/pages/page1/content"));
            assertFalse("site page content must not be requested as a user: " + contentPath, contentPath.startsWith("/users/"));

            assertTrue("section content must be included: " + content, content.contains("Section One"));
            assertTrue("page content must be included: " + content, content.contains("Page One"));
        }
    }

    /**
     * Same as the SITE full-chain test above, but for GROUP. Before this test, GROUP
     * coverage stopped at {@code getNotebookPage} (the listing call); sections, pages and
     * page content for groups had no coverage at all.
     */
    @Test
    public void test_getNotebookContent_groupScopeFullChainUsesGroupsPathThroughout() throws Exception {
        try (GraphMockServer mock = new GraphMockServer(); Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            mock.enqueueJson("{\"value\":[{\"id\":\"sec1\",\"displayName\":\"Section One\"}]}");
            mock.enqueueJson("{\"value\":[{\"id\":\"page1\",\"title\":\"Page One\"}]}");
            mock.enqueueContent("text/html", "<html><body>Group page body</body></html>");
            client.client = mock.newGraphClient();

            final String content = client.getNotebookContent(NotebookScope.GROUP, "group-1", "nb1");

            assertEquals("sections, pages and page content must each be requested once", 3, mock.requestCount());

            final String sectionsPath = mock.takePath();
            assertTrue("sections must come from /groups/, got " + sectionsPath,
                    sectionsPath.startsWith("/groups/group-1/onenote/notebooks/nb1/sections"));

            final String pagesPath = mock.takePath();
            assertTrue("pages must come from /groups/, got " + pagesPath,
                    pagesPath.startsWith("/groups/group-1/onenote/sections/sec1/pages"));
            assertFalse("group pages must not be requested as a user: " + pagesPath, pagesPath.startsWith("/users/"));

            final String contentPath = mock.takePath();
            assertTrue("page content must come from /groups/, got " + contentPath,
                    contentPath.startsWith("/groups/group-1/onenote/pages/page1/content"));
            assertFalse("group page content must not be requested as a user: " + contentPath, contentPath.startsWith("/users/"));

            assertTrue("section content must be included: " + content, content.contains("Section One"));
            assertTrue("page content must be included: " + content, content.contains("Page One"));
        }
    }

    @Test
    public void test_getNotebookPage_requiresOwnerIdForSiteAndGroupScopes() throws Exception {
        try (Microsoft365Client client = new Microsoft365Client(dummyParams())) {
            assertThrows(IllegalArgumentException.class, () -> client.getNotebookPage(NotebookScope.SITE, null));
            assertThrows(IllegalArgumentException.class, () -> client.getNotebookPage(NotebookScope.GROUP, null));
        }
    }
}

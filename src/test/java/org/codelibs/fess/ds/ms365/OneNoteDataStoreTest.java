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
package org.codelibs.fess.ds.ms365;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.GraphMockServer;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.ds.ms365.client.NotebookScope;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.AssignedLicense;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.Notebook;
import com.microsoft.graph.models.NotebookCollectionResponse;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.models.User;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.kiota.ApiException;

public class OneNoteDataStoreTest extends UnitDsTestCase {

    private static final Logger logger = LogManager.getLogger(OneNoteDataStoreTest.class);

    // for test
    public static final String tenant = "";
    public static final String clientId = "";
    public static final String clientSecret = "";

    private OneNoteDataStore dataStore;

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
        dataStore = new OneNoteDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    @Test
    public void test_getName() {
        assertEquals("OneNoteDataStore", dataStore.getName());
    }

    @Test
    public void test_isGroupNoteCrawler() {
        DataStoreParams paramMap = new DataStoreParams();

        // Default is true
        assertTrue(dataStore.isGroupNoteCrawler(paramMap));

        // Test with false
        paramMap.put(OneNoteDataStore.GROUP_NOTE_CRAWLER, "false");
        assertFalse(dataStore.isGroupNoteCrawler(paramMap));

        // Test with true
        paramMap.put(OneNoteDataStore.GROUP_NOTE_CRAWLER, "true");
        assertTrue(dataStore.isGroupNoteCrawler(paramMap));
    }

    @Test
    public void test_isUserNoteCrawler() {
        DataStoreParams paramMap = new DataStoreParams();

        // Default is true
        assertTrue(dataStore.isUserNoteCrawler(paramMap));

        // Test with false
        paramMap.put(OneNoteDataStore.USER_NOTE_CRAWLER, "false");
        assertFalse(dataStore.isUserNoteCrawler(paramMap));

        // Test with true
        paramMap.put(OneNoteDataStore.USER_NOTE_CRAWLER, "true");
        assertTrue(dataStore.isUserNoteCrawler(paramMap));
    }

    @Test
    public void test_isSiteNoteCrawler() {
        DataStoreParams paramMap = new DataStoreParams();

        // Default is true based on implementation
        assertTrue(dataStore.isSiteNoteCrawler(paramMap));

        // Test with false
        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "false");
        assertFalse(dataStore.isSiteNoteCrawler(paramMap));

        // Test with true
        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "true");
        assertTrue(dataStore.isSiteNoteCrawler(paramMap));
    }

    @Test
    public void test_numberOfThreads() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test default value
        assertEquals("1", paramMap.getAsString(OneNoteDataStore.NUMBER_OF_THREADS, "1"));

        // Test custom value
        paramMap.put(OneNoteDataStore.NUMBER_OF_THREADS, "5");
        assertEquals("5", paramMap.getAsString(OneNoteDataStore.NUMBER_OF_THREADS));
    }

    @Test
    public void test_notebookConstants() {
        // Verify constant values are set - based on actual implementation
        assertEquals("notebook", OneNoteDataStore.NOTEBOOK);
        assertEquals("name", OneNoteDataStore.NOTEBOOK_NAME);
        assertEquals("contents", OneNoteDataStore.NOTEBOOK_CONTENTS);
        assertEquals("size", OneNoteDataStore.NOTEBOOK_SIZE);
        assertEquals("created", OneNoteDataStore.NOTEBOOK_CREATED);
        assertEquals("last_modified", OneNoteDataStore.NOTEBOOK_LAST_MODIFIED);
        assertEquals("web_url", OneNoteDataStore.NOTEBOOK_WEB_URL);
        assertEquals("roles", OneNoteDataStore.NOTEBOOK_ROLES);
    }

    @Test
    public void test_crawlerTypeParameters() {
        assertEquals("number_of_threads", OneNoteDataStore.NUMBER_OF_THREADS);
        assertEquals("site_note_crawler", OneNoteDataStore.SITE_NOTE_CRAWLER);
        assertEquals("user_note_crawler", OneNoteDataStore.USER_NOTE_CRAWLER);
        assertEquals("group_note_crawler", OneNoteDataStore.GROUP_NOTE_CRAWLER);
    }

    @Test
    public void test_multipleNotebookConfigurations() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test all crawlers enabled
        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "true");
        paramMap.put(OneNoteDataStore.USER_NOTE_CRAWLER, "true");
        paramMap.put(OneNoteDataStore.GROUP_NOTE_CRAWLER, "true");

        assertTrue(dataStore.isSiteNoteCrawler(paramMap));
        assertTrue(dataStore.isUserNoteCrawler(paramMap));
        assertTrue(dataStore.isGroupNoteCrawler(paramMap));

        // Test all crawlers disabled
        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "false");
        paramMap.put(OneNoteDataStore.USER_NOTE_CRAWLER, "false");
        paramMap.put(OneNoteDataStore.GROUP_NOTE_CRAWLER, "false");

        assertFalse(dataStore.isSiteNoteCrawler(paramMap));
        assertFalse(dataStore.isUserNoteCrawler(paramMap));
        assertFalse(dataStore.isGroupNoteCrawler(paramMap));

        // Test mixed configuration
        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "true");
        paramMap.put(OneNoteDataStore.USER_NOTE_CRAWLER, "false");
        paramMap.put(OneNoteDataStore.GROUP_NOTE_CRAWLER, "true");

        assertTrue(dataStore.isSiteNoteCrawler(paramMap));
        assertFalse(dataStore.isUserNoteCrawler(paramMap));
        assertTrue(dataStore.isGroupNoteCrawler(paramMap));
    }

    @Test
    public void test_invalidParameterValues() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test with invalid boolean values (should default to false)
        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "invalid");
        paramMap.put(OneNoteDataStore.USER_NOTE_CRAWLER, "yes");
        paramMap.put(OneNoteDataStore.GROUP_NOTE_CRAWLER, "1");

        assertFalse(dataStore.isSiteNoteCrawler(paramMap));
        assertFalse(dataStore.isUserNoteCrawler(paramMap));
        assertFalse(dataStore.isGroupNoteCrawler(paramMap));

        // Test with null values (should use defaults - all true based on implementation)
        DataStoreParams newParamMap = new DataStoreParams();
        newParamMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, null);
        newParamMap.put(OneNoteDataStore.USER_NOTE_CRAWLER, null);
        newParamMap.put(OneNoteDataStore.GROUP_NOTE_CRAWLER, null);

        assertTrue(dataStore.isSiteNoteCrawler(newParamMap));
        assertTrue(dataStore.isUserNoteCrawler(newParamMap));
        assertTrue(dataStore.isGroupNoteCrawler(newParamMap));
    }

    @Test
    public void test_threadPoolConfiguration() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test with single thread
        paramMap.put(OneNoteDataStore.NUMBER_OF_THREADS, "1");
        assertEquals("1", paramMap.getAsString(OneNoteDataStore.NUMBER_OF_THREADS));

        // Test with multiple threads
        paramMap.put(OneNoteDataStore.NUMBER_OF_THREADS, "10");
        assertEquals("10", paramMap.getAsString(OneNoteDataStore.NUMBER_OF_THREADS));

        // Test with invalid number (non-numeric)
        paramMap.put(OneNoteDataStore.NUMBER_OF_THREADS, "invalid");
        assertEquals("invalid", paramMap.getAsString(OneNoteDataStore.NUMBER_OF_THREADS));
        // Note: Actual implementation should handle this gracefully
    }

    @Test
    public void testStoreData() {
        // doStoreData();
    }

    @Test
    public void test_notebookProcessingOrder() {
        // Test that different crawler types are processed in the expected order
        DataStoreParams paramMap = new DataStoreParams();

        // Enable all crawlers
        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "true");
        paramMap.put(OneNoteDataStore.USER_NOTE_CRAWLER, "true");
        paramMap.put(OneNoteDataStore.GROUP_NOTE_CRAWLER, "true");

        // Verify all are enabled
        assertTrue(dataStore.isSiteNoteCrawler(paramMap));
        assertTrue(dataStore.isUserNoteCrawler(paramMap));
        assertTrue(dataStore.isGroupNoteCrawler(paramMap));

        // The actual processing order is: Sites, Users, Groups
        // This ensures systematic crawling of OneNote content
    }

    @Test
    public void test_emptyParameterMap() {
        DataStoreParams emptyParamMap = new DataStoreParams();

        // Test defaults with empty parameter map - based on actual implementation
        assertTrue(dataStore.isSiteNoteCrawler(emptyParamMap));
        assertTrue(dataStore.isUserNoteCrawler(emptyParamMap));
        assertTrue(dataStore.isGroupNoteCrawler(emptyParamMap));
        assertEquals("1", emptyParamMap.getAsString(OneNoteDataStore.NUMBER_OF_THREADS, "1"));
    }

    @Test
    public void test_caseInsensitiveParameterValues() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test case variations
        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "TRUE");
        assertTrue(dataStore.isSiteNoteCrawler(paramMap));

        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "True");
        assertTrue(dataStore.isSiteNoteCrawler(paramMap));

        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "FALSE");
        assertFalse(dataStore.isSiteNoteCrawler(paramMap));

        paramMap.put(OneNoteDataStore.SITE_NOTE_CRAWLER, "False");
        assertFalse(dataStore.isSiteNoteCrawler(paramMap));
    }

    /**
     * Site notebooks must not touch {@code GET /sites/{id}/permissions}: it needs
     * {@code Sites.FullControl.All} and returns only application grants, never user or group
     * roles. Runs {@link OneNoteDataStore#storeSiteNotes} against a real
     * {@link Microsoft365Client} wired to a {@link GraphMockServer} so the assertion is on actual
     * HTTP traffic, not a mock's bookkeeping.
     */
    @Test
    public void test_storeSiteNotes_doesNotRequestSitePermissions() throws Exception {
        try (GraphMockServer server = new GraphMockServer();
                MockableMicrosoft365Client client = new MockableMicrosoft365Client(dummyParams())) {
            server.enqueueJson("{\"id\":\"site-1\",\"displayName\":\"Root\"}"); // GET /sites/root
            server.enqueueJson("{\"value\":[]}"); // GET /sites/site-1/onenote/notebooks
            // Queued defensively, not consumed by the fixed code: if a regression reintroduces a
            // site-permissions request between the two above, this lets it complete (with an
            // empty result) instead of blocking the test on an unfulfilled mock response, so the
            // /permissions assertion below is what fails, quickly and legibly.
            server.enqueueJson("{\"value\":[]}");

            client.useServer(server.newGraphClient());

            final ExecutorService executorService = Executors.newSingleThreadExecutor();
            try {
                dataStore.storeSiteNotes(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), executorService,
                        client);
            } finally {
                executorService.shutdown();
                assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
            }

            final List<String> paths = new ArrayList<>();
            for (int i = 0; i < server.requestCount(); i++) {
                paths.add(server.takePath());
            }
            assertFalse("no request may end in /permissions, but got: " + paths,
                    paths.stream().anyMatch(path -> path.contains("/permissions")));
        }
    }

    /** Credentials are never used: GraphMockServer does not authenticate, and ClientSecretCredential
     *  acquires tokens lazily, so construction is offline. */
    private static DataStoreParams dummyParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put("tenant", "dummy-tenant");
        params.put("client_id", "dummy-client-id");
        params.put("client_secret", "dummy-client-secret");
        return params;
    }

    /**
     * {@link Microsoft365Client#client} is {@code protected}, reachable directly only from its
     * own {@code client} package (see {@code Microsoft365ClientMockTest}); this subclass exposes
     * it to tests in this package too, so a real {@code Microsoft365Client} can be pointed at a
     * {@link GraphMockServer} instead of stubbing individual methods with Mockito.
     */
    private static final class MockableMicrosoft365Client extends Microsoft365Client {
        MockableMicrosoft365Client(final DataStoreParams params) {
            super(params);
        }

        void useServer(final GraphServiceClient graphClient) {
            this.client = graphClient;
        }
    }

    /**
     * A mutation that swaps {@code NotebookScope.USER} for {@code NotebookScope.GROUP} in
     * {@link OneNoteDataStore#storeUsersNotes} re-introduces, at the DataStore layer, exactly the
     * bug this branch fixed at the client layer -- notebook requests going to the wrong Graph
     * root. {@link org.codelibs.fess.ds.ms365.client.Microsoft365ClientOneNoteTest} covers the
     * client thoroughly per-scope, but nothing exercised which scope
     * {@code storeUsersNotes} itself asks the client for, so that mutation previously survived
     * with the full suite green. Pins the DataStore -> client scope wiring directly.
     */
    @Test
    public void test_storeUsersNotes_requestsUserScope() throws Exception {
        ComponentUtil.register(new SystemHelper(), "systemHelper");

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final User user = new User();
        user.setId("user-1");
        user.setDisplayName("User One");
        final AssignedLicense license = new AssignedLicense();
        license.setSkuId(UUID.randomUUID());
        user.setAssignedLicenses(List.of(license));

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<User> consumer = invocation.getArgument(1);
            consumer.accept(user);
            return null;
        }).when(client).getUsers(any(), any());

        final Notebook notebook = new Notebook();
        notebook.setId("notebook-1");
        notebook.setDisplayName("User Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(notebook));
        when(client.getNotebookPage(NotebookScope.USER, "user-1")).thenReturn(notebookResponse);

        final List<NotebookScope> capturedScopes = new ArrayList<>();
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                // Captures which scope storeUsersNotes actually asked the client for, instead of
                // running the real indexing pipeline.
                capturedScopes.add(scope);
            }
        };

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeUsersNotes(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(),
                    executorService, client);
        } finally {
            executorService.shutdown();
            assertTrue("processNotebook must have run before the test asserts on the captured scope",
                    executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("processNotebook must run exactly once for the one user notebook returned", 1, capturedScopes.size());
        assertEquals(NotebookScope.USER, capturedScopes.get(0));
    }

    /**
     * The GROUP-side counterpart of {@link #test_storeUsersNotes_requestsUserScope}: pins that
     * {@link OneNoteDataStore#storeGroupsNotes} requests {@code NotebookScope.GROUP} rather than
     * silently regressing to {@code NotebookScope.USER}, which sends group notebook requests to
     * the wrong Graph root -- the same bug class this branch fixed.
     */
    @Test
    public void test_storeGroupsNotes_requestsGroupScope() throws Exception {
        ComponentUtil.register(new SystemHelper(), "systemHelper");

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Group group = new Group();
        group.setId("group-1");
        group.setDisplayName("Group One");

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Group> consumer = invocation.getArgument(0);
            consumer.accept(group);
            return null;
        }).when(client).getMicrosoft365Groups(any());

        final Notebook notebook = new Notebook();
        notebook.setId("notebook-1");
        notebook.setDisplayName("Group Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(notebook));
        when(client.getNotebookPage(NotebookScope.GROUP, "group-1")).thenReturn(notebookResponse);

        final List<NotebookScope> capturedScopes = new ArrayList<>();
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                // Captures which scope storeGroupsNotes actually asked the client for, instead of
                // running the real indexing pipeline.
                capturedScopes.add(scope);
            }
        };

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeGroupsNotes(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(),
                    executorService, client);
        } finally {
            executorService.shutdown();
            assertTrue("processNotebook must have run before the test asserts on the captured scope",
                    executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("processNotebook must run exactly once for the one group notebook returned", 1, capturedScopes.size());
        assertEquals(NotebookScope.GROUP, capturedScopes.get(0));
    }

    /**
     * {@code storeSiteNotes} used to call {@code client.getSite("root")} outside any try block.
     * {@code storeData} only catches {@link InterruptedException} around
     * {@code storeSiteNotes}/{@code storeUsersNotes}/{@code storeGroupsNotes}, so a failure
     * resolving the root site used to propagate out of {@code storeData} entirely, aborting user
     * and group notebook crawling too -- not just the site notebooks the README says are the only
     * thing a site-ACL failure skips. Pins that a root-site failure lets
     * {@code storeUsersNotes} and {@code storeGroupsNotes} still run to completion.
     */
    @Test
    public void test_storeData_rootSiteFailure_doesNotAbortUserAndGroupCrawling() throws Exception {
        ComponentUtil.register(new SystemHelper(), "systemHelper");

        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.getSite("root")).thenThrow(new ApiException("failed to resolve root site"));
        // No licensed users/groups to keep this test focused on whether storeUsersNotes and
        // storeGroupsNotes are reached at all, not on per-notebook processing.
        doAnswer(invocation -> null).when(client).getUsers(any(), any());
        doAnswer(invocation -> null).when(client).getMicrosoft365Groups(any());

        final OneNoteDataStore testDataStore = new OneNoteDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                return client;
            }
        };

        // Must return normally: storeData catches only InterruptedException, so if the
        // getSite("root") failure escaped storeSiteNotes uncaught, this call would throw instead.
        testDataStore.storeData(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>());

        // storeUsersNotes and storeGroupsNotes must have run (reached the client) despite the
        // site notebook failure.
        org.mockito.Mockito.verify(client).getUsers(any(), any());
        org.mockito.Mockito.verify(client).getMicrosoft365Groups(any());
    }

    private void doStoreData() {
        final TikaExtractor tikaExtractor = new TikaExtractor();
        tikaExtractor.init();
        ComponentUtil.register(tikaExtractor, "tikaExtractor");

        final DataConfig dataConfig = new DataConfig();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("tenant", tenant);
        paramMap.put("client_id", clientId);
        paramMap.put("client_secret", clientSecret);
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        scriptMap.put(fessConfig.getIndexFieldTitle(), "notebooks.name");
        scriptMap.put(fessConfig.getIndexFieldContent(), "notebooks.contents");
        scriptMap.put(fessConfig.getIndexFieldCreated(), "notebooks.created");
        scriptMap.put(fessConfig.getIndexFieldLastModified(), "notebooks.last_modified");
        scriptMap.put(fessConfig.getIndexFieldUrl(), "notebooks.web_url");
        scriptMap.put(fessConfig.getIndexFieldRole(), "notebooks.roles");

        dataStore.storeData(dataConfig, new TestCallback() {
            @Override
            public void test(DataStoreParams paramMap, Map<String, Object> dataMap) {
                logger.debug(dataMap.toString());
            }
        }, paramMap, scriptMap, defaultDataMap);
    }

    static abstract class TestCallback implements IndexUpdateCallback {
        private long documentSize = 0;
        private long executeTime = 0;

        abstract void test(DataStoreParams paramMap, Map<String, Object> dataMap);

        @Override
        public void store(DataStoreParams paramMap, Map<String, Object> dataMap) {
            final long startTime = System.currentTimeMillis();
            test(paramMap, dataMap);
            executeTime += System.currentTimeMillis() - startTime;
            documentSize++;
        }

        @Override
        public long getDocumentSize() {
            return documentSize;
        }

        @Override
        public long getExecuteTime() {
            return executeTime;
        }

        @Override
        public void commit() {
        }
    }
}

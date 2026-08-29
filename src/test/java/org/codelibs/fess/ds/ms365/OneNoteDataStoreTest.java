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
import java.util.Collections;
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
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.AssignedLicense;
import com.microsoft.graph.models.ExternalLink;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.Notebook;
import com.microsoft.graph.models.NotebookCollectionResponse;
import com.microsoft.graph.models.NotebookLinks;
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

    /**
     * OneNoteDataStore was the only one of the six data stores that ignored
     * {@code default_permissions} (confirmed by {@code grep -c DEFAULT_PERMISSIONS
     * src/main/java/org/codelibs/fess/ds/ms365/*.java}). Task 4 removed site notebooks' only
     * other role source, so this is now the mechanism that keeps a site notebook from being
     * indexed with an empty ACL.
     */
    @Test
    public void test_defaultPermissions_areAppliedToNotebookRoles() {
        registerPermissionHelper();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin,{group}everyone");

        final List<String> roles = dataStore.getDefaultPermissions(paramMap);

        final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
        assertEquals(List.of(permissionHelper.encode("{role}admin"), permissionHelper.encode("{group}everyone")), roles);
    }

    @Test
    public void test_defaultPermissions_absentYieldsNoRoles() {
        registerPermissionHelper();
        assertTrue(dataStore.getDefaultPermissions(new DataStoreParams()).isEmpty());
    }

    @Test
    public void test_defaultPermissions_blankEntriesAreSkipped() {
        registerPermissionHelper();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin, ,");
        assertEquals(1, dataStore.getDefaultPermissions(paramMap).size());
    }

    /**
     * permissionHelper is not wired into test_app.xml, and it in turn needs systemHelper (also
     * not wired) via its {@code @Resource} field, which plain {@link ComponentUtil#register} does
     * not auto-inject -- the same pattern {@code Microsoft365DataStorePermissionTest} and this
     * class's own {@code test_storeSiteNotes_doesNotRequestSitePermissions} predecessor use.
     * {@link TestablePermissionHelper} exposes a same-package-crossing setter so the field can be
     * wired by hand.
     */
    private static void registerPermissionHelper() {
        final SystemHelper systemHelper = new SystemHelper();
        ComponentUtil.register(systemHelper, "systemHelper");
        final TestablePermissionHelper permissionHelper = new TestablePermissionHelper();
        permissionHelper.useSystemHelper(systemHelper);
        ComponentUtil.register(permissionHelper, "permissionHelper");
    }

    private static final class TestablePermissionHelper extends PermissionHelper {
        void useSystemHelper(final SystemHelper systemHelper) {
            this.systemHelper = systemHelper;
        }
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
        // storeSiteNotes now reads default_permissions (Task 5), which needs permissionHelper.
        registerPermissionHelper();
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
            // Closes a vacuous-pass gap: client.getSite("root") alone makes zero requests
            // impossible, but without this a regression that also skipped the notebooks-list
            // request (leaving only the root-site request, still free of "/permissions") would
            // still pass the assertion below.
            assertEquals("expected exactly the root-site and notebooks-list requests, got: " + paths, 2, paths.size());
            assertFalse("no request may end in /permissions, but got: " + paths,
                    paths.stream().anyMatch(path -> path.contains("/permissions")));
        }
    }

    /**
     * The helper being correct (see {@code test_defaultPermissions_*} above) proves nothing if
     * {@code storeSiteNotes} never calls it. Pins that a site notebook's roles actually include
     * the configured {@code default_permissions} value, by overriding {@code processNotebook} to
     * capture the roles argument {@code storeSiteNotes} resolved and passed down -- the same
     * technique the removed {@code test_storeSiteNotes_usesSitePermissionsNotAnEmptyAcl} used for
     * the site-permissions role source it pinned.
     */
    @Test
    public void test_storeSiteNotes_appliesDefaultPermissionsToNotebookRoles() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site root = new Site();
        root.setId("site-1");
        when(client.getSite("root")).thenReturn(root);

        final Notebook notebook = new Notebook();
        notebook.setId("notebook-1");
        notebook.setDisplayName("Site Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(notebook));
        when(client.getNotebookPage(NotebookScope.SITE, "site-1")).thenReturn(notebookResponse);

        final List<List<String>> capturedRoles = new ArrayList<>();
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                // Captures what storeSiteNotes actually resolved and passed down, instead of
                // running the real indexing pipeline.
                capturedRoles.add(roles);
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin");

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeSiteNotes(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>(), executorService, client);
        } finally {
            executorService.shutdown();
            assertTrue("processNotebook must have run before the test asserts on captured roles",
                    executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("processNotebook must run exactly once for the one site notebook returned", 1, capturedRoles.size());
        final List<String> roles = capturedRoles.get(0);
        final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
        assertTrue("expected the configured default_permissions role in the notebook's roles, got " + roles,
                roles.contains(permissionHelper.encode("{role}admin")));
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
        // storeUsersNotes now reads default_permissions (Task 5) in addition to the user's own
        // role, so permissionHelper (and the systemHelper it needs) must be registered too.
        registerPermissionHelper();

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
        // storeGroupsNotes now reads default_permissions (Task 5) in addition to the group's own
        // role, so permissionHelper (and the systemHelper it needs) must be registered too.
        registerPermissionHelper();

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
     * {@code test_defaultPermissions_*} exercises the helper in isolation, and
     * {@link #test_storeUsersNotes_requestsUserScope} above captures only the {@code scope}
     * argument -- neither would catch {@code default_permissions} being dropped from
     * {@link OneNoteDataStore#storeUsersNotes} itself. Pins that a user notebook's roles include
     * both the user's own role and the configured {@code default_permissions} value.
     */
    @Test
    public void test_storeUsersNotes_appliesDefaultPermissionsToNotebookRoles() throws Exception {
        registerPermissionHelper();

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

        final List<List<String>> capturedRoles = new ArrayList<>();
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                // Captures what storeUsersNotes actually resolved and passed down, instead of
                // running the real indexing pipeline.
                capturedRoles.add(roles);
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin");

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeUsersNotes(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>(), executorService, client);
        } finally {
            executorService.shutdown();
            assertTrue("processNotebook must have run before the test asserts on captured roles",
                    executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("processNotebook must run exactly once for the one user notebook returned", 1, capturedRoles.size());
        final List<String> roles = capturedRoles.get(0);
        final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        assertTrue("expected the user's own role in the notebook's roles, got " + roles,
                roles.contains(systemHelper.getSearchRoleByUser("user-1")));
        assertTrue("expected the configured default_permissions role in the notebook's roles, got " + roles,
                roles.contains(permissionHelper.encode("{role}admin")));
    }

    /**
     * The GROUP-side counterpart of {@link #test_storeUsersNotes_appliesDefaultPermissionsToNotebookRoles}:
     * pins that a group notebook's roles include both the group's own role and the configured
     * {@code default_permissions} value.
     */
    @Test
    public void test_storeGroupsNotes_appliesDefaultPermissionsToNotebookRoles() throws Exception {
        registerPermissionHelper();

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

        final List<List<String>> capturedRoles = new ArrayList<>();
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                // Captures what storeGroupsNotes actually resolved and passed down, instead of
                // running the real indexing pipeline.
                capturedRoles.add(roles);
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin");

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeGroupsNotes(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>(), executorService, client);
        } finally {
            executorService.shutdown();
            assertTrue("processNotebook must have run before the test asserts on captured roles",
                    executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("processNotebook must run exactly once for the one group notebook returned", 1, capturedRoles.size());
        final List<String> roles = capturedRoles.get(0);
        final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        assertTrue("expected the group's own role in the notebook's roles, got " + roles,
                roles.contains(systemHelper.getSearchRoleByGroup("group-1")));
        assertTrue("expected the configured default_permissions role in the notebook's roles, got " + roles,
                roles.contains(permissionHelper.encode("{role}admin")));
    }

    /**
     * Every sibling data store folds the crawl config's own Permissions field -- seeded into
     * {@code defaultDataMap} under the role index field -- into the roles list before it reaches
     * the script layer, and de-duplicates the result. {@link OneNoteDataStore#processNotebook}
     * did neither, so with the documented mapping {@code role=notebook.roles} the script's
     * {@code dataMap.put("role", roles)} would overwrite whatever the field seeded instead of
     * adding to it. Pins that a role entry in {@code defaultDataMap} survives into
     * {@code NOTEBOOK_ROLES} alongside the roles {@code processNotebook} was called with, and
     * that the {@code roles} argument itself is not mutated in place (it is shared across
     * concurrent notebook-processing tasks for the same owner in
     * {@code storeUsersNotes}/{@code storeGroupsNotes}) -- an unmodifiable list is passed in to
     * make an in-place mutation fail loudly rather than pass silently.
     */
    @Test
    public void test_processNotebook_foldsDefaultDataMapRoleIntoNotebookRoles() throws Exception {
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final org.codelibs.fess.helper.CrawlerStatsHelper crawlerStatsHelper = new org.codelibs.fess.helper.CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");

        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.getNotebookContent(NotebookScope.USER, "user-1", "notebook-1")).thenReturn("content");

        final Notebook notebook = new Notebook();
        notebook.setId("notebook-1");
        notebook.setDisplayName("Test Notebook");
        final NotebookLinks links = new NotebookLinks();
        final ExternalLink oneNoteWebUrl = new ExternalLink();
        oneNoteWebUrl.setHref("https://example.com/notebook");
        links.setOneNoteWebUrl(oneNoteWebUrl);
        notebook.setLinks(links);

        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        final Map<String, Object> defaultDataMap = new HashMap<>();
        defaultDataMap.put(fessConfig.getIndexFieldRole(), List.of("Rconfigured-role"));

        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put(fessConfig.getIndexFieldRole(), "notebook.roles");

        // convertValue's real path goes through ComponentUtil.getScriptEngineFactory(), which
        // this unit test has no business standing up. "notebook.roles" is the only template used
        // here, so it is resolved with a direct nested map lookup instead; processNotebook
        // itself, including the roles-folding logic under test, is exercised completely
        // unmodified.
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                if ("notebook.roles".equals(template) && resultMap.get(NOTEBOOK) instanceof final Map<?, ?> notebookMap) {
                    return notebookMap.get(NOTEBOOK_ROLES);
                }
                return super.convertValue(scriptType, template, resultMap);
            }
        };

        final List<Map<String, Object>> capturedDataMaps = new ArrayList<>();
        final IndexUpdateCallback callback = new IndexUpdateCallback() {
            @Override
            public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
                capturedDataMaps.add(dataMap);
            }

            @Override
            public long getDocumentSize() {
                return capturedDataMaps.size();
            }

            @Override
            public long getExecuteTime() {
                return 0;
            }

            @Override
            public void commit() {
                // do nothing
            }
        };

        // Passed in as an unmodifiable list: processNotebook must not mutate it in place, since
        // storeUsersNotes/storeGroupsNotes share this same list reference across every
        // notebook-processing task for the same owner.
        final List<String> roles = Collections.singletonList("Rowner-role");

        captureDataStore.processNotebook(new DataConfig(), callback, new DataStoreParams(), scriptMap, defaultDataMap, client,
                NotebookScope.USER, "user-1", notebook, roles);

        assertEquals("processNotebook must have called the callback exactly once", 1, capturedDataMaps.size());
        @SuppressWarnings("unchecked")
        final List<String> finalRoles = (List<String>) capturedDataMaps.get(0).get(fessConfig.getIndexFieldRole());
        assertTrue("expected the notebook's own role to survive, got " + finalRoles, finalRoles.contains("Rowner-role"));
        assertTrue("expected the defaultDataMap-seeded role to survive, got " + finalRoles, finalRoles.contains("Rconfigured-role"));
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

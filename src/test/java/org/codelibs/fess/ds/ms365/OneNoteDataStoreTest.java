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

import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.GraphMockServer;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.ds.ms365.client.NotebookScope;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
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
            // site_id is set so the crawl stays on one site: with it unset storeSiteNotes
            // enumerates the whole tenant, and this test is about which requests one site makes.
            server.enqueueJson("{\"id\":\"site-1\",\"displayName\":\"Team\"}"); // GET /sites/site-1
            server.enqueueJson("{\"value\":[]}"); // GET /sites/site-1/onenote/notebooks
            // Queued defensively, not consumed by the fixed code: if a regression reintroduces a
            // site-permissions request between the two above, this lets it complete (with an
            // empty result) instead of blocking the test on an unfulfilled mock response, so the
            // /permissions assertion below is what fails, quickly and legibly.
            server.enqueueJson("{\"value\":[]}");

            client.useServer(server.newGraphClient());

            final DataStoreParams params = new DataStoreParams();
            params.put("site_id", "site-1");

            final ExecutorService executorService = Executors.newSingleThreadExecutor();
            try {
                dataStore.storeSiteNotes(new DataConfig(), null, params, new HashMap<>(), new HashMap<>(), executorService, client,
                        new OneNoteDataStore.NotebookFilterStats());
            } finally {
                executorService.shutdown();
                assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
            }

            final List<String> paths = new ArrayList<>();
            for (int i = 0; i < server.requestCount(); i++) {
                paths.add(server.takePath());
            }
            // Closes a vacuous-pass gap: client.getSite(siteId) alone makes zero requests
            // impossible, but without this a regression that also skipped the notebooks-list
            // request (leaving only the site request, still free of "/permissions") would
            // still pass the assertion below.
            assertEquals("expected exactly the site and notebooks-list requests, got: " + paths, 2, paths.size());
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
        // storeSiteNotes enumerates the tenant's sites when site_id is unset, so the one site
        // this test cares about is delivered through getSites rather than getSite("root").
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Site> consumer = invocation.getArgument(0);
            consumer.accept(root);
            return null;
        }).when(client).getSites(any());

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
            captureDataStore.storeSiteNotes(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>(), executorService, client,
                    new OneNoteDataStore.NotebookFilterStats());
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

    @Test
    public void test_getPattern_blankAndInvalid() {
        final DataStoreParams paramMap = new DataStoreParams();
        assertNull("an unset pattern must not filter anything", dataStore.getPattern(paramMap, "include_pattern"));

        paramMap.put("include_pattern", "   ");
        assertNull("a blank pattern must not filter anything", dataStore.getPattern(paramMap, "include_pattern"));

        // A malformed pattern used to be logged and swallowed, and null reads to every caller as
        // "no filtering configured" -- which turns a mistyped exclude_pattern into a fail-open.
        paramMap.put("include_pattern", "[invalid");
        final DataStoreException e = assertThrows(DataStoreException.class, () -> dataStore.getPattern(paramMap, "include_pattern"));
        assertTrue("the message must name the parameter, got: " + e.getMessage(), e.getMessage().contains("include_pattern"));
        assertTrue("the message must carry the regex syntax error, got: " + e.getMessage(),
                e.getMessage().contains("Unclosed character class"));

        paramMap.put("include_pattern", "Project.*");
        assertNotNull(dataStore.getPattern(paramMap, "include_pattern"));
    }

    @Test
    public void test_isTargetNotebook_noPatternsAcceptsEverything() {
        assertTrue(dataStore.isTargetNotebook(null, null, notebookNamed("Anything At All")));
        assertTrue("a null display name must not be filtered out", dataStore.isTargetNotebook(null, null, notebookNamed(null)));
    }

    @Test
    public void test_isTargetNotebook_includePatternIsFullMatch() {
        final Pattern includePattern = Pattern.compile("Project.*");
        assertTrue(dataStore.isTargetNotebook(includePattern, null, notebookNamed("Project Apollo")));
        // Full match, not find(): a name that merely contains the pattern is excluded.
        assertFalse(dataStore.isTargetNotebook(includePattern, null, notebookNamed("Archived Project Apollo")));
    }

    @Test
    public void test_isTargetNotebook_excludePatternIsFullMatch() {
        final Pattern excludePattern = Pattern.compile("Test.*");
        assertFalse(dataStore.isTargetNotebook(null, excludePattern, notebookNamed("Test Notebook")));
        assertTrue(dataStore.isTargetNotebook(null, excludePattern, notebookNamed("Production Notebook")));
        // Full match, not find(): "Latest Notes" contains "test" but must survive Test.*
        assertTrue("full-match semantics must not exclude a name that merely contains the pattern",
                dataStore.isTargetNotebook(null, excludePattern, notebookNamed("Latest Notes")));
        // Full match, not find(): "Test.*" is found as a substring starting mid-name, but the
        // whole name does not start with "Test", so a find()-based mutation would wrongly exclude
        // this notebook while matches() correctly keeps it.
        assertTrue("full-match semantics must not exclude a name that merely contains the pattern elsewhere",
                dataStore.isTargetNotebook(null, excludePattern, notebookNamed("Production Test Notes")));
    }

    /**
     * A notebook with no usable display name is matched as "" instead of bypassing both patterns.
     * An operator who configured include_pattern said what they want indexed, and an unnamed
     * notebook is not it; an operator who only configured exclude_pattern named what they want
     * dropped, and an unnamed notebook is not that either.
     */
    @Test
    public void test_isTargetNotebook_blankNameIsMatchedAsEmptyString() {
        final Pattern includePattern = Pattern.compile("Project.*");
        final Pattern excludePattern = Pattern.compile("Test.*");

        assertFalse("a null-named notebook must not satisfy an include_pattern that rejects \"\"",
                dataStore.isTargetNotebook(includePattern, null, notebookNamed(null)));
        assertFalse("an empty-named notebook must not satisfy an include_pattern that rejects \"\"",
                dataStore.isTargetNotebook(includePattern, null, notebookNamed("")));
        assertFalse("a whitespace-named notebook must not satisfy an include_pattern that rejects it",
                dataStore.isTargetNotebook(includePattern, null, notebookNamed("   ")));

        assertTrue("a null-named notebook must survive an exclude_pattern that does not match \"\"",
                dataStore.isTargetNotebook(null, excludePattern, notebookNamed(null)));
        assertTrue("an empty-named notebook must survive an exclude_pattern that does not match \"\"",
                dataStore.isTargetNotebook(null, excludePattern, notebookNamed("")));

        // The patterns really do decide: one that matches "" admits/drops the unnamed notebook.
        assertTrue("an include_pattern matching \"\" must admit an unnamed notebook",
                dataStore.isTargetNotebook(Pattern.compile(".*"), null, notebookNamed(null)));
        assertFalse("an exclude_pattern matching \"\" must drop an unnamed notebook",
                dataStore.isTargetNotebook(null, Pattern.compile(".*"), notebookNamed(null)));
    }

    /**
     * The assertions above all use patterns that reject {@code ""} and {@code "   "} alike, so they
     * cannot tell whether a whitespace-only name is normalised or matched verbatim. {@code .+} can:
     * it matches {@code "   "} but not {@code ""}. Normalising only null left the three spellings of
     * "this notebook has no usable name" behaving differently -- {@code include_pattern=.+} admitted
     * a notebook named {@code "   "} while rejecting a null-named one -- contradicting both the
     * javadoc and the README.
     */
    @Test
    public void test_isTargetNotebook_whitespaceOnlyNameIsTreatedAsMissing() {
        final Pattern anyNonEmpty = Pattern.compile(".+");

        assertFalse("a null-named notebook must not satisfy include_pattern=.+",
                dataStore.isTargetNotebook(anyNonEmpty, null, notebookNamed(null)));
        assertFalse("an empty-named notebook must not satisfy include_pattern=.+",
                dataStore.isTargetNotebook(anyNonEmpty, null, notebookNamed("")));
        assertFalse("a whitespace-only name must be treated like a missing one, not admitted by .+",
                dataStore.isTargetNotebook(anyNonEmpty, null, notebookNamed("   ")));
        assertFalse("a tab-and-newline-only name is just as unusable",
                dataStore.isTargetNotebook(anyNonEmpty, null, notebookNamed("\t\n")));

        assertTrue("a null-named notebook must survive exclude_pattern=.+",
                dataStore.isTargetNotebook(null, anyNonEmpty, notebookNamed(null)));
        assertTrue("a whitespace-only name must survive exclude_pattern=.+ for the same reason",
                dataStore.isTargetNotebook(null, anyNonEmpty, notebookNamed("   ")));

        // Only a name that is entirely whitespace is normalised. A real name is matched verbatim,
        // surrounding whitespace included, so no existing pattern changes meaning.
        assertTrue("a padded real name must still be matched verbatim",
                dataStore.isTargetNotebook(Pattern.compile("\\s*Project Apollo\\s*"), null, notebookNamed("  Project Apollo  ")));
        assertFalse("a padded real name must not be normalised away",
                dataStore.isTargetNotebook(Pattern.compile("Project Apollo"), null, notebookNamed("  Project Apollo  ")));
    }

    @Test
    public void test_isTargetNotebook_excludeWinsOverInclude() {
        final Pattern includePattern = Pattern.compile(".*Notebook");
        final Pattern excludePattern = Pattern.compile("Test.*");
        assertFalse(dataStore.isTargetNotebook(includePattern, excludePattern, notebookNamed("Test Notebook")));
    }

    /**
     * The predicate being correct proves nothing if storeSiteNotes never calls it. Pins the
     * wiring by overriding processNotebook to capture which notebooks actually got through.
     */
    @Test
    public void test_storeSiteNotes_appliesExcludePatternToNotebooks() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site root = new Site();
        root.setId("site-1");
        // storeSiteNotes enumerates the tenant's sites when site_id is unset, so the one site
        // this test cares about is delivered through getSites rather than getSite("root").
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Site> consumer = invocation.getArgument(0);
            consumer.accept(root);
            return null;
        }).when(client).getSites(any());

        final Notebook kept = new Notebook();
        kept.setId("notebook-kept");
        kept.setDisplayName("Production Notebook");
        final Notebook dropped = new Notebook();
        dropped.setId("notebook-dropped");
        dropped.setDisplayName("Test Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(kept, dropped));
        when(client.getNotebookPage(NotebookScope.SITE, "site-1")).thenReturn(notebookResponse);

        final List<String> processedIds = Collections.synchronizedList(new ArrayList<>());
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                processedIds.add(notebook.getId());
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", "Test.*");

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeSiteNotes(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>(), executorService, client,
                    new OneNoteDataStore.NotebookFilterStats());
        } finally {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        // 2-argument assertEquals: the expected/actual order of the 3-argument form is a known
        // trap in this codebase, and the failure message already prints both values.
        assertEquals(List.of("notebook-kept"), processedIds);
    }

    /**
     * The USER-side counterpart of {@link #test_storeSiteNotes_appliesExcludePatternToNotebooks}:
     * SITE wiring going green proves nothing about USER wiring, since each scope has its own
     * {@code getNotebooks} consumer with its own filter guard.
     */
    @Test
    public void test_storeUsersNotes_appliesExcludePatternToNotebooks() throws Exception {
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

        final Notebook kept = new Notebook();
        kept.setId("notebook-kept");
        kept.setDisplayName("Production Notebook");
        final Notebook dropped = new Notebook();
        dropped.setId("notebook-dropped");
        dropped.setDisplayName("Test Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(kept, dropped));
        when(client.getNotebookPage(NotebookScope.USER, "user-1")).thenReturn(notebookResponse);

        final List<String> processedIds = Collections.synchronizedList(new ArrayList<>());
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                processedIds.add(notebook.getId());
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", "Test.*");

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeUsersNotes(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>(), executorService, client,
                    new OneNoteDataStore.NotebookFilterStats());
        } finally {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals(List.of("notebook-kept"), processedIds);
    }

    /**
     * The GROUP-side counterpart of {@link #test_storeSiteNotes_appliesExcludePatternToNotebooks}:
     * SITE (and USER) wiring going green proves nothing about GROUP wiring, since each scope has
     * its own {@code getNotebooks} consumer with its own filter guard.
     */
    @Test
    public void test_storeGroupsNotes_appliesExcludePatternToNotebooks() throws Exception {
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

        final Notebook kept = new Notebook();
        kept.setId("notebook-kept");
        kept.setDisplayName("Production Notebook");
        final Notebook dropped = new Notebook();
        dropped.setId("notebook-dropped");
        dropped.setDisplayName("Test Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(kept, dropped));
        when(client.getNotebookPage(NotebookScope.GROUP, "group-1")).thenReturn(notebookResponse);

        final List<String> processedIds = Collections.synchronizedList(new ArrayList<>());
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                processedIds.add(notebook.getId());
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", "Test.*");

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeGroupsNotes(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>(), executorService, client,
                    new OneNoteDataStore.NotebookFilterStats());
        } finally {
            executorService.shutdown();
            assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals(List.of("notebook-kept"), processedIds);
    }

    private static Notebook notebookNamed(final String displayName) {
        final Notebook notebook = new Notebook();
        notebook.setDisplayName(displayName);
        return notebook;
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
                    executorService, client, new OneNoteDataStore.NotebookFilterStats());
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
                    executorService, client, new OneNoteDataStore.NotebookFilterStats());
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
            captureDataStore.storeUsersNotes(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>(), executorService, client,
                    new OneNoteDataStore.NotebookFilterStats());
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
            captureDataStore.storeGroupsNotes(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>(), executorService, client,
                    new OneNoteDataStore.NotebookFilterStats());
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
     * {@code storeSiteNotes} used to resolve its site outside any try block. {@code storeData}
     * only catches {@link InterruptedException} around
     * {@code storeSiteNotes}/{@code storeUsersNotes}/{@code storeGroupsNotes}, so a failure
     * listing sites used to propagate out of {@code storeData} entirely, aborting user and group
     * notebook crawling too -- not just the site notebooks the README says are the only thing a
     * site-ACL failure skips. Pins that a site-enumeration failure lets
     * {@code storeUsersNotes} and {@code storeGroupsNotes} still run to completion.
     */
    @Test
    public void test_storeData_siteEnumerationFailure_doesNotAbortUserAndGroupCrawling() throws Exception {
        ComponentUtil.register(new SystemHelper(), "systemHelper");

        final Microsoft365Client client = mock(Microsoft365Client.class);
        doThrow(new ApiException("failed to list sites")).when(client).getSites(any());
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
        // getSites failure escaped storeSiteNotes uncaught, this call would throw instead.
        testDataStore.storeData(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>());

        // storeUsersNotes and storeGroupsNotes must have run (reached the client) despite the
        // site notebook failure.
        org.mockito.Mockito.verify(client).getUsers(any(), any());
        org.mockito.Mockito.verify(client).getMicrosoft365Groups(any());
    }

    /**
     * A mistyped {@code include_pattern}/{@code exclude_pattern} excludes every notebook without
     * any error: the crawl finishes normally and simply indexes nothing. Pins that {@code
     * storeData} reports that case with exactly one {@code WARN}, not silence and not one WARN
     * per skipped notebook.
     */
    @Test
    public void test_storeData_warnsOnceWhenConfiguredPatternMatchesNothing() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site root = new Site();
        root.setId("site-1");
        // storeSiteNotes enumerates the tenant's sites when site_id is unset, so the one site
        // this test cares about is delivered through getSites rather than getSite("root").
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Site> consumer = invocation.getArgument(0);
            consumer.accept(root);
            return null;
        }).when(client).getSites(any());

        final Notebook excluded = new Notebook();
        excluded.setId("notebook-1");
        excluded.setDisplayName("Test Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(excluded));
        when(client.getNotebookPage(NotebookScope.SITE, "site-1")).thenReturn(notebookResponse);

        // No licensed users/groups: keeps this test's notebook count to the one SITE notebook.
        doAnswer(invocation -> null).when(client).getUsers(any(), any());
        doAnswer(invocation -> null).when(client).getMicrosoft365Groups(any());

        final OneNoteDataStore testDataStore = new OneNoteDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                return client;
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", "Test.*");

        final List<LogEvent> events = captureOneNoteDataStoreWarnings(
                () -> testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>()));

        final List<String> matchedNothingWarnings =
                messagesOf(events).stream().filter(message -> message.contains("matched none")).collect(Collectors.toList());
        assertEquals("expected exactly one 'matched none' WARN, got: " + messagesOf(events), 1, matchedNothingWarnings.size());
    }

    /**
     * The counterpart to {@link #test_storeData_warnsOnceWhenConfiguredPatternMatchesNothing}:
     * when the configured pattern admits at least one notebook, the WARN must not fire at all.
     */
    @Test
    public void test_storeData_doesNotWarnWhenConfiguredPatternMatchesSomething() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site root = new Site();
        root.setId("site-1");
        // storeSiteNotes enumerates the tenant's sites when site_id is unset, so the one site
        // this test cares about is delivered through getSites rather than getSite("root").
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Site> consumer = invocation.getArgument(0);
            consumer.accept(root);
            return null;
        }).when(client).getSites(any());

        final Notebook kept = new Notebook();
        kept.setId("notebook-1");
        kept.setDisplayName("Production Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(kept));
        when(client.getNotebookPage(NotebookScope.SITE, "site-1")).thenReturn(notebookResponse);

        doAnswer(invocation -> null).when(client).getUsers(any(), any());
        doAnswer(invocation -> null).when(client).getMicrosoft365Groups(any());

        final OneNoteDataStore testDataStore = new OneNoteDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                return client;
            }

            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                // Skip the real indexing pipeline; this test only cares about the WARN.
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", "Test.*");

        final List<LogEvent> events = captureOneNoteDataStoreWarnings(
                () -> testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>()));

        final List<String> matchedNothingWarnings =
                messagesOf(events).stream().filter(message -> message.contains("matched none")).collect(Collectors.toList());
        assertTrue("expected no 'matched none' WARN when a notebook was admitted, got: " + messagesOf(events),
                matchedNothingWarnings.isEmpty());
    }

    /**
     * A malformed pattern used to be logged and swallowed by {@code getPattern}, once per scope -
     * three identical WARNs with three stack traces for one typo - and the null it returned reads
     * to {@code isTargetNotebook} as "no filtering", so a mistyped {@code exclude_pattern}
     * indexed every notebook it was meant to keep out. Pins that the crawl fails once, before any
     * Graph call, and that the swallowed WARNs are gone.
     */
    @Test
    public void test_storeData_malformedExcludePatternFailsOnceBeforeAnyGraphCall() {
        final java.util.concurrent.atomic.AtomicInteger clientsCreated = new java.util.concurrent.atomic.AtomicInteger();
        final OneNoteDataStore testDataStore = new OneNoteDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                clientsCreated.incrementAndGet();
                throw new AssertionError("storeData must fail on the malformed pattern before creating a client");
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", "Archive.*[");

        final DataStoreException e = assertThrows(DataStoreException.class,
                () -> testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>()));
        assertTrue("the failure must name the parameter, got: " + e.getMessage(), e.getMessage().contains("exclude_pattern"));
        assertTrue("the failure must carry the regex syntax error, got: " + e.getMessage(),
                e.getMessage().contains("Unclosed character class"));
        assertEquals("no Graph client may be created for a crawl that cannot honour its own filter", 0, clientsCreated.get());
    }

    /**
     * The same typo used to produce one {@code Invalid regex pattern} WARN per scope, because all
     * three of {@code site_note_crawler}, {@code user_note_crawler} and {@code group_note_crawler}
     * default to true and each asked {@code getPattern} for itself. It must be reported once.
     */
    @Test
    public void test_storeData_malformedPatternIsReportedOnceNotOncePerScope() {
        final OneNoteDataStore testDataStore = new OneNoteDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                throw new AssertionError("storeData must fail on the malformed pattern before creating a client");
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_pattern", "Project[");

        final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());
        final org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(Microsoft365DataStore.class);
        final AbstractAppender appender =
                new AbstractAppender("test-onenote-invalid-pattern-warn-capture", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(final LogEvent event) {
                        if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
                            events.add(event.toImmutable());
                        }
                    }
                };
        appender.start();
        coreLogger.addAppender(appender);
        try {
            assertThrows(DataStoreException.class,
                    () -> testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>()));
        } finally {
            coreLogger.removeAppender(appender);
            appender.stop();
        }

        final List<String> invalidPatternWarnings =
                messagesOf(events).stream().filter(message -> message.contains("Invalid regex pattern")).collect(Collectors.toList());
        assertTrue("one malformed pattern must not be reported through a per-scope WARN, got: " + messagesOf(events),
                invalidPatternWarnings.isEmpty());
    }

    /**
     * The "matched none" WARN used to name {@code include_pattern/exclude_pattern} unconditionally,
     * telling an operator who set only {@code exclude_pattern} to go and check an
     * {@code include_pattern} that is not in their configuration.
     */
    @Test
    public void test_storeData_matchedNoneWarningNamesOnlyTheConfiguredPattern() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site root = new Site();
        root.setId("site-1");
        // storeSiteNotes enumerates the tenant's sites when site_id is unset, so the one site
        // this test cares about is delivered through getSites rather than getSite("root").
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Site> consumer = invocation.getArgument(0);
            consumer.accept(root);
            return null;
        }).when(client).getSites(any());

        final Notebook excluded = new Notebook();
        excluded.setId("notebook-1");
        excluded.setDisplayName("Test Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(excluded));
        when(client.getNotebookPage(NotebookScope.SITE, "site-1")).thenReturn(notebookResponse);

        doAnswer(invocation -> null).when(client).getUsers(any(), any());
        doAnswer(invocation -> null).when(client).getMicrosoft365Groups(any());

        final OneNoteDataStore testDataStore = new OneNoteDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                return client;
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", "Test.*");

        final List<LogEvent> events = captureOneNoteDataStoreWarnings(
                () -> testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>()));

        final String matchedNone = messagesOf(events).stream()
                .filter(message -> message.contains("matched none"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected a 'matched none' WARN, got: " + messagesOf(events)));
        assertTrue("the WARN must name exclude_pattern, got: " + matchedNone, matchedNone.contains("exclude_pattern"));
        assertFalse("the WARN must not name a pattern the operator never set, got: " + matchedNone,
                matchedNone.contains("include_pattern"));
    }

    /**
     * The notebook filter counters used to be stashed in {@code paramMap} under
     * {@code _onenote_notebook_filter_stats}, and {@code processNotebook} copies {@code paramMap}
     * wholesale into every notebook's script bindings - so internal bookkeeping became an
     * operator-visible script variable on every OneNote document, whether or not a pattern was
     * configured. Pins that nothing internal reaches the bindings.
     */
    @Test
    public void test_storeData_doesNotLeakInternalStateIntoScriptBindings() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site root = new Site();
        root.setId("site-1");
        // storeSiteNotes enumerates the tenant's sites when site_id is unset, so the one site
        // this test cares about is delivered through getSites rather than getSite("root").
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Site> consumer = invocation.getArgument(0);
            consumer.accept(root);
            return null;
        }).when(client).getSites(any());

        final Notebook notebook = new Notebook();
        notebook.setId("notebook-1");
        notebook.setDisplayName("Production Notebook");
        final NotebookCollectionResponse notebookResponse = new NotebookCollectionResponse();
        notebookResponse.setValue(List.of(notebook));
        when(client.getNotebookPage(NotebookScope.SITE, "site-1")).thenReturn(notebookResponse);

        doAnswer(invocation -> null).when(client).getUsers(any(), any());
        doAnswer(invocation -> null).when(client).getMicrosoft365Groups(any());

        final List<java.util.Set<String>> capturedBindingKeys = Collections.synchronizedList(new ArrayList<>());
        final OneNoteDataStore testDataStore = new OneNoteDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                return client;
            }

            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                // The real processNotebook builds its script bindings from exactly this map.
                capturedBindingKeys.add(new java.util.LinkedHashSet<>(paramMap.asMap().keySet()));
            }
        };

        testDataStore.storeData(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>());

        assertEquals("expected the one site notebook to be processed", 1, capturedBindingKeys.size());
        final java.util.Set<String> keys = capturedBindingKeys.get(0);
        assertFalse("the notebook filter counters must not reach the script bindings, got: " + keys,
                keys.contains("_onenote_notebook_filter_stats"));
        assertTrue("no internal bookkeeping key may reach the script bindings, got: " + keys,
                keys.stream().noneMatch(key -> key.startsWith("_onenote")));
    }

    /**
     * Captures {@code WARN}-or-worse log records emitted by {@link OneNoteDataStore}'s logger
     * while {@code action} runs, mirroring the capture helper {@code Microsoft365DataStoreTest}
     * uses for the base class's own logger.
     *
     * @param action the code whose logging should be captured.
     * @return the captured records.
     */
    private static List<LogEvent> captureOneNoteDataStoreWarnings(final Runnable action) {
        final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());
        final org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(OneNoteDataStore.class);
        final AbstractAppender appender =
                new AbstractAppender("test-onenote-datastore-warn-capture", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(final LogEvent event) {
                        if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
                            events.add(event.toImmutable());
                        }
                    }
                };
        appender.start();
        coreLogger.addAppender(appender);
        try {
            action.run();
        } finally {
            coreLogger.removeAppender(appender);
            appender.stop();
        }
        return events;
    }

    /**
     * @param events the captured records.
     * @return their formatted messages, for an assertion failure that can be read.
     */
    private static List<String> messagesOf(final List<LogEvent> events) {
        return events.stream().map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
    }

    /**
     * {@code FailureUrlService.store} looks its row up with {@code setUrl_Equal}, so the URL
     * argument is the row key. {@code processNotebook} used to pass the display name, which is not
     * unique: two notebooks that share one collapsed into a single failure row and an operator saw
     * one failure where there were two. Pins that the notebook's own web URL is used instead.
     */
    @Test
    public void test_processNotebook_failuresAreKeyedByTheNotebookWebUrl() {
        final CapturingFailureUrlService failureUrlService = CapturingFailureUrlService.empty();
        registerNotebookProcessingComponents();

        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.getNotebookContent(any(), any(), any())).thenThrow(new CrawlingAccessException("content unavailable"));

        // same display name, different notebooks
        processNotebook(client, newNotebook("notebook-1", "Shared Name", "https://example.com/notebook/1"));
        processNotebook(client, newNotebook("notebook-2", "Shared Name", "https://example.com/notebook/2"));

        assertEquals("each notebook must get its own failure row",
                List.of("https://example.com/notebook/1", "https://example.com/notebook/2"), storedFailureUrls(failureUrlService));
    }

    /**
     * The web URL is read inside the try, so a failure before that point leaves it unset. The id
     * is the next-best value that is still unique per notebook; the display name is not.
     */
    @Test
    public void test_processNotebook_failureBeforeTheUrlIsReadFallsBackToTheNotebookId() {
        final CapturingFailureUrlService failureUrlService = CapturingFailureUrlService.empty();
        registerNotebookProcessingComponents();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        // no links at all: getLinks() is dereferenced on the first line of the try
        processNotebook(client, newNotebook("notebook-1", "Shared Name", null));
        processNotebook(client, newNotebook("notebook-2", "Shared Name", null));

        assertEquals("each notebook must still get its own failure row", List.of("notebook-1", "notebook-2"),
                storedFailureUrls(failureUrlService));
    }

    /**
     * The whole fallback chain, at the seam. The last arm cannot be reached through
     * {@code processNotebook}: a notebook with no id makes {@code CrawlerStatsHelper.done}
     * throw from the {@code finally} block (a {@code StatsKeyObject} with a null id has no cache
     * key), which is pre-existing behaviour unrelated to which value keys the failure row.
     */
    @Test
    public void test_failureUrlOf_prefersTheWebUrlThenTheIdThenTheDisplayName() {
        final Notebook complete = newNotebook("notebook-1", "Notebook", "https://example.com/notebook/1");
        assertEquals("https://example.com/notebook/1", OneNoteDataStore.failureUrlOf("https://example.com/notebook/1", complete));

        // the URL is read inside the try, so a failure before that point leaves it unset
        assertEquals("notebook-1", OneNoteDataStore.failureUrlOf(null, complete));
        assertEquals("notebook-1", OneNoteDataStore.failureUrlOf("", complete));

        // last resort: not unique, but never null
        assertEquals("Only Name", OneNoteDataStore.failureUrlOf(null, newNotebook(null, "Only Name", null)));
    }

    /**
     * {@code processNotebook} resolves the stats helper from the container, which in turn needs
     * the system helper.
     */
    private static void registerNotebookProcessingComponents() {
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final org.codelibs.fess.helper.CrawlerStatsHelper crawlerStatsHelper = new org.codelibs.fess.helper.CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
    }

    /**
     * @param id the notebook id, or {@code null} to leave it unset.
     * @param displayName the notebook display name.
     * @param webUrl the OneNote web URL, or {@code null} to leave the notebook without links.
     * @return the notebook.
     */
    private static Notebook newNotebook(final String id, final String displayName, final String webUrl) {
        final Notebook notebook = new Notebook();
        notebook.setId(id);
        notebook.setDisplayName(displayName);
        if (webUrl != null) {
            final NotebookLinks links = new NotebookLinks();
            final ExternalLink oneNoteWebUrl = new ExternalLink();
            oneNoteWebUrl.setHref(webUrl);
            links.setOneNoteWebUrl(oneNoteWebUrl);
            notebook.setLinks(links);
        }
        return notebook;
    }

    private void processNotebook(final Microsoft365Client client, final Notebook notebook) {
        dataStore.processNotebook(new DataConfig(), null, new DataStoreParams(), Collections.emptyMap(), new HashMap<>(), client,
                NotebookScope.USER, "user-1", notebook, Collections.emptyList());
    }

    /**
     * @param failureUrlService the stub.
     * @return the URL argument of every recorded failure, in order.
     */
    private static List<String> storedFailureUrls(final CapturingFailureUrlService failureUrlService) {
        return failureUrlService.getStoredFailures().stream().map(CapturingFailureUrlService.StoredFailure::url).toList();
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

    /**
     * Site notebooks used to come from {@code GET /sites/root} and nowhere else, so a notebook on
     * any other team site was unreachable however the crawl was configured. Pins that an unset
     * {@code site_id} now covers every site in the tenant, the same way
     * {@code sharePointPageDataStore} reads the parameter.
     */
    @Test
    public void test_storeSiteNotes_withoutSiteId_crawlsEverySite() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site first = new Site();
        first.setId("site-1");
        final Site second = new Site();
        second.setId("site-2");
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Site> consumer = invocation.getArgument(0);
            consumer.accept(first);
            consumer.accept(second);
            return null;
        }).when(client).getSites(any());

        for (final String siteId : List.of("site-1", "site-2")) {
            final Notebook notebook = new Notebook();
            notebook.setId("notebook-" + siteId);
            notebook.setDisplayName("Notebook " + siteId);
            final NotebookCollectionResponse response = new NotebookCollectionResponse();
            response.setValue(List.of(notebook));
            when(client.getNotebookPage(NotebookScope.SITE, siteId)).thenReturn(response);
        }

        final List<String> capturedOwnerIds = new ArrayList<>();
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                synchronized (capturedOwnerIds) {
                    capturedOwnerIds.add(ownerId);
                }
            }
        };

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeSiteNotes(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(),
                    executorService, client, new OneNoteDataStore.NotebookFilterStats());
        } finally {
            executorService.shutdown();
            assertTrue("processNotebook must have run before the test asserts on the captured sites",
                    executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("both sites' notebooks must be processed, got: " + capturedOwnerIds, 2, capturedOwnerIds.size());
        assertTrue("site-1's notebook must be processed, got: " + capturedOwnerIds, capturedOwnerIds.contains("site-1"));
        assertTrue("site-2's notebook must be processed, got: " + capturedOwnerIds, capturedOwnerIds.contains("site-2"));
    }

    /**
     * The counterpart of {@link #test_storeSiteNotes_withoutSiteId_crawlsEverySite()}: a
     * {@code site_id} confines the crawl to that one site instead of enumerating the tenant, so an
     * operator who has one notebook-bearing site does not pay for a full site listing.
     */
    @Test
    public void test_storeSiteNotes_withSiteId_crawlsOnlyThatSite() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site site = new Site();
        site.setId("site-1");
        when(client.getSite("site-1")).thenReturn(site);

        final Notebook notebook = new Notebook();
        notebook.setId("notebook-1");
        notebook.setDisplayName("Site Notebook");
        final NotebookCollectionResponse response = new NotebookCollectionResponse();
        response.setValue(List.of(notebook));
        when(client.getNotebookPage(NotebookScope.SITE, "site-1")).thenReturn(response);

        final List<String> capturedOwnerIds = new ArrayList<>();
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                capturedOwnerIds.add(ownerId);
            }
        };

        final DataStoreParams params = new DataStoreParams();
        params.put("site_id", "site-1");

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeSiteNotes(new DataConfig(), null, params, new HashMap<>(), new HashMap<>(), executorService, client,
                    new OneNoteDataStore.NotebookFilterStats());
        } finally {
            executorService.shutdown();
            assertTrue("processNotebook must have run before the test asserts on the captured site",
                    executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("only the configured site must be processed, got: " + capturedOwnerIds, List.of("site-1"), capturedOwnerIds);
        // Without this, a regression that enumerated the tenant *and* honoured site_id would still
        // pass the assertion above whenever the tenant happens to hold one site.
        org.mockito.Mockito.verify(client, org.mockito.Mockito.never()).getSites(any());
    }

    /**
     * Now that the site scope enumerates the tenant, one bad site must not take the rest with it:
     * a site with no id makes {@code Microsoft365Client.getNotebookPage} throw
     * {@code IllegalArgumentException}, which would escape the {@code getSites} consumer and end
     * the whole site scope. Pins that the sites after it are still crawled.
     */
    @Test
    public void test_storeSiteNotes_siteWithoutAnIdDoesNotStopTheOtherSites() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site broken = new Site();
        broken.setWebUrl("https://example.sharepoint.com/sites/broken");
        final Site usable = new Site();
        usable.setId("site-2");
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Site> consumer = invocation.getArgument(0);
            consumer.accept(broken);
            consumer.accept(usable);
            return null;
        }).when(client).getSites(any());

        final Notebook notebook = new Notebook();
        notebook.setId("notebook-1");
        notebook.setDisplayName("Site Notebook");
        final NotebookCollectionResponse response = new NotebookCollectionResponse();
        response.setValue(List.of(notebook));
        when(client.getNotebookPage(NotebookScope.SITE, "site-2")).thenReturn(response);

        final List<String> capturedOwnerIds = new ArrayList<>();
        final OneNoteDataStore captureDataStore = new OneNoteDataStore() {
            @Override
            protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
                    final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
                    final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
                capturedOwnerIds.add(ownerId);
            }
        };

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            captureDataStore.storeSiteNotes(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(),
                    executorService, client, new OneNoteDataStore.NotebookFilterStats());
        } finally {
            executorService.shutdown();
            assertTrue("processNotebook must have run before the test asserts on the captured site",
                    executorService.awaitTermination(5, TimeUnit.SECONDS));
        }

        assertEquals("the site after the broken one must still be crawled, got: " + capturedOwnerIds, List.of("site-2"), capturedOwnerIds);
    }

    /**
     * An {@link ApiException} carrying a status code. {@code ApiException.setResponseStatusCode}
     * is {@code protected}, so a status-bearing instance can only be built from a subclass.
     */
    private static final class StatusApiException extends ApiException {
        private static final long serialVersionUID = 1L;

        StatusApiException(final int statusCode, final String message) {
            super(message);
            setResponseStatusCode(statusCode);
        }
    }

    /**
     * The Microsoft Graph OneNote API stopped accepting app-only tokens on 2025-03-31 and answers
     * every {@code /onenote/} request made with one with a 401. {@code getNotebooks} used to log
     * that at {@code WARN} and return, so the crawl finished reporting success with zero documents
     * indexed and {@code ignore_error=false} had no effect at all -- exactly what the field report
     * showed. Pins that the default configuration now aborts the crawl instead.
     */
    @Test
    public void test_getNotebooks_appOnlyRejectionAbortsTheCrawl() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.isDelegatedAuth()).thenReturn(false);
        when(client.getNotebookPage(NotebookScope.SITE, "site-1"))
                .thenThrow(new StatusApiException(401, "The request does not contain a valid authentication token."));

        final OneNoteDataStore dataStore = new OneNoteDataStore();
        final DataStoreException e = assertThrows(DataStoreException.class,
                () -> dataStore.getNotebooks(client, new DataStoreParams(), NotebookScope.SITE, "site-1", notebook -> {}));

        // The message has to name the way out, not merely report a 401: the fix is a configuration
        // change, and an operator who only sees "401" will go looking at the client secret.
        assertTrue("the failure must name the app-only retirement: " + e.getMessage(), e.getMessage().contains("app-only"));
        assertTrue("the failure must name the username parameter: " + e.getMessage(),
                e.getMessage().contains(Microsoft365Client.USERNAME_PARAM));
        assertTrue("the failure must name the password parameter: " + e.getMessage(),
                e.getMessage().contains(Microsoft365Client.PASSWORD_PARAM));
    }

    /**
     * {@code ignore_error=true} is the operator asking for failures to be skipped, so the app-only
     * rejection must be logged and skipped rather than aborting -- the counterpart to
     * {@link #test_getNotebooks_appOnlyRejectionAbortsTheCrawl}. Either half regressing (always
     * throwing, or never throwing) fails exactly one of the two.
     */
    @Test
    public void test_getNotebooks_appOnlyRejectionIsSkippedWhenIgnoreErrorIsOn() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.isDelegatedAuth()).thenReturn(false);
        when(client.getNotebookPage(NotebookScope.GROUP, "group-1"))
                .thenThrow(new StatusApiException(401, "The request does not contain a valid authentication token."));

        final DataStoreParams params = new DataStoreParams();
        params.put("ignore_error", "true");

        final OneNoteDataStore dataStore = new OneNoteDataStore();
        // Must return normally rather than throw.
        dataStore.getNotebooks(client, params, NotebookScope.GROUP, "group-1", notebook -> {});
    }

    /**
     * A 401 against a delegated credential is an ordinary authentication failure -- a wrong
     * password, a revoked grant -- not the app-only retirement, and telling an operator to
     * configure the delegated parameters they already configured is worse than useless. Pins that
     * the app-only advice is keyed on the credential kind, not on the status code alone.
     */
    @Test
    public void test_getNotebooks_delegated401IsReportedAsAnOrdinaryFailure() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.isDelegatedAuth()).thenReturn(true);
        when(client.getNotebookPage(NotebookScope.USER, "user-1")).thenThrow(new StatusApiException(401, "invalid credentials"));

        final OneNoteDataStore dataStore = new OneNoteDataStore();
        final DataStoreException e = assertThrows(DataStoreException.class,
                () -> dataStore.getNotebooks(client, new DataStoreParams(), NotebookScope.USER, "user-1", notebook -> {}));
        assertFalse("a delegated 401 must not be blamed on the app-only retirement: " + e.getMessage(),
                e.getMessage().contains("app-only"));
    }

    /**
     * A 404 is routine -- a user with no provisioned personal site, a group with no notebook -- and
     * must keep skipping the owner rather than aborting the crawl now that other failures do not.
     * Without this, wiring {@code ignore_error} into {@code getNotebooks} would turn every
     * notebook-less user in the tenant into a crawl abort.
     */
    @Test
    public void test_getNotebooks_404StillSkipsTheOwner() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.getNotebookPage(NotebookScope.USER, "user-1")).thenThrow(new StatusApiException(404, "not found"));

        final OneNoteDataStore dataStore = new OneNoteDataStore();
        // Must return normally, with ignore_error left at its default of false.
        dataStore.getNotebooks(client, new DataStoreParams(), NotebookScope.USER, "user-1", notebook -> {});
    }

    /**
     * The configuration this data store now recommends -- a delegated service account -- reaches
     * owners it cannot see as a matter of course: a user who shared no notebook, a group it is not
     * a member of, a site it cannot read. Those must stay per-owner skips. Wiring
     * {@code ignore_error} into every non-404 status would have aborted the whole crawl on the
     * first such owner, which is worse than the silent green crawl this change set out to fix.
     */
    @Test
    public void test_getNotebooks_403SkipsTheOwnerRatherThanAbortingTheCrawl() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.isDelegatedAuth()).thenReturn(true);
        when(client.getNotebookPage(NotebookScope.USER, "user-1")).thenThrow(new StatusApiException(403, "accessDenied"));

        final OneNoteDataStore dataStore = new OneNoteDataStore();
        // Must return normally, with ignore_error left at its default of false.
        dataStore.getNotebooks(client, new DataStoreParams(), NotebookScope.USER, "user-1", notebook -> {});
    }

    /**
     * {@code storeGroupsNotes} wraps its {@code getNotebooks} call in a {@code catch (Exception)}
     * net. That net swallowed the abort {@code getNotebooks} now raises, so the group scope would
     * have gone on reporting a successful crawl of zero notebooks while the site scope aborted.
     * Pins that the abort reaches {@code storeData}'s caller through the group path too.
     */
    @Test
    public void test_storeGroupsNotes_appOnlyRejectionIsNotSwallowed() throws Exception {
        registerPermissionHelper();

        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.isDelegatedAuth()).thenReturn(false);

        final Group group = new Group();
        group.setId("group-1");
        group.setDisplayName("Group One");
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            final Consumer<Group> consumer = invocation.getArgument(0);
            consumer.accept(group);
            return null;
        }).when(client).getMicrosoft365Groups(any());
        when(client.getNotebookPage(NotebookScope.GROUP, "group-1"))
                .thenThrow(new StatusApiException(401, "The request does not contain a valid authentication token."));

        final OneNoteDataStore dataStore = new OneNoteDataStore();
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            assertThrows(DataStoreException.class, () -> dataStore.storeGroupsNotes(new DataConfig(), null, new DataStoreParams(),
                    new HashMap<>(), new HashMap<>(), executorService, client, new OneNoteDataStore.NotebookFilterStats()));
        } finally {
            executorService.shutdownNow();
        }
    }
}

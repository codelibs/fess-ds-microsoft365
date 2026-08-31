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

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.Site;

public class SharePointDocLibDataStoreTest extends UnitDsTestCase {

    private static final Logger logger = LogManager.getLogger(SharePointDocLibDataStoreTest.class);

    // for test
    public static final String tenant = "";
    public static final String clientId = "";
    public static final String clientSecret = "";

    private SharePointDocLibDataStore dataStore;

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
        dataStore = new SharePointDocLibDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        dataStore = null;
        super.tearDown(testInfo);
    }

    @Test
    public void test_getName() {
        assertEquals("SharePointDocLibDataStore", dataStore.getName());
    }

    @Test
    public void test_getSiteId() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "test-site-id");

        final String siteId = dataStore.getSiteId(paramMap);
        assertEquals("test-site-id", siteId);
    }

    @Test
    public void test_isExcludedSite() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_site_id", "site1,site2,site3");

        final Site site1 = new Site();
        site1.setId("site1");
        site1.setDisplayName("Site 1");

        final Site site2 = new Site();
        site2.setId("site4");
        site2.setDisplayName("Site 4");

        assertTrue(dataStore.isExcludedSite(paramMap, site1));
        assertFalse(dataStore.isExcludedSite(paramMap, site2));
    }

    @Test
    public void test_isSystemLibrary() {
        final Drive drive1 = new Drive();
        drive1.setName("Documents");
        drive1.setWebUrl("https://contoso.sharepoint.com/sites/test/Shared%20Documents");

        final Drive drive2 = new Drive();
        drive2.setName("Style Library");
        drive2.setWebUrl("https://contoso.sharepoint.com/sites/test/Style%20Library/");

        final Drive drive3 = new Drive();
        drive3.setName("Form Templates");
        drive3.setWebUrl("https://contoso.sharepoint.com/sites/test/Forms/AllItems.aspx");

        final Drive drive4 = new Drive();
        drive4.setName("_catalogs");
        drive4.setWebUrl("https://contoso.sharepoint.com/sites/test/_catalogs/masterpage");

        final Drive drive5 = new Drive();
        drive5.setName("FormServerTemplates");
        drive5.setWebUrl("https://contoso.sharepoint.com/sites/test/FormServerTemplates/");

        assertFalse(dataStore.isSystemLibrary(drive1));
        assertTrue(dataStore.isSystemLibrary(drive2));
        assertTrue(dataStore.isSystemLibrary(drive3));
        assertTrue(dataStore.isSystemLibrary(drive4));
        assertTrue(dataStore.isSystemLibrary(drive5));
    }

    @Test
    public void test_isSystemLibrary_multilingual() {
        // Test with non-English library names but system URLs
        final Drive drive1 = new Drive();
        drive1.setName("スタイル ライブラリ"); // Japanese for "Style Library"
        drive1.setWebUrl("https://contoso.sharepoint.com/sites/test/Style%20Library/");

        final Drive drive2 = new Drive();
        drive2.setName("Bibliothèque de styles"); // French for "Style Library"
        drive2.setWebUrl("https://contoso.sharepoint.com/sites/test/Style%20Library/");

        final Drive drive3 = new Drive();
        drive3.setName("Formulare"); // German for "Forms"
        drive3.setWebUrl("https://contoso.sharepoint.com/sites/test/Forms/AllItems.aspx");

        final Drive drive4 = new Drive();
        drive4.setName("ドキュメント"); // Japanese for "Documents"
        drive4.setWebUrl("https://contoso.sharepoint.com/sites/test/Shared%20Documents");

        // System libraries should be detected regardless of display name language
        assertTrue("Japanese Style Library should be detected as system", dataStore.isSystemLibrary(drive1));
        assertTrue("French Style Library should be detected as system", dataStore.isSystemLibrary(drive2));
        assertTrue("German Forms should be detected as system", dataStore.isSystemLibrary(drive3));
        assertFalse("Japanese Documents should not be detected as system", dataStore.isSystemLibrary(drive4));
    }

    @Test
    public void test_isSystemLibrary_nullWebUrl() {
        // Test behavior when webUrl is null
        final Drive drive = new Drive();
        drive.setName("Style Library");
        // webUrl is null

        assertFalse("Drive without webUrl should not be considered system library", dataStore.isSystemLibrary(drive));
    }

    @Test
    public void test_isIgnoreSystemLibraries() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_system_libraries", "true");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_system_libraries", "false");

        final DataStoreParams paramMap3 = new DataStoreParams();

        assertTrue(dataStore.isIgnoreSystemLibraries(paramMap1));
        assertFalse(dataStore.isIgnoreSystemLibraries(paramMap2));
        assertTrue(dataStore.isIgnoreSystemLibraries(paramMap3)); // default is true
    }

    @Test
    public void test_isIgnoreError() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_error", "true");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_error", "false");

        final DataStoreParams paramMap3 = new DataStoreParams();

        assertTrue(dataStore.isIgnoreError(paramMap1));
        assertFalse(dataStore.isIgnoreError(paramMap2));
        assertFalse(dataStore.isIgnoreError(paramMap3)); // default is false
    }

    @Test
    public void test_isExcludedSite_multipleSites() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_site_id", "site1,site2, site3 ");

        final Site excludedSite1 = new Site();
        excludedSite1.setId("site1");
        excludedSite1.setDisplayName("Excluded Site 1");

        final Site excludedSite2 = new Site();
        excludedSite2.setId("site2");
        excludedSite2.setDisplayName("Excluded Site 2");

        final Site excludedSite3 = new Site();
        excludedSite3.setId("site3");
        excludedSite3.setDisplayName("Excluded Site 3");

        final Site allowedSite = new Site();
        allowedSite.setId("site4");
        allowedSite.setDisplayName("Allowed Site");

        assertTrue("Site 1 should be excluded", dataStore.isExcludedSite(paramMap, excludedSite1));
        assertTrue("Site 2 should be excluded", dataStore.isExcludedSite(paramMap, excludedSite2));
        assertTrue("Site 3 should be excluded", dataStore.isExcludedSite(paramMap, excludedSite3));
        assertFalse("Site 4 should not be excluded", dataStore.isExcludedSite(paramMap, allowedSite));
    }

    @Test
    public void test_isExcludedSite_emptyExcludeList() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("exclude_site_id", "");

        final DataStoreParams paramMap2 = new DataStoreParams();
        // No exclude_site_id parameter set

        final Site site = new Site();
        site.setId("any-site-id");
        site.setDisplayName("Any Site");

        assertFalse("Site should not be excluded with empty exclude list", dataStore.isExcludedSite(paramMap1, site));
        assertFalse("Site should not be excluded with no exclude parameter", dataStore.isExcludedSite(paramMap2, site));
    }

    @Test
    public void test_threadPoolCreation() {
        // Test that number_of_threads parameter is correctly parsed
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("number_of_threads", "1");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("number_of_threads", "5");

        final DataStoreParams paramMap3 = new DataStoreParams();
        // No number_of_threads parameter - should default to 1

        assertEquals("Should parse number_of_threads=1", "1", paramMap1.getAsString("number_of_threads", "1"));
        assertEquals("Should parse number_of_threads=5", "5", paramMap2.getAsString("number_of_threads", "1"));
        assertEquals("Should default to 1 when not specified", "1", paramMap3.getAsString("number_of_threads", "1"));

        // Test that the parameter gets parsed as an integer without exceptions
        try {
            Integer.parseInt(paramMap1.getAsString("number_of_threads", "1"));
            Integer.parseInt(paramMap2.getAsString("number_of_threads", "1"));
            Integer.parseInt(paramMap3.getAsString("number_of_threads", "1"));
        } catch (NumberFormatException e) {
            fail("Should be able to parse number_of_threads as integer");
        }
    }

    @Test
    public void test_isExcludedSite_sharePointSiteIdWithCommas() {
        // Test with SharePoint site IDs that contain commas
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_site_id",
                "n2smdev6.sharepoint.com,684d3f1a-a382-4368-b4f5-94b98baabcf3,12048305-5e53-421e-bd6c-32af610f6d8a");

        final Site excludedSite = new Site();
        excludedSite.setId("n2smdev6.sharepoint.com,684d3f1a-a382-4368-b4f5-94b98baabcf3,12048305-5e53-421e-bd6c-32af610f6d8a");
        excludedSite.setDisplayName("Test1 Site");

        final Site allowedSite = new Site();
        allowedSite.setId("anotherdomain.sharepoint.com,123e4567-e89b-12d3-a456-426614174000,98765432-1234-5678-9abc-def012345678");
        allowedSite.setDisplayName("Allowed Site");

        assertTrue("SharePoint site with comma-containing ID should be excluded", dataStore.isExcludedSite(paramMap, excludedSite));
        assertFalse("Different SharePoint site should not be excluded", dataStore.isExcludedSite(paramMap, allowedSite));
    }

    @Test
    public void test_isExcludedSite_multipleSharePointSiteIdsWithSemicolon() {
        // Test multiple SharePoint site IDs separated by semicolon
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_site_id",
                "n2smdev6.sharepoint.com,684d3f1a-a382-4368-b4f5-94b98baabcf3,12048305-5e53-421e-bd6c-32af610f6d8a;otherdomain.sharepoint.com,123e4567-e89b-12d3-a456-426614174000,98765432-1234-5678-9abc-def012345678");

        final Site excludedSite1 = new Site();
        excludedSite1.setId("n2smdev6.sharepoint.com,684d3f1a-a382-4368-b4f5-94b98baabcf3,12048305-5e53-421e-bd6c-32af610f6d8a");
        excludedSite1.setDisplayName("Test1 Site");

        final Site excludedSite2 = new Site();
        excludedSite2.setId("otherdomain.sharepoint.com,123e4567-e89b-12d3-a456-426614174000,98765432-1234-5678-9abc-def012345678");
        excludedSite2.setDisplayName("Test2 Site");

        final Site allowedSite = new Site();
        allowedSite.setId("alloweddomain.sharepoint.com,aaa4567-e89b-12d3-a456-426614174000,11111111-1234-5678-9abc-def012345678");
        allowedSite.setDisplayName("Allowed Site");

        assertTrue("First SharePoint site should be excluded", dataStore.isExcludedSite(paramMap, excludedSite1));
        assertTrue("Second SharePoint site should be excluded", dataStore.isExcludedSite(paramMap, excludedSite2));
        assertFalse("Different SharePoint site should not be excluded", dataStore.isExcludedSite(paramMap, allowedSite));
    }

    @Test
    public void test_documentLibraryCrawling_parameters() {
        // Test that parameters needed for document library crawling are available
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "doclib-test-site");
        paramMap.put("ignore_system_libraries", "true");

        // Verify configuration parameters for document library crawling
        assertEquals("Should get site ID for document library context", "doclib-test-site", paramMap.getAsString("site_id"));
        assertTrue("Should ignore system libraries by default", dataStore.isIgnoreSystemLibraries(paramMap));
    }

    @Test
    public void test_documentLibraryMetadata_configuration() {
        // Test that document library metadata collection parameters are properly configured
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "metadata-test-site");
        paramMap.put("ignore_system_libraries", "false"); // Include system libraries for testing
        paramMap.put("number_of_threads", "3");

        assertEquals("Should get site ID for document library enumeration", "metadata-test-site", paramMap.getAsString("site_id"));
        assertFalse("Should include system libraries when configured", dataStore.isIgnoreSystemLibraries(paramMap));
        assertEquals("Should get thread count", "3", paramMap.getAsString("number_of_threads", "1"));
    }

    @Test
    public void test_isTargetLibrary_noFilterConfigured() {
        final Site site = new Site();
        site.setWebUrl("https://contoso.sharepoint.com/sites/test");

        final Drive drive = new Drive();
        drive.setName("Documents");
        drive.setWebUrl("https://contoso.sharepoint.com/sites/test/Shared%20Documents");

        assertTrue("No URL filter configured should crawl every library, matching pre-existing behavior",
                dataStore.isTargetLibrary(null, site, drive));
    }

    @Test
    public void test_isTargetLibrary_filterAcceptsMatchingLibrary() {
        final Site site = new Site();
        site.setWebUrl("https://contoso.sharepoint.com/sites/test");

        final Drive drive = new Drive();
        drive.setName("Marketing Assets");
        // Deliberately different from the canonical URL so the assertions below prove which URL is filtered.
        drive.setWebUrl("https://contoso.sharepoint.com/sites/test/_layouts/15/Doc.aspx?id=1");

        final UrlFilter urlFilter = mock(UrlFilter.class);
        final String canonicalUrl = dataStore.generateDocumentLibraryUrl(site, drive);
        when(urlFilter.match(canonicalUrl)).thenReturn(true);

        assertTrue("A library whose canonical URL is accepted by the filter must still be crawled",
                dataStore.isTargetLibrary(urlFilter, site, drive));
        verify(urlFilter).match(canonicalUrl);
    }

    @Test
    public void test_isTargetLibrary_filterRejectsNonMatchingLibrary() {
        final Site site = new Site();
        site.setWebUrl("https://contoso.sharepoint.com/sites/test");

        final Drive drive = new Drive();
        drive.setName("Marketing Assets");
        drive.setWebUrl("https://contoso.sharepoint.com/sites/test/_layouts/15/Doc.aspx?id=1");

        final UrlFilter urlFilter = mock(UrlFilter.class);
        final String canonicalUrl = dataStore.generateDocumentLibraryUrl(site, drive);
        when(urlFilter.match(canonicalUrl)).thenReturn(false);

        assertFalse("A library rejected by the URL filter must not be crawled", dataStore.isTargetLibrary(urlFilter, site, drive));
        verify(urlFilter).match(canonicalUrl);
    }

    @Test
    public void test_isTargetLibrary_filtersOnCanonicalUrlNotRawWebUrl() {
        final Site site = new Site();
        site.setWebUrl("https://contoso.sharepoint.com/sites/test");

        final Drive drive = new Drive();
        drive.setName("Marketing Assets");
        drive.setWebUrl("https://contoso.sharepoint.com/sites/test/_layouts/15/Doc.aspx?id=1");

        final String canonicalUrl = dataStore.generateDocumentLibraryUrl(site, drive);
        assertFalse("Test setup should keep the raw webUrl distinct from the canonical URL", drive.getWebUrl().equals(canonicalUrl));

        final UrlFilter urlFilter = mock(UrlFilter.class);
        // Only the raw webUrl matches; the canonical (indexed doclib.url) URL does not.
        when(urlFilter.match(drive.getWebUrl())).thenReturn(true);
        when(urlFilter.match(canonicalUrl)).thenReturn(false);

        assertFalse("isTargetLibrary must filter on the canonical URL indexed as doclib.url, not drive.getWebUrl()",
                dataStore.isTargetLibrary(urlFilter, site, drive));
    }

    @Test
    public void test_getUrlFilter_wiresIncludeAndExcludePatterns() {
        final UrlFilter mockFilter = mock(UrlFilter.class);
        ComponentUtil.register(mockFilter, UrlFilter.class.getCanonicalName());

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_pattern", "https://contoso\\.sharepoint\\.com/sites/allowed/.*");
        paramMap.put("exclude_pattern", "https://contoso\\.sharepoint\\.com/sites/blocked/.*");

        final UrlFilter result = dataStore.getUrlFilter(paramMap);

        assertEquals(mockFilter, result);
        verify(mockFilter).addInclude("https://contoso\\.sharepoint\\.com/sites/allowed/.*");
        verify(mockFilter).addExclude("https://contoso\\.sharepoint\\.com/sites/blocked/.*");
        verify(mockFilter).init(null);
    }

    @Test
    public void test_getUrlFilter_leavesPatternsUnsetWhenNotConfigured() {
        final UrlFilter mockFilter = mock(UrlFilter.class);
        ComponentUtil.register(mockFilter, UrlFilter.class.getCanonicalName());

        final UrlFilter result = dataStore.getUrlFilter(new DataStoreParams());

        assertEquals(mockFilter, result);
        verify(mockFilter, never()).addInclude(anyString());
        verify(mockFilter, never()).addExclude(anyString());
        verify(mockFilter).init(null);
    }

    @Test
    public void testStoreData() {
        // This test requires actual Microsoft 365 credentials and would be integration test
        // Uncomment and provide credentials for actual testing

        /*
        if (tenant.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
            logger.info("Skip testStoreData because credentials are not set.");
            return;
        }

        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("tenant", tenant);
        paramMap.put("client_id", clientId);
        paramMap.put("client_secret", clientSecret);
        paramMap.put("number_of_threads", "1");
        paramMap.put("ignore_error", "true");
        paramMap.put("site_id", "root"); // Test with root site

        final TestCallback callback = new TestCallback();

        dataStore.storeData(null, callback, paramMap, scriptMap, defaultDataMap);

        logger.info("Callback count: {}", callback.getCount());
        assertTrue(callback.getCount() > 0);
        */
    }

    /**
     * A document library's ACL is assembled from the drive's own Graph permissions, the
     * operator-configured {@code default_permissions}, and the data config's own Permissions field
     * (seeded into {@code defaultDataMap} under the role index field).
     *
     * <p>Nothing in this class asserted the roles a document library is actually indexed with, so
     * dropping either the {@code default_permissions} step or the {@code defaultDataMap} fold
     * narrowed every document library's ACL with the suite green. Pins all three contributions and
     * their order.</p>
     */
    @Test
    public void test_storeDocumentLibrary_assemblesRolesFromAllThreeSources() {
        final SystemHelper systemHelper = new SystemHelper();
        ComponentUtil.register(systemHelper, "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        final TestablePermissionHelper permissionHelper = new TestablePermissionHelper();
        permissionHelper.useSystemHelper(systemHelper);
        ComponentUtil.register(permissionHelper, "permissionHelper");

        // convertValue's real path goes through ComponentUtil.getScriptEngineFactory(), which this
        // unit test has no business standing up -- see OneNoteDataStoreTest's identical seam.
        // "doclib.roles" is the only template used here, so it is resolved with a direct nested
        // map lookup instead; storeDocumentLibrary itself, including the role assembly under test,
        // runs completely unmodified. getDrivePermissions is stubbed because it is the only member
        // that would reach Graph.
        final SharePointDocLibDataStore roleAwareDataStore = new SharePointDocLibDataStore() {
            @Override
            protected List<String> getDrivePermissions(final Microsoft365Client client, final String driveId,
                    final DataStoreParams paramMap) {
                return new ArrayList<>(List.of("1drive-permission"));
            }

            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                if ("doclib.roles".equals(template) && resultMap.get(DOCLIB) instanceof final Map<?, ?> docLibMap) {
                    return docLibMap.get(DOCLIB_ROLES);
                }
                return super.convertValue(scriptType, template, resultMap);
            }
        };

        final Site site = new Site();
        site.setId("site-1");
        site.setDisplayName("Site");

        final Drive drive = new Drive();
        drive.setId("drive-1");
        drive.setName("Documents");
        drive.setWebUrl("https://example.sharepoint.com/sites/site-1/Shared%20Documents");

        final String roleField = ComponentUtil.getFessConfig().getIndexFieldRole();
        final Map<String, Object> defaultDataMap = new HashMap<>();
        defaultDataMap.put(roleField, List.of("1config-role"));

        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put(roleField, "doclib.roles");

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(SharePointDocLibDataStore.DEFAULT_PERMISSIONS, "{role}admin,{group}sales");

        final TestCallback callback = new TestCallback();
        roleAwareDataStore.storeDocumentLibrary(new DataConfig(), callback, new HashMap<>(), paramMap, scriptMap, defaultDataMap, null,
                site, drive);

        assertEquals("storeDocumentLibrary must have indexed the library exactly once", 1, callback.getCount());

        @SuppressWarnings("unchecked")
        final List<String> roles = (List<String>) callback.getLastDataMap().get(roleField);
        assertEquals("the library's ACL must hold, in order: drive permissions, default_permissions, then the config's own roles", List
                .of("1drive-permission", permissionHelper.encode("{role}admin"), permissionHelper.encode("{group}sales"), "1config-role"),
                roles);
    }

    /**
     * {@code PermissionHelper#systemHelper} is {@code @Resource}-injected, which plain
     * {@code ComponentUtil.register(...)} does not perform in this minimal test container; this
     * subclass exposes a same-package-crossing setter so the field can be wired by hand.
     */
    private static final class TestablePermissionHelper extends PermissionHelper {
        void useSystemHelper(final SystemHelper systemHelper) {
            this.systemHelper = systemHelper;
        }
    }

    /**
     * {@code getUrlFilter} hands include_pattern/exclude_pattern to fess-crawler's
     * {@code UrlFilterImpl}, which logs one WARN for a pattern that does not compile and then
     * drops it - leaving the crawl running with no filter, so a mistyped {@code exclude_pattern}
     * indexes exactly what it was meant to keep out. Pins that the crawl fails at its start
     * instead, the same way the three {@code getPattern} DataStores now do.
     */
    @Test
    public void test_storeData_malformedExcludePatternFailsBeforeAnyGraphCall() {
        final java.util.concurrent.atomic.AtomicInteger clientsCreated = new java.util.concurrent.atomic.AtomicInteger();
        final SharePointDocLibDataStore testDataStore = new SharePointDocLibDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                clientsCreated.incrementAndGet();
                throw new AssertionError("storeData must fail on the malformed pattern before creating a client");
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", ".*secret.*[");

        final DataStoreException e = assertThrows(DataStoreException.class,
                () -> testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>()));
        assertTrue("the failure must name the parameter, got: " + e.getMessage(), e.getMessage().contains("exclude_pattern"));
        assertEquals("no Graph client may be created for a crawl that cannot honour its own filter", 0, clientsCreated.get());
    }

    private static class TestCallback implements IndexUpdateCallback {
        private int count = 0;
        private Map<String, Object> lastDataMap;

        @Override
        public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
            count++;
            lastDataMap = new HashMap<>(dataMap);
            logger.info("Stored document {}: {}", count, dataMap.get("url"));
        }

        @Override
        public long getExecuteTime() {
            return 0;
        }

        @Override
        public long getDocumentSize() {
            return 0;
        }

        @Override
        public void commit() {
            // do nothing
        }

        public int getCount() {
            return count;
        }

        public Map<String, Object> getLastDataMap() {
            return lastDataMap;
        }
    }
}
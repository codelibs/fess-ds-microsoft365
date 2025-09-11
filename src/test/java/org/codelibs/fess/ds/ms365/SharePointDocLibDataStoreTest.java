/*
 * Copyright 2012-2024 CodeLibs Project and the Others.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.Site;

public class SharePointDocLibDataStoreTest extends LastaFluteTestCase {

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
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new SharePointDocLibDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        dataStore = null;
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("SharePointDocLibDataStore", dataStore.getName());
    }

    public void test_getSiteId() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "test-site-id");

        final String siteId = dataStore.getSiteId(paramMap);
        assertEquals("test-site-id", siteId);
    }

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

    public void test_isSystemLibrary_nullWebUrl() {
        // Test behavior when webUrl is null
        final Drive drive = new Drive();
        drive.setName("Style Library");
        // webUrl is null

        assertFalse("Drive without webUrl should not be considered system library", dataStore.isSystemLibrary(drive));
    }

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

    public void test_isIgnoreFolder() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_folder", "true");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_folder", "false");

        final DataStoreParams paramMap3 = new DataStoreParams();

        assertTrue(dataStore.isIgnoreFolder(paramMap1));
        assertFalse(dataStore.isIgnoreFolder(paramMap2));
        assertTrue(dataStore.isIgnoreFolder(paramMap3)); // default is true
    }

    public void test_getMaxSize() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("max_content_length", "1048576");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("max_content_length", "invalid");

        final DataStoreParams paramMap3 = new DataStoreParams();

        assertEquals(1048576L, dataStore.getMaxSize(paramMap1));
        assertEquals(10485760L, dataStore.getMaxSize(paramMap2)); // default on invalid
        assertEquals(10485760L, dataStore.getMaxSize(paramMap3)); // default
    }

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

    public void test_getMaxSize_customValues() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("max_content_length", "5242880"); // 5MB

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("max_content_length", "20971520"); // 20MB

        final DataStoreParams paramMap3 = new DataStoreParams();
        paramMap3.put("max_content_length", "0"); // 0 bytes

        assertEquals("Should parse 5MB correctly", 5242880L, dataStore.getMaxSize(paramMap1));
        assertEquals("Should parse 20MB correctly", 20971520L, dataStore.getMaxSize(paramMap2));
        assertEquals("Should parse 0 bytes correctly", 0L, dataStore.getMaxSize(paramMap3));
    }

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

    public void test_documentLibraryCrawling_parameters() {
        // Test that parameters needed for document library crawling are available
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "doclib-test-site");
        paramMap.put("ignore_system_libraries", "true");
        paramMap.put("max_content_length", "5242880");

        // Verify configuration parameters for document library crawling
        assertEquals("Should get site ID for document library context", "doclib-test-site", paramMap.getAsString("site_id"));
        assertTrue("Should ignore system libraries by default", dataStore.isIgnoreSystemLibraries(paramMap));
        assertEquals("Should get max content length", 5242880L, dataStore.getMaxSize(paramMap));
    }

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
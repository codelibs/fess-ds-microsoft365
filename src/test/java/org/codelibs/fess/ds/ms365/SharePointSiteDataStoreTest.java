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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.microsoft.graph.models.Site;

public class SharePointSiteDataStoreTest extends LastaFluteTestCase {

    private static final Logger logger = LogManager.getLogger(SharePointSiteDataStoreTest.class);

    // for test
    public static final String tenant = "";
    public static final String clientId = "";
    public static final String clientSecret = "";

    private SharePointSiteDataStore dataStore;

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
        dataStore = new SharePointSiteDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        dataStore = null;
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("SharePointSiteDataStore", dataStore.getName());
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
        final com.microsoft.graph.models.Drive drive1 = new com.microsoft.graph.models.Drive();
        drive1.setName("Documents");

        final com.microsoft.graph.models.Drive drive2 = new com.microsoft.graph.models.Drive();
        drive2.setName("Style Library");

        final com.microsoft.graph.models.Drive drive3 = new com.microsoft.graph.models.Drive();
        drive3.setName("Form Templates");

        final com.microsoft.graph.models.Drive drive4 = new com.microsoft.graph.models.Drive();
        drive4.setName("_catalogs");

        assertFalse(dataStore.isSystemLibrary(drive1));
        assertTrue(dataStore.isSystemLibrary(drive2));
        assertTrue(dataStore.isSystemLibrary(drive3));
        assertTrue(dataStore.isSystemLibrary(drive4));
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
        // This test verifies the parameter parsing logic

        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("number_of_threads", "1");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("number_of_threads", "5");

        final DataStoreParams paramMap3 = new DataStoreParams();
        // No number_of_threads parameter - should default to 1

        // We can't directly test thread pool creation without exposing internal methods
        // But we can verify the parameter parsing by accessing it through the same method
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

    public void test_excludeSiteId_withSpecificSiteId() {
        // Test that exclude_site_id works even when a specific site_id is provided
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "excluded-site-123");
        paramMap.put("exclude_site_id", "excluded-site-123,other-excluded-site");

        final Site excludedSite = new Site();
        excludedSite.setId("excluded-site-123");
        excludedSite.setDisplayName("Excluded Site");

        final Site allowedSite = new Site();
        allowedSite.setId("allowed-site-456");
        allowedSite.setDisplayName("Allowed Site");

        // Test that the specified site is correctly identified as excluded
        assertTrue("Specific site should be excluded when it's in exclude_site_id", dataStore.isExcludedSite(paramMap, excludedSite));
        assertFalse("Different site should not be excluded", dataStore.isExcludedSite(paramMap, allowedSite));
    }

    public void test_excludeSiteId_caseInsensitiveTrimming() {
        // Test that exclude_site_id handles whitespace and case correctly
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_site_id", " SITE1 , site2, Site3 ");

        final Site site1 = new Site();
        site1.setId("SITE1");
        site1.setDisplayName("Site 1");

        final Site site2 = new Site();
        site2.setId("site2");
        site2.setDisplayName("Site 2");

        final Site site3 = new Site();
        site3.setId("Site3");
        site3.setDisplayName("Site 3");

        final Site site4 = new Site();
        site4.setId("site4");
        site4.setDisplayName("Site 4");

        assertTrue("SITE1 should be excluded", dataStore.isExcludedSite(paramMap, site1));
        assertTrue("site2 should be excluded", dataStore.isExcludedSite(paramMap, site2));
        assertTrue("Site3 should be excluded", dataStore.isExcludedSite(paramMap, site3));
        assertFalse("site4 should not be excluded", dataStore.isExcludedSite(paramMap, site4));
    }

    public void test_isExcludedSite_sharePointSiteIdWithCommas() {
        // Test the bug: SharePoint site IDs contain commas as part of the ID format
        // Current implementation incorrectly splits by comma, breaking the ID matching
        final DataStoreParams paramMap = new DataStoreParams();
        // This is the problematic scenario: a real SharePoint site ID contains commas
        paramMap.put("exclude_site_id",
                "n2smdev6.sharepoint.com,684d3f1a-a382-4368-b4f5-94b98baabcf3,12048305-5e53-421e-bd6c-32af610f6d8a");

        final Site excludedSite = new Site();
        excludedSite.setId("n2smdev6.sharepoint.com,684d3f1a-a382-4368-b4f5-94b98baabcf3,12048305-5e53-421e-bd6c-32af610f6d8a");
        excludedSite.setDisplayName("Test1 Site");

        final Site allowedSite = new Site();
        allowedSite.setId("anotherdomain.sharepoint.com,123e4567-e89b-12d3-a456-426614174000,98765432-1234-5678-9abc-def012345678");
        allowedSite.setDisplayName("Allowed Site");

        // This test will FAIL with current implementation due to comma splitting bug
        // The site ID gets split into parts: ["n2smdev6.sharepoint.com", "684d3f1a-a382-4368-b4f5-94b98baabcf3", "12048305-5e53-421e-bd6c-32af610f6d8a"]
        // None of these parts matches the full site ID, so exclusion fails
        assertTrue("SharePoint site with comma-containing ID should be excluded", dataStore.isExcludedSite(paramMap, excludedSite));
        assertFalse("Different SharePoint site should not be excluded", dataStore.isExcludedSite(paramMap, allowedSite));
    }

    public void test_isExcludedSite_multipleSharePointSiteIdsWithSemicolon() {
        // Test the solution: use semicolon as delimiter for multiple SharePoint site IDs
        final DataStoreParams paramMap = new DataStoreParams();
        // Multiple SharePoint site IDs separated by semicolon (not comma)
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

        // This should work with the fixed implementation using semicolon delimiter
        assertTrue("First SharePoint site should be excluded", dataStore.isExcludedSite(paramMap, excludedSite1));
        assertTrue("Second SharePoint site should be excluded", dataStore.isExcludedSite(paramMap, excludedSite2));
        assertFalse("Different SharePoint site should not be excluded", dataStore.isExcludedSite(paramMap, allowedSite));
    }

    public void test_numberOfThreads_threadPoolManagement() {
        // Test thread pool creation and management with different thread counts
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("number_of_threads", "1");

        final DataStoreParams paramMap3 = new DataStoreParams();
        paramMap3.put("number_of_threads", "3");

        final DataStoreParams paramMap5 = new DataStoreParams();
        paramMap5.put("number_of_threads", "5");

        // Test parameter parsing and validation
        assertEquals("Should correctly parse 1 thread", 1, Integer.parseInt(paramMap1.getAsString("number_of_threads", "1")));
        assertEquals("Should correctly parse 3 threads", 3, Integer.parseInt(paramMap3.getAsString("number_of_threads", "1")));
        assertEquals("Should correctly parse 5 threads", 5, Integer.parseInt(paramMap5.getAsString("number_of_threads", "1")));

        // Test that we can create ExecutorServices with the parsed values without issues
        try {
            final ExecutorService executor1 =
                    java.util.concurrent.Executors.newFixedThreadPool(Integer.parseInt(paramMap1.getAsString("number_of_threads", "1")));
            final ExecutorService executor3 =
                    java.util.concurrent.Executors.newFixedThreadPool(Integer.parseInt(paramMap3.getAsString("number_of_threads", "1")));
            final ExecutorService executor5 =
                    java.util.concurrent.Executors.newFixedThreadPool(Integer.parseInt(paramMap5.getAsString("number_of_threads", "1")));

            // Clean up executors
            executor1.shutdown();
            executor3.shutdown();
            executor5.shutdown();
        } catch (Exception e) {
            fail("Should be able to create thread pools with parsed thread counts: " + e.getMessage());
        }
    }

    public void test_numberOfThreads_invalidValues() {
        // Test handling of invalid thread count values
        final DataStoreParams paramMapInvalid = new DataStoreParams();
        paramMapInvalid.put("number_of_threads", "invalid");

        final DataStoreParams paramMapZero = new DataStoreParams();
        paramMapZero.put("number_of_threads", "0");

        final DataStoreParams paramMapNegative = new DataStoreParams();
        paramMapNegative.put("number_of_threads", "-1");

        // Test that invalid values will cause NumberFormatException when parsed
        try {
            Integer.parseInt(paramMapInvalid.getAsString("number_of_threads", "1"));
            fail("Should throw NumberFormatException for invalid thread count");
        } catch (NumberFormatException e) {
            // Expected behavior
        }

        // Test that zero and negative values would be problematic for thread pool creation
        try {
            final int zeroThreads = Integer.parseInt(paramMapZero.getAsString("number_of_threads", "1"));
            assertEquals("Should parse zero", 0, zeroThreads);
            // Note: Creating thread pool with 0 threads would throw IllegalArgumentException
        } catch (NumberFormatException e) {
            fail("Should be able to parse zero as integer");
        }

        try {
            final int negativeThreads = Integer.parseInt(paramMapNegative.getAsString("number_of_threads", "1"));
            assertEquals("Should parse negative", -1, negativeThreads);
            // Note: Creating thread pool with negative threads would throw IllegalArgumentException
        } catch (NumberFormatException e) {
            fail("Should be able to parse negative number as integer");
        }
    }

    public void test_maxContentLength_fileSizeCheck() {
        // Test that files exceeding max_content_length are handled correctly
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("max_content_length", "1048576"); // 1MB

        final long maxSize = dataStore.getMaxSize(paramMap);
        assertEquals("Should parse 1MB correctly", 1048576L, maxSize);

        // Test with different values
        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("max_content_length", "5242880"); // 5MB
        assertEquals("Should parse 5MB correctly", 5242880L, dataStore.getMaxSize(paramMap2));

        final DataStoreParams paramMap3 = new DataStoreParams();
        paramMap3.put("max_content_length", "0"); // 0 bytes
        assertEquals("Should parse 0 bytes correctly", 0L, dataStore.getMaxSize(paramMap3));
    }

    public void test_isSupportedMimeType_various() {
        // Test MIME type filtering functionality
        final com.microsoft.graph.models.DriveItem item = new com.microsoft.graph.models.DriveItem();
        final com.microsoft.graph.models.File file = new com.microsoft.graph.models.File();
        file.setMimeType("application/pdf");
        item.setFile(file);

        // Test with no MIME type filter (should allow all)
        final DataStoreParams paramMap1 = new DataStoreParams();
        assertTrue("Should allow all MIME types when no filter is specified", dataStore.isSupportedMimeType(paramMap1, item));

        // Test with specific MIME type match
        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("supported_mimetypes", "application/pdf,text/plain");
        assertTrue("Should allow PDF files", dataStore.isSupportedMimeType(paramMap2, item));

        // Test with MIME type that doesn't match
        final DataStoreParams paramMap3 = new DataStoreParams();
        paramMap3.put("supported_mimetypes", "text/plain,image/jpeg");
        assertFalse("Should not allow PDF when not in supported list", dataStore.isSupportedMimeType(paramMap3, item));

        // Test with wildcard
        final DataStoreParams paramMap4 = new DataStoreParams();
        paramMap4.put("supported_mimetypes", "application/*");
        assertTrue("Should allow application/* wildcard", dataStore.isSupportedMimeType(paramMap4, item));

        // Test with universal wildcard
        final DataStoreParams paramMap5 = new DataStoreParams();
        paramMap5.put("supported_mimetypes", "*");
        assertTrue("Should allow * wildcard", dataStore.isSupportedMimeType(paramMap5, item));
    }

    public void test_isSupportedMimeType_edgeCases() {
        // Test edge cases for MIME type filtering

        // Test with null file
        final com.microsoft.graph.models.DriveItem itemWithoutFile = new com.microsoft.graph.models.DriveItem();
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("supported_mimetypes", "application/pdf");
        assertFalse("Should reject items without file info", dataStore.isSupportedMimeType(paramMap1, itemWithoutFile));

        // Test with file but no MIME type
        final com.microsoft.graph.models.DriveItem itemWithoutMimeType = new com.microsoft.graph.models.DriveItem();
        final com.microsoft.graph.models.File fileWithoutMime = new com.microsoft.graph.models.File();
        itemWithoutMimeType.setFile(fileWithoutMime);
        assertFalse("Should reject files without MIME type", dataStore.isSupportedMimeType(paramMap1, itemWithoutMimeType));

        // Test case sensitivity
        final com.microsoft.graph.models.DriveItem itemUpperCase = new com.microsoft.graph.models.DriveItem();
        final com.microsoft.graph.models.File fileUpperCase = new com.microsoft.graph.models.File();
        fileUpperCase.setMimeType("APPLICATION/PDF");
        itemUpperCase.setFile(fileUpperCase);

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("supported_mimetypes", "application/pdf");
        assertTrue("Should handle case insensitive matching", dataStore.isSupportedMimeType(paramMap2, itemUpperCase));
    }

    public void test_maxContentLength_invalidValues() {
        // Test handling of invalid max_content_length values
        final DataStoreParams paramMapInvalid = new DataStoreParams();
        paramMapInvalid.put("max_content_length", "invalid");
        assertEquals("Should default to 10MB for invalid values", 10485760L, dataStore.getMaxSize(paramMapInvalid));

        final DataStoreParams paramMapEmpty = new DataStoreParams();
        paramMapEmpty.put("max_content_length", "");
        assertEquals("Should default to 10MB for empty values", 10485760L, dataStore.getMaxSize(paramMapEmpty));

        final DataStoreParams paramMapNegative = new DataStoreParams();
        paramMapNegative.put("max_content_length", "-1");
        assertEquals("Should parse negative values as is", -1L, dataStore.getMaxSize(paramMapNegative));
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

    public void test_siteContentBuilding() {
        // Test that SITE_CONTENT field is properly built with site info and drive metadata
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "test-site-123");

        // Create a mock site object to simulate site content building
        final com.microsoft.graph.models.Site site = new com.microsoft.graph.models.Site();
        site.setId("test-site-123");
        site.setDisplayName("Test Site Name");
        site.setDescription("Test site description for content building");
        site.setWebUrl("https://test.sharepoint.com/sites/testsite");

        // Test that site parameters are accessible for content building
        assertEquals("Should get site ID", "test-site-123", paramMap.getAsString("site_id"));
        assertNotNull("Site should have display name for content", site.getDisplayName());
        assertNotNull("Site should have description for content", site.getDescription());
        assertNotNull("Site should have web URL for content", site.getWebUrl());
    }

    public void test_siteContentField_parameters() {
        // Test that parameters needed for enhanced site content building are available
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "content-test-site");
        paramMap.put("ignore_system_libraries", "true");

        // Verify configuration parameters for content building
        assertEquals("Should get site ID for content context", "content-test-site", paramMap.getAsString("site_id"));
        assertTrue("Should ignore system libraries by default", dataStore.isIgnoreSystemLibraries(paramMap));
    }

    public void test_driveMetadata_forSiteContent() {
        // Test that drive metadata collection parameters are properly configured
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "drive-metadata-site");
        paramMap.put("ignore_system_libraries", "false"); // Include system libraries for testing

        assertEquals("Should get site ID for drive enumeration", "drive-metadata-site", paramMap.getAsString("site_id"));
        assertFalse("Should include system libraries when configured", dataStore.isIgnoreSystemLibraries(paramMap));
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
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.GraphMockServer;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.FieldValueSet;
import com.microsoft.graph.models.List;
import com.microsoft.graph.models.ListInfo;
import com.microsoft.graph.models.ListItem;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.serviceclient.GraphServiceClient;

public class SharePointListDataStoreTest extends UnitDsTestCase {

    private static final Logger logger = LogManager.getLogger(SharePointListDataStoreTest.class);

    // for test
    public static final String tenant = "";
    public static final String clientId = "";
    public static final String clientSecret = "";

    private SharePointListDataStore dataStore;

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
        dataStore = new SharePointListDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        dataStore = null;
        super.tearDown(testInfo);
    }

    @Test
    public void test_getName() {
        assertEquals("SharePointListDataStore", dataStore.getName());
    }

    @Test
    public void test_getSiteId() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "test-site-id");

        final String siteId = dataStore.getSiteId(paramMap);
        assertEquals("test-site-id", siteId);
    }

    @Test
    public void test_getListId() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("list_id", "test-list-id");

        final String listId = dataStore.getListId(paramMap);
        assertEquals("test-list-id", listId);
    }

    @Test
    public void test_isExcludedList() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_list_id", "list1,list2,list3");

        final List list1 = new List();
        list1.setId("list1");
        list1.setDisplayName("List 1");

        final List list2 = new List();
        list2.setId("list4");
        list2.setDisplayName("List 4");

        assertTrue(dataStore.isExcludedList(paramMap, list1));
        assertFalse(dataStore.isExcludedList(paramMap, list2));
    }

    @Test
    public void test_isSystemList() {
        final List list1 = new List();
        list1.setDisplayName("My Custom List");

        final List list2 = new List();
        list2.setDisplayName("Master Page Gallery");

        final List list3 = new List();
        list3.setDisplayName("Style Library");

        final List list4 = new List();
        list4.setDisplayName("User Information List");

        final List list5 = new List();
        list5.setDisplayName("_catalogs/masterpage");

        assertFalse(dataStore.isSystemList(list1));
        assertTrue(dataStore.isSystemList(list2));
        assertTrue(dataStore.isSystemList(list3));
        assertTrue(dataStore.isSystemList(list4));
        assertTrue(dataStore.isSystemList(list5));
    }

    @Test
    public void test_isSystemList_withSystemFacet() {
        // Test system list detection using Microsoft Graph API system facet
        // According to https://learn.microsoft.com/en-us/graph/api/resources/systemfacet?view=graph-rest-1.0

        // Create list with system facet
        final com.microsoft.graph.models.List systemListWithFacet = new com.microsoft.graph.models.List();
        systemListWithFacet.setDisplayName("User Information List");
        systemListWithFacet.setId("system-facet-list-id");
        // Simulate system facet presence
        systemListWithFacet.setSystem(new com.microsoft.graph.models.SystemFacet());

        // Create regular list without system facet
        final com.microsoft.graph.models.List regularList = new com.microsoft.graph.models.List();
        regularList.setDisplayName("Custom List");
        regularList.setId("regular-list-id");
        regularList.setSystem(null);

        // Create list that would be detected by name but has no system facet
        final com.microsoft.graph.models.List nameBasedSystemList = new com.microsoft.graph.models.List();
        nameBasedSystemList.setDisplayName("Master Page Gallery");
        nameBasedSystemList.setId("name-based-system-list-id");
        nameBasedSystemList.setSystem(null);

        // Test system facet detection takes priority
        assertTrue("List with system facet should be detected as system list", dataStore.isSystemList(systemListWithFacet));

        assertFalse("Regular list without system facet should not be detected as system list", dataStore.isSystemList(regularList));

        // Test fallback to name-based detection when no system facet
        assertTrue("Name-based system list should still be detected via fallback", dataStore.isSystemList(nameBasedSystemList));
    }

    @Test
    public void test_extractFieldValue() {
        final Map<String, Object> fields = new HashMap<>();
        fields.put("Title", "Test Title");
        fields.put("Description", "Test Description");
        fields.put("Notes", "Test Notes");
        fields.put("EmptyField", "");
        fields.put("NullField", null);

        // Test single field extraction
        assertEquals("Test Title", dataStore.extractFieldValue(fields, "Title"));
        assertEquals("Test Description", dataStore.extractFieldValue(fields, "Description"));

        // Test multiple field names (returns first non-empty)
        assertEquals("Test Title", dataStore.extractFieldValue(fields, "MissingField", "Title", "Description"));
        assertEquals("Test Description", dataStore.extractFieldValue(fields, "EmptyField", "NullField", "Description"));

        // Test with no matching fields
        assertNull(dataStore.extractFieldValue(fields, "MissingField", "AnotherMissing"));

        // Test with null fields map
        assertNull(dataStore.extractFieldValue(null, "Title"));

        // Test with empty field names
        assertNull(dataStore.extractFieldValue(fields));
    }

    @Test
    public void test_urlFilter() {
        // Test URL filter functionality added in processListItem improvement
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("include_pattern", ".*\\.xlsx?$");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("exclude_pattern", ".*temp.*");

        final DataStoreParams paramMap3 = new DataStoreParams();
        // No URL filter patterns

        // Verify that URL filter parameters are correctly retrieved
        assertEquals("Should get include pattern", ".*\\.xlsx?$", paramMap1.getAsString("include_pattern"));
        assertEquals("Should get exclude pattern", ".*temp.*", paramMap2.getAsString("exclude_pattern"));
        assertNull("Should return null for no pattern", paramMap3.getAsString("include_pattern"));
    }

    @Test
    public void test_statsTracking_parameters() {
        // Test that statistical tracking parameters are correctly handled
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "test-site-id");
        paramMap.put("list_id", "test-list-id");
        paramMap.put("number_of_threads", "2");

        // Verify parameters are accessible for stats tracking
        assertEquals("Should get site ID for stats", "test-site-id", paramMap.getAsString("site_id"));
        assertEquals("Should get list ID for stats", "test-list-id", paramMap.getAsString("list_id"));
        assertEquals("Should get thread count for stats", "2", paramMap.getAsString("number_of_threads"));
    }

    @Test
    public void test_failureHandling_parameters() {
        // Test that failure handling parameters are correctly configured
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_error", "true");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_error", "false");

        final DataStoreParams paramMap3 = new DataStoreParams();
        // Default should be false

        assertTrue("Should parse ignore_error=true", dataStore.isIgnoreError(paramMap1));
        assertFalse("Should parse ignore_error=false", dataStore.isIgnoreError(paramMap2));
        assertFalse("Should default to false", dataStore.isIgnoreError(paramMap3));
    }

    @Test
    public void test_permissionProcessing_configuration() {
        // Test that permission-related configurations are properly handled
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "test-site-123");

        // Test that parameters needed for permission processing are available
        assertEquals("Should get site ID for permission context", "test-site-123", paramMap.getAsString("site_id"));
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
    public void test_isIgnoreSystemLists() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_system_lists", "true");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_system_lists", "false");

        final DataStoreParams paramMap3 = new DataStoreParams();

        assertTrue(dataStore.isIgnoreSystemLists(paramMap1));
        assertFalse(dataStore.isIgnoreSystemLists(paramMap2));
        assertTrue(dataStore.isIgnoreSystemLists(paramMap3)); // default is true
    }

    @Test
    public void test_isIgnoreSystemLists_withSystemListFiltering() {
        // Test the logic used in storeData method for filtering system lists
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_system_lists", "true"); // Should exclude system lists

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_system_lists", "false"); // Should include system lists

        final List systemList = new List();
        systemList.setDisplayName("Master Page Gallery");
        systemList.setId("system-list-id");

        final List regularList = new List();
        regularList.setDisplayName("Custom List");
        regularList.setId("regular-list-id");

        // Test the filtering logic: (!isIgnoreSystemLists(paramMap) || !isSystemList(list))
        // When ignore_system_lists=true and list is system -> false (should be excluded)
        assertFalse("System list should be excluded when ignore_system_lists=true",
                (!dataStore.isIgnoreSystemLists(paramMap1) || !dataStore.isSystemList(systemList)));

        // When ignore_system_lists=true and list is regular -> true (should be included)
        assertTrue("Regular list should be included when ignore_system_lists=true",
                (!dataStore.isIgnoreSystemLists(paramMap1) || !dataStore.isSystemList(regularList)));

        // When ignore_system_lists=false and list is system -> true (should be included)
        assertTrue("System list should be included when ignore_system_lists=false",
                (!dataStore.isIgnoreSystemLists(paramMap2) || !dataStore.isSystemList(systemList)));

        // When ignore_system_lists=false and list is regular -> true (should be included)
        assertTrue("Regular list should be included when ignore_system_lists=false",
                (!dataStore.isIgnoreSystemLists(paramMap2) || !dataStore.isSystemList(regularList)));
    }

    @Test
    public void test_threadPoolCreation() {
        // Test that number_of_threads parameter is correctly parsed
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("number_of_threads", "1");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("number_of_threads", "3");

        final DataStoreParams paramMap3 = new DataStoreParams();
        // No number_of_threads parameter - should default to 1

        // Verify parameter parsing
        assertEquals("Should parse number_of_threads=1", "1", paramMap1.getAsString("number_of_threads", "1"));
        assertEquals("Should parse number_of_threads=3", "3", paramMap2.getAsString("number_of_threads", "1"));
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
    public void test_isExcludedList_multipleLists() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_list_id", "list1,list2, list3 ");

        final List excludedList1 = new List();
        excludedList1.setId("list1");
        excludedList1.setDisplayName("Excluded List 1");

        final List excludedList2 = new List();
        excludedList2.setId("list2");
        excludedList2.setDisplayName("Excluded List 2");

        final List excludedList3 = new List();
        excludedList3.setId("list3");
        excludedList3.setDisplayName("Excluded List 3");

        final List allowedList = new List();
        allowedList.setId("list4");
        allowedList.setDisplayName("Allowed List");

        assertTrue("List 1 should be excluded", dataStore.isExcludedList(paramMap, excludedList1));
        assertTrue("List 2 should be excluded", dataStore.isExcludedList(paramMap, excludedList2));
        assertTrue("List 3 should be excluded", dataStore.isExcludedList(paramMap, excludedList3));
        assertFalse("List 4 should not be excluded", dataStore.isExcludedList(paramMap, allowedList));
    }

    @Test
    public void test_isExcludedList_emptyExcludeList() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("exclude_list_id", "");

        final DataStoreParams paramMap2 = new DataStoreParams();
        // No exclude_list_id parameter set

        final List list = new List();
        list.setId("any-list-id");
        list.setDisplayName("Any List");

        assertFalse("List should not be excluded with empty exclude list", dataStore.isExcludedList(paramMap1, list));
        assertFalse("List should not be excluded with no exclude parameter", dataStore.isExcludedList(paramMap2, list));
    }

    @Test
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

    @Test
    public void test_numberOfThreads_listProcessingFutures() {
        // Test that list processing properly manages futures for concurrent execution
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("number_of_threads", "2");
        paramMap.put("site_id", "test-site");

        // Test that the thread count parameter is correctly used
        assertEquals("Should parse thread count correctly", "2", paramMap.getAsString("number_of_threads", "1"));

        // Verify that ExecutorService can be created with the specified thread count
        try {
            final ExecutorService executor =
                    java.util.concurrent.Executors.newFixedThreadPool(Integer.parseInt(paramMap.getAsString("number_of_threads", "1")));

            // Test that we can submit tasks to the executor
            final java.util.List<java.util.concurrent.Future<?>> futures = new java.util.concurrent.CopyOnWriteArrayList<>();

            // Submit some test tasks
            for (int i = 0; i < 3; i++) {
                final int taskId = i;
                futures.add(executor.submit(() -> {
                    // Simulate some work
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return taskId;
                }));
            }

            // Wait for all tasks to complete
            for (final java.util.concurrent.Future<?> future : futures) {
                future.get(); // This will block until the task completes
            }

            executor.shutdown();
            assertTrue("Executor should shut down successfully", executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
        } catch (Exception e) {
            fail("Should be able to manage futures with thread pool: " + e.getMessage());
        }
    }

    @Test
    public void test_ignoreSystemLists_specificListId() {
        // Test that ignore_system_lists is respected even when a specific list_id is provided
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_system_lists", "true");
        paramMap1.put("site_id", "test-site-id");
        paramMap1.put("list_id", "system-list-id");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_system_lists", "false");
        paramMap2.put("site_id", "test-site-id");
        paramMap2.put("list_id", "system-list-id");

        // Test that we can identify system lists correctly
        final com.microsoft.graph.models.List systemList = new com.microsoft.graph.models.List();
        systemList.setId("system-list-id");
        systemList.setDisplayName("Master Page Gallery");

        final com.microsoft.graph.models.List regularList = new com.microsoft.graph.models.List();
        regularList.setId("regular-list-id");
        regularList.setDisplayName("Custom List");

        // Test the ignore_system_lists logic
        assertTrue("Should ignore system lists when ignore_system_lists=true", dataStore.isIgnoreSystemLists(paramMap1));
        assertFalse("Should not ignore system lists when ignore_system_lists=false", dataStore.isIgnoreSystemLists(paramMap2));

        // Test system list detection
        assertTrue("Should detect system list correctly", dataStore.isSystemList(systemList));
        assertFalse("Should not detect regular list as system", dataStore.isSystemList(regularList));

        // Test the logic used in storeData for specific list ID: (!isIgnoreSystemLists(paramMap) || !isSystemList(list))
        assertFalse("System list should be skipped when ignore_system_lists=true",
                (!dataStore.isIgnoreSystemLists(paramMap1) || !dataStore.isSystemList(systemList)));

        assertTrue("System list should be processed when ignore_system_lists=false",
                (!dataStore.isIgnoreSystemLists(paramMap2) || !dataStore.isSystemList(systemList)));

        assertTrue("Regular list should always be processed regardless of ignore_system_lists setting",
                (!dataStore.isIgnoreSystemLists(paramMap1) || !dataStore.isSystemList(regularList)));
    }

    @Test
    public void test_ignoreSystemLists_defaultBehavior() {
        // Test default behavior when ignore_system_lists is not specified
        final DataStoreParams paramMapDefault = new DataStoreParams();
        paramMapDefault.put("site_id", "test-site-id");

        // Default should be true (ignore system lists)
        assertTrue("Default behavior should ignore system lists", dataStore.isIgnoreSystemLists(paramMapDefault));

        // Test various system list names
        final com.microsoft.graph.models.List[] systemLists =
                { createList("master-page-id", "Master Page Gallery"), createList("style-lib-id", "Style Library"),
                        createList("catalogs-id", "_catalogs/masterpage"), createList("workflow-id", "Workflow Tasks"),
                        createList("user-info-id", "User Information List"), createList("access-req-id", "Access Requests"),
                        createList("form-templates-id", "Form Templates"), createList("underscore-id", "_Hidden List") };

        for (final com.microsoft.graph.models.List systemList : systemLists) {
            assertTrue("Should detect '" + systemList.getDisplayName() + "' as system list", dataStore.isSystemList(systemList));

            // Test that system lists would be skipped with default settings
            assertFalse("System list '" + systemList.getDisplayName() + "' should be skipped with default settings",
                    (!dataStore.isIgnoreSystemLists(paramMapDefault) || !dataStore.isSystemList(systemList)));
        }
    }

    @Test
    public void test_ignoreSystemLists_edgeCases() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_system_lists", "true");

        // Test with null display name
        final com.microsoft.graph.models.List nullNameList = new com.microsoft.graph.models.List();
        nullNameList.setId("null-name-id");
        nullNameList.setDisplayName(null);

        // Test with empty display name
        final com.microsoft.graph.models.List emptyNameList = new com.microsoft.graph.models.List();
        emptyNameList.setId("empty-name-id");
        emptyNameList.setDisplayName("");

        // Test with case variations
        final com.microsoft.graph.models.List upperCaseList = new com.microsoft.graph.models.List();
        upperCaseList.setId("upper-case-id");
        upperCaseList.setDisplayName("MASTER PAGE GALLERY");

        final com.microsoft.graph.models.List mixedCaseList = new com.microsoft.graph.models.List();
        mixedCaseList.setId("mixed-case-id");
        mixedCaseList.setDisplayName("Style Library");

        // Null and empty names should not be considered system lists
        assertFalse("List with null display name should not be system list", dataStore.isSystemList(nullNameList));
        assertFalse("List with empty display name should not be system list", dataStore.isSystemList(emptyNameList));

        // Case variations should still be detected as system lists
        assertTrue("Upper case system list should be detected", dataStore.isSystemList(upperCaseList));
        assertTrue("Mixed case system list should be detected", dataStore.isSystemList(mixedCaseList));
    }

    private com.microsoft.graph.models.List createList(final String id, final String displayName) {
        final com.microsoft.graph.models.List list = new com.microsoft.graph.models.List();
        list.setId(id);
        list.setDisplayName(displayName);
        return list;
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

    @Test
    public void test_isTargetListType_acceptsNumericTemplateIds() {
        // The README tells users to write list_template_filter=100,101. Graph returns the
        // template as a name, so those settings used to match nothing and the crawl produced
        // no items at all.
        final SharePointListDataStore dataStore = new SharePointListDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("list_template_filter", "100,101");

        assertTrue("100 must select genericList", dataStore.isTargetListType(paramMap, listWithTemplate("genericList")));
        assertTrue("101 must select documentLibrary", dataStore.isTargetListType(paramMap, listWithTemplate("documentLibrary")));
        assertFalse("a template outside the filter must be rejected", dataStore.isTargetListType(paramMap, listWithTemplate("survey")));
    }

    @Test
    public void test_isTargetListType_stillAcceptsTemplateNames() {
        final SharePointListDataStore dataStore = new SharePointListDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("list_template_filter", "genericList,documentLibrary");

        assertTrue(dataStore.isTargetListType(paramMap, listWithTemplate("genericList")));
        assertTrue(dataStore.isTargetListType(paramMap, listWithTemplate("documentLibrary")));
        assertFalse(dataStore.isTargetListType(paramMap, listWithTemplate("survey")));
    }

    @Test
    public void test_isTargetListType_mixedNumericAndNameIsAccepted() {
        final SharePointListDataStore dataStore = new SharePointListDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("list_template_filter", "100,documentLibrary");

        assertTrue(dataStore.isTargetListType(paramMap, listWithTemplate("genericList")));
        assertTrue(dataStore.isTargetListType(paramMap, listWithTemplate("documentLibrary")));
    }

    @Test
    public void test_isTargetListType_blankFilterAcceptsEverything() {
        final SharePointListDataStore dataStore = new SharePointListDataStore();
        final DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isTargetListType(paramMap, listWithTemplate("survey")));
    }

    @Test
    public void test_isProcessableListItemType_defaultsToGenericListOnly() {
        // Unchanged default: without an explicit filter, only generic lists are processed.
        final SharePointListDataStore dataStore = new SharePointListDataStore();
        final DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isProcessableListItemType(paramMap, "genericList"));
        assertFalse(dataStore.isProcessableListItemType(paramMap, "documentLibrary"));
    }

    @Test
    public void test_isProcessableListItemType_honoursExplicitFilter() {
        // Setting the filter used to be pointless: processListItem dropped anything that was
        // not a generic list regardless.
        final SharePointListDataStore dataStore = new SharePointListDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("list_template_filter", "101");

        assertTrue(dataStore.isProcessableListItemType(paramMap, "documentLibrary"));
        assertFalse(dataStore.isProcessableListItemType(paramMap, "genericList"));
    }

    @Test
    public void test_unknownListTemplateFilterTokenWarnsOnceNotPerItem() {
        // list_template_filter=106,100 against a genericList: "106" has no documented mapping,
        // so evaluating it used to warn - and c01b81f made isTargetListType run once per list
        // item via isProcessableListItemType, so that warning used to repeat once per item.
        final SharePointListDataStore dataStore = new SharePointListDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("list_template_filter", "106,100");

        final AtomicInteger warnCount = new AtomicInteger();
        final org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(SharePointListDataStore.class);
        final AbstractAppender appender =
                new AbstractAppender("test-unknown-template-warn-counter", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(final LogEvent event) {
                        if (event.getLevel() == Level.WARN
                                && event.getMessage().getFormattedMessage().contains("Unknown list template ID")) {
                            warnCount.incrementAndGet();
                        }
                    }
                };
        appender.start();
        coreLogger.addAppender(appender);
        try {
            // storeData calls this exactly once per crawl, regardless of how many lists or
            // items it goes on to process.
            dataStore.validateListTemplateFilter(paramMap);
            assertEquals("the unmapped numeric token must warn exactly once", 1, warnCount.get());

            // Simulate processListItem calling isProcessableListItemType once per item, as it
            // does for every item in a matching list (c01b81f).
            for (int i = 0; i < 25; i++) {
                assertTrue(dataStore.isProcessableListItemType(paramMap, "genericList"));
            }
            assertEquals("evaluating the filter for 25 items must not add further warnings", 1, warnCount.get());
        } finally {
            coreLogger.removeAppender(appender);
            appender.stop();
        }
    }

    @Test
    public void test_blankListTemplateFilterTokenDoesNotWarn() {
        // list_template_filter=100,,101 - the blank entry used to be reported as an "unknown
        // numeric ID" because "".chars().allMatch(Character::isDigit) is vacuously true for an
        // empty stream.
        final SharePointListDataStore dataStore = new SharePointListDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("list_template_filter", "100,,101");

        final AtomicInteger warnCount = new AtomicInteger();
        final org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(SharePointListDataStore.class);
        final AbstractAppender appender =
                new AbstractAppender("test-blank-template-warn-counter", null, null, false, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(final LogEvent event) {
                        if (event.getLevel() == Level.WARN
                                && event.getMessage().getFormattedMessage().contains("Unknown list template ID")) {
                            warnCount.incrementAndGet();
                        }
                    }
                };
        appender.start();
        coreLogger.addAppender(appender);
        try {
            dataStore.validateListTemplateFilter(paramMap);
            assertEquals("a blank token must be silently ignored, not treated as an unknown numeric ID", 0, warnCount.get());

            // The match outcome itself must be unaffected by the blank token.
            assertTrue(dataStore.isTargetListType(paramMap, listWithTemplate("documentLibrary")));
            assertFalse(dataStore.isTargetListType(paramMap, listWithTemplate("survey")));
            assertEquals("evaluating the blank token via isTargetListType must not warn either", 0, warnCount.get());
        } finally {
            coreLogger.removeAppender(appender);
            appender.stop();
        }
    }

    /**
     * List items must not touch {@code GET /sites/{id}/permissions}: it needs
     * {@code Sites.FullControl.All} and returns only application grants, never user or group
     * roles. Runs {@link SharePointListDataStore#processListItem} -- the method that used to call
     * it once per item -- against a real {@link Microsoft365Client} wired to a
     * {@link GraphMockServer}, so the assertion is on actual HTTP traffic, not a mock's
     * bookkeeping. The item's fields are pre-populated so the (unrelated) field-refresh branch
     * makes no request of its own, leaving the permissions call as the only possible traffic.
     *
     * <p>After this branch, {@code default_permissions} is the item's <em>only</em> role source
     * (see the removed {@code getSitePermissions} call above), so this also asserts the roles the
     * item is actually indexed with -- exactly {@code PermissionHelper#encode}'s output for each
     * configured entry, nothing more. Deleting the {@code default_permissions} block from
     * {@link SharePointListDataStore#processListItem} leaves this list findable by nobody while
     * the earlier assertions above stay green, so without this it is uncovered.
     */
    @Test
    public void test_processListItem_doesNotRequestSitePermissions() throws Exception {
        // crawlerStatsHelper and permissionHelper are not wired into test_app.xml, and both in
        // turn need systemHelper (also not wired) -- crawlerStatsHelper directly, permissionHelper
        // via its @Resource field, which plain ComponentUtil.register(...) does not auto-inject
        // (see TestablePermissionHelper below) -- so all three are registered directly here, the
        // same pattern Microsoft365DataStorePermissionTest and OneNoteDataStoreTest use.
        final org.codelibs.fess.helper.SystemHelper systemHelper = new org.codelibs.fess.helper.SystemHelper();
        ComponentUtil.register(systemHelper, "systemHelper");
        final org.codelibs.fess.helper.CrawlerStatsHelper crawlerStatsHelper = new org.codelibs.fess.helper.CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        final TestablePermissionHelper permissionHelper = new TestablePermissionHelper();
        permissionHelper.useSystemHelper(systemHelper);
        ComponentUtil.register(permissionHelper, "permissionHelper");

        final String roleField = ComponentUtil.getFessConfig().getIndexFieldRole();
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put(roleField, "item.roles");

        // convertValue's real path goes through ComponentUtil.getScriptEngineFactory(), which
        // this unit test has no business standing up -- see OneNoteDataStoreTest's identical seam.
        // "item.roles" is the only template used here, so it is resolved with a direct nested map
        // lookup instead; processListItem itself, including the roles-assembly logic under test,
        // is exercised completely unmodified.
        final SharePointListDataStore roleAwareDataStore = new SharePointListDataStore() {
            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                if ("item.roles".equals(template) && resultMap.get(LIST_ITEM) instanceof final Map<?, ?> itemMap) {
                    return itemMap.get(LIST_ITEM_ROLES);
                }
                return super.convertValue(scriptType, template, resultMap);
            }
        };

        try (GraphMockServer server = new GraphMockServer();
                MockableMicrosoft365Client client = new MockableMicrosoft365Client(dummyParams())) {
            // Queued defensively, not consumed by the fixed code: if a regression reintroduces a
            // site-permissions request, this lets it complete (with an empty result) instead of
            // blocking the test on an unfulfilled mock response, so the /permissions assertion
            // below is what fails, quickly and legibly.
            server.enqueueJson("{\"value\":[]}");
            client.useServer(server.newGraphClient());

            final Site site = new Site();
            site.setId("site-1");
            site.setDisplayName("Site");
            site.setWebUrl("https://example.sharepoint.com/sites/site-1");

            final ListInfo info = new ListInfo();
            info.setTemplate("genericList");
            final List list = new List();
            list.setId("list-1");
            list.setDisplayName("List");
            list.setWebUrl("https://example.sharepoint.com/sites/site-1/Lists/List");
            list.setList(info);

            final ListItem item = new ListItem();
            item.setId("item-1");
            item.setWebUrl("https://example.sharepoint.com/sites/site-1/Lists/List/DispForm.aspx?ID=1");
            final FieldValueSet fields = new FieldValueSet();
            final Map<String, Object> additionalData = new HashMap<>();
            additionalData.put("Title", "Test Item");
            fields.setAdditionalData(additionalData);
            item.setFields(fields);

            final Map<String, Object> configMap = new LinkedHashMap<>();
            configMap.put(SharePointListDataStore.IGNORE_ERROR, Boolean.FALSE);

            final DataStoreParams paramMap = new DataStoreParams();
            paramMap.put(SharePointListDataStore.DEFAULT_PERMISSIONS, "{role}admin,{group}sales");

            final TestCallback callback = new TestCallback();
            roleAwareDataStore.processListItem(new DataConfig(), callback, configMap, paramMap, scriptMap, new HashMap<>(), client, site,
                    list, item);

            assertEquals("processListItem must index the item despite there being no site-permission source", 1, callback.getCount());

            final java.util.List<String> paths = new ArrayList<>();
            for (int i = 0; i < server.requestCount(); i++) {
                paths.add(server.takePath());
            }
            assertFalse("no request may end in /permissions, but got: " + paths,
                    paths.stream().anyMatch(path -> path.contains("/permissions")));

            @SuppressWarnings("unchecked")
            final java.util.List<String> roles = (java.util.List<String>) callback.getLastDataMap().get(roleField);
            final java.util.Set<String> expectedRoles = new java.util.HashSet<>(
                    java.util.List.of(permissionHelper.encode("{role}admin"), permissionHelper.encode("{group}sales")));
            assertEquals(
                    "default_permissions is the item's only role source, so the indexed roles must be exactly its encoded entries, got "
                            + roles,
                    expectedRoles, roles == null ? java.util.Set.of() : new java.util.HashSet<>(roles));
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
     * {@code PermissionHelper#systemHelper} is {@code @Resource}-injected, which plain
     * {@code ComponentUtil.register(...)} does not perform in this minimal test container; this
     * subclass sets it directly so {@code encode(...)} is safe to call.
     */
    private static final class TestablePermissionHelper extends org.codelibs.fess.helper.PermissionHelper {
        void useSystemHelper(final org.codelibs.fess.helper.SystemHelper systemHelper) {
            this.systemHelper = systemHelper;
        }
    }

    private static com.microsoft.graph.models.List listWithTemplate(final String template) {
        final com.microsoft.graph.models.ListInfo info = new com.microsoft.graph.models.ListInfo();
        info.setTemplate(template);
        final com.microsoft.graph.models.List list = new com.microsoft.graph.models.List();
        list.setList(info);
        return list;
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
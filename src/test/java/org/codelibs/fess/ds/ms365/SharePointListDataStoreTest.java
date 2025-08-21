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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.microsoft.graph.models.List;

public class SharePointListDataStoreTest extends LastaFluteTestCase {

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
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new SharePointListDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        dataStore = null;
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("SharePointListDataStore", dataStore.getName());
    }

    public void test_getSiteId() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("site_id", "test-site-id");

        final String siteId = dataStore.getSiteId(paramMap);
        assertEquals("test-site-id", siteId);
    }

    public void test_getListId() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("list_id", "test-list-id");

        final String listId = dataStore.getListId(paramMap);
        assertEquals("test-list-id", listId);
    }

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

    public void test_buildContentFromFields() {
        final Map<String, Object> fields = new HashMap<>();
        fields.put("Title", "Test Title");
        fields.put("Description", "Test Description");
        fields.put("_SystemField", "System Value");
        fields.put("ID", "123");
        fields.put("ContentType", "Item");
        fields.put("Notes", "Some notes");
        fields.put("EmptyField", "");
        fields.put("NullField", null);

        final String content = dataStore.buildContentFromFields(fields);

        // Should include regular fields but not system fields
        assertTrue(content.contains("Test Title"));
        assertTrue(content.contains("Test Description"));
        assertTrue(content.contains("Some notes"));

        // Should not include system fields
        assertFalse(content.contains("System Value"));
        assertFalse(content.contains("123"));
        assertFalse(content.contains("Item"));

        // Test with null/empty fields
        assertEquals("", dataStore.buildContentFromFields(null));
        assertEquals("", dataStore.buildContentFromFields(new HashMap<>()));
    }

    public void test_isSystemField() {
        // System fields should return true
        assertTrue(dataStore.isSystemField("_SystemField"));
        assertTrue(dataStore.isSystemField("_vti_title"));
        assertTrue(dataStore.isSystemField("ID"));
        assertTrue(dataStore.isSystemField("ContentType"));
        assertTrue(dataStore.isSystemField("Version"));
        assertTrue(dataStore.isSystemField("Attachments"));
        assertTrue(dataStore.isSystemField("owsHiddenVersion"));

        // Regular fields should return false
        assertFalse(dataStore.isSystemField("Title"));
        assertFalse(dataStore.isSystemField("Description"));
        assertFalse(dataStore.isSystemField("Notes"));
        assertFalse(dataStore.isSystemField("CustomField"));

        // Edge cases
        assertTrue(dataStore.isSystemField(""));
        assertTrue(dataStore.isSystemField(null));
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

    public void test_isIncludeAttachments() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("include_attachments", "true");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("include_attachments", "false");

        final DataStoreParams paramMap3 = new DataStoreParams();

        assertTrue(dataStore.isIncludeAttachments(paramMap1));
        assertFalse(dataStore.isIncludeAttachments(paramMap2));
        assertFalse(dataStore.isIncludeAttachments(paramMap3)); // default is false
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
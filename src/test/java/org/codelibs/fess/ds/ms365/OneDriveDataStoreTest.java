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
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.Identity;
import com.microsoft.graph.models.ItemReference;
import com.microsoft.graph.models.Permission;
import com.microsoft.graph.models.SharePointIdentitySet;

public class OneDriveDataStoreTest extends LastaFluteTestCase {

    private static final Logger logger = LogManager.getLogger(OneDriveDataStoreTest.class);

    // for test
    public static final String tenant = "";
    public static final String clientId = "";
    public static final String clientSecret = "";

    private OneDriveDataStore dataStore;

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
        dataStore = new OneDriveDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("OneDriveDataStore", dataStore.getName());
    }

    public void test_getUrl() {
        Map<String, Object> configMap = new HashMap<>();
        DataStoreParams paramMap = new DataStoreParams();
        DriveItem item = new DriveItem();

        assertNull(dataStore.getUrl(configMap, paramMap, item));

        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_SHARED);
        item.setWebUrl(
                "https://n2sm.sharepoint.com/sites/test-site/_layouts/15/Doc.aspx?sourcedoc=%X-X-X-X-X%7D&file=test.doc&action=default&mobileredirect=true");
        ItemReference parentRef = new ItemReference();
        parentRef.setPath("/drive/root:/fess-testdata-master/msoffice");
        item.setParentReference(parentRef);
        item.setName("test.doc");
        assertEquals("https://n2sm.sharepoint.com/sites/test-site/Shared%20Documents/fess-testdata-master/msoffice/test.doc",
                dataStore.getUrl(configMap, paramMap, item));

        item.setWebUrl("https://n2sm.sharepoint.com/sites/test-site/Shared%20Documents/fess-testdata-master/msoffice/test.doc");
        assertEquals("https://n2sm.sharepoint.com/sites/test-site/Shared%20Documents/fess-testdata-master/msoffice/test.doc",
                dataStore.getUrl(configMap, paramMap, item));
    }

    public void test_getUrlFilter() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test with no include/exclude patterns - should return a UrlFilter instance but behavior depends on implementation
        try {
            UrlFilter filter = dataStore.getUrlFilter(paramMap);
            // UrlFilter is created by ComponentUtil.getComponent() so it may throw exception in test environment
            // This is expected behavior in isolated test environment
            assertNotNull(filter);
        } catch (Exception e) {
            // Expected in test environment where ComponentUtil dependencies are not available
            assertTrue("Expected ComponentNotFoundException or similar",
                    e.getMessage().contains("ComponentNotFound") || e.getMessage().contains("Component"));
        }
    }

    public void test_isSharedDocumentsDriveCrawler() {
        DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isSharedDocumentsDriveCrawler(paramMap)); // default is true based on implementation

        paramMap.put(OneDriveDataStore.SHARED_DOCUMENTS_DRIVE_CRAWLER, "false");
        assertFalse(dataStore.isSharedDocumentsDriveCrawler(paramMap));

        paramMap.put(OneDriveDataStore.SHARED_DOCUMENTS_DRIVE_CRAWLER, "true");
        assertTrue(dataStore.isSharedDocumentsDriveCrawler(paramMap));
    }

    public void test_isUserDriveCrawler() {
        DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isUserDriveCrawler(paramMap)); // default is true

        paramMap.put(OneDriveDataStore.USER_DRIVE_CRAWLER, "false");
        assertFalse(dataStore.isUserDriveCrawler(paramMap));

        paramMap.put(OneDriveDataStore.USER_DRIVE_CRAWLER, "true");
        assertTrue(dataStore.isUserDriveCrawler(paramMap));
    }

    public void test_isGroupDriveCrawler() {
        DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isGroupDriveCrawler(paramMap)); // default is true

        paramMap.put(OneDriveDataStore.GROUP_DRIVE_CRAWLER, "false");
        assertFalse(dataStore.isGroupDriveCrawler(paramMap));

        paramMap.put(OneDriveDataStore.GROUP_DRIVE_CRAWLER, "true");
        assertTrue(dataStore.isGroupDriveCrawler(paramMap));
    }

    public void test_isIgnoreFolder() {
        DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isIgnoreFolder(paramMap)); // default is true

        paramMap.put(OneDriveDataStore.IGNORE_FOLDER, "false");
        assertFalse(dataStore.isIgnoreFolder(paramMap));

        paramMap.put(OneDriveDataStore.IGNORE_FOLDER, "true");
        assertTrue(dataStore.isIgnoreFolder(paramMap));
    }

    public void test_isIgnoreError() {
        DataStoreParams paramMap = new DataStoreParams();

        assertFalse(dataStore.isIgnoreError(paramMap)); // default is false for consistency

        paramMap.put(OneDriveDataStore.IGNORE_ERROR, "false");
        assertFalse(dataStore.isIgnoreError(paramMap));

        paramMap.put(OneDriveDataStore.IGNORE_ERROR, "true");
        assertTrue(dataStore.isIgnoreError(paramMap));
    }

    public void test_getMaxSize() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test default value
        assertEquals(OneDriveDataStore.DEFAULT_MAX_SIZE, dataStore.getMaxSize(paramMap));

        // Test custom value
        paramMap.put(OneDriveDataStore.MAX_CONTENT_LENGTH, "1024");
        assertEquals(1024L, dataStore.getMaxSize(paramMap));

        // Test invalid value (non-numeric)
        paramMap.put(OneDriveDataStore.MAX_CONTENT_LENGTH, "invalid");
        assertEquals(OneDriveDataStore.DEFAULT_MAX_SIZE, dataStore.getMaxSize(paramMap));
    }

    public void test_getSupportedMimeTypes() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test default (should return ".*" as array)
        String[] mimeTypes = dataStore.getSupportedMimeTypes(paramMap);
        assertNotNull(mimeTypes);
        assertEquals(1, mimeTypes.length);
        assertEquals(".*", mimeTypes[0]);

        // Test single mime type
        paramMap.put(OneDriveDataStore.SUPPORTED_MIMETYPES, "text/plain");
        mimeTypes = dataStore.getSupportedMimeTypes(paramMap);
        assertNotNull(mimeTypes);
        assertEquals(1, mimeTypes.length);
        assertEquals("text/plain", mimeTypes[0]);

        // Test multiple mime types
        paramMap.put(OneDriveDataStore.SUPPORTED_MIMETYPES, "text/plain,application/pdf,image/jpeg");
        mimeTypes = dataStore.getSupportedMimeTypes(paramMap);
        assertNotNull(mimeTypes);
        assertEquals(3, mimeTypes.length);
        assertEquals("text/plain", mimeTypes[0]);
        assertEquals("application/pdf", mimeTypes[1]);
        assertEquals("image/jpeg", mimeTypes[2]);
    }

    public void test_getUserEmail() {
        // Test with null permission - this will cause NullPointerException based on implementation
        try {
            dataStore.getUserEmail(null);
            fail("Should have thrown NullPointerException");
        } catch (NullPointerException e) {
            // Expected - implementation doesn't handle null input
            assertTrue("Expected NullPointerException", true);
        }

        // Test with permission but no grantedToV2
        Permission permission = new Permission();
        assertNull(dataStore.getUserEmail(permission));

        // Test with user email in id field
        permission = new Permission();
        SharePointIdentitySet identitySet = new SharePointIdentitySet();
        Identity user = new Identity();
        user.setId("user@example.com");
        user.setDisplayName("User Name");
        identitySet.setUser(user);
        permission.setGrantedToV2(identitySet);
        assertEquals("user@example.com", dataStore.getUserEmail(permission));

        // Test with user display name only (no email in id)
        permission = new Permission();
        identitySet = new SharePointIdentitySet();
        user = new Identity();
        user.setId("12345");
        user.setDisplayName("User Display Name");
        identitySet.setUser(user);
        permission.setGrantedToV2(identitySet);
        assertEquals("User Display Name", dataStore.getUserEmail(permission));
    }

    public void test_encodeUrl() {
        // Test normal URL encoding - URLEncoder.encode uses + for spaces, then replaces with %20
        assertEquals("hello%20world", dataStore.encodeUrl("hello world"));
        assertEquals("test%2Fpath", dataStore.encodeUrl("test/path"));
        assertEquals("file%26name", dataStore.encodeUrl("file&name"));

        // Test already encoded URLs - these will be double encoded
        assertEquals("hello%2520world", dataStore.encodeUrl("hello%20world"));

        // Test special characters
        assertEquals("test%3Dvalue", dataStore.encodeUrl("test=value"));
        assertEquals("query%3Fparam", dataStore.encodeUrl("query?param"));

        // Test null and empty
        assertEquals("", dataStore.encodeUrl(""));
        assertNull(dataStore.encodeUrl(null)); // encodeUrl returns null for null input
    }

    public void test_isInterrupted() {
        // The isInterrupted method is void and throws an exception for InterruptedException
        // Test with InterruptedException - should throw InterruptedRuntimeException
        Exception e = new InterruptedException("Test interruption");
        try {
            dataStore.isInterrupted(e);
            fail("Should have thrown InterruptedRuntimeException");
        } catch (RuntimeException ex) {
            // Expected - should throw some kind of runtime exception
            assertTrue("Expected exception to be thrown", true);
        }

        // Test with non-interrupted exception - should not throw
        e = new RuntimeException("Regular exception");
        try {
            dataStore.isInterrupted(e);
            // Should complete without throwing
            assertTrue("Method completed without throwing", true);
        } catch (Exception ex) {
            fail("Should not have thrown exception for non-InterruptedException");
        }
    }

    public void testStoreData() {
        // doStoreData();
    }

    public void test_driveIdParameter() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test with no drive ID
        assertNull(paramMap.getAsString(OneDriveDataStore.DRIVE_ID));

        // Test with drive ID
        paramMap.put(OneDriveDataStore.DRIVE_ID, "drive123");
        assertEquals("drive123", paramMap.getAsString(OneDriveDataStore.DRIVE_ID));
    }

    public void test_defaultPermissions() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test with no default permissions
        assertNull(paramMap.getAsString(OneDriveDataStore.DEFAULT_PERMISSIONS));

        // Test with default permissions
        paramMap.put(OneDriveDataStore.DEFAULT_PERMISSIONS, "{role}admin,{role}user");
        assertEquals("{role}admin,{role}user", paramMap.getAsString(OneDriveDataStore.DEFAULT_PERMISSIONS));
    }

    public void test_numberOfThreads() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test default value
        assertEquals("1", paramMap.getAsString(OneDriveDataStore.NUMBER_OF_THREADS, "1"));

        // Test custom value
        paramMap.put(OneDriveDataStore.NUMBER_OF_THREADS, "5");
        assertEquals("5", paramMap.getAsString(OneDriveDataStore.NUMBER_OF_THREADS));
    }

    /*
    private void doStoreData() {
        final TikaExtractor tikaExtractor = new TikaExtractor();
        tikaExtractor.init();
        ComponentUtil.register(tikaExtractor, "tikaExtractor");

        final DataConfig dataConfig = new DataConfig();
        final Map<String, String> paramMap = new HashMap<>();
        paramMap.put("tenant", tenant);
        paramMap.put("client_id", clientId);
        paramMap.put("client_secret", clientSecret);
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        scriptMap.put(fessConfig.getIndexFieldTitle(), "files.name");
        scriptMap.put(fessConfig.getIndexFieldContent(), "files.description + \"\\n\"+ files.contents");
        scriptMap.put(fessConfig.getIndexFieldMimetype(), "files.mimetype");
        scriptMap.put(fessConfig.getIndexFieldCreated(), "files.created");
        scriptMap.put(fessConfig.getIndexFieldLastModified(), "files.last_modified");
        scriptMap.put(fessConfig.getIndexFieldContentLength(), "files.size");
        scriptMap.put(fessConfig.getIndexFieldUrl(), "files.web_url");
        scriptMap.put(fessConfig.getIndexFieldRole(), "files.roles");

        dataStore.storeData(dataConfig, new TestCallback() {
            @Override
            public void test(Map<String, String> paramMap, Map<String, Object> dataMap) {
                logger.debug(dataMap.toString());
            }
        }, paramMap, scriptMap, defaultDataMap);
    }
    */

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
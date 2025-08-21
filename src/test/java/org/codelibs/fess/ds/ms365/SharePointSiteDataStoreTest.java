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
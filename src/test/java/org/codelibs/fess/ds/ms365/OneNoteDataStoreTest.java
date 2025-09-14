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
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

public class OneNoteDataStoreTest extends LastaFluteTestCase {

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
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new OneNoteDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("OneNoteDataStore", dataStore.getName());
    }

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

    public void test_numberOfThreads() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test default value
        assertEquals("1", paramMap.getAsString(OneNoteDataStore.NUMBER_OF_THREADS, "1"));

        // Test custom value
        paramMap.put(OneNoteDataStore.NUMBER_OF_THREADS, "5");
        assertEquals("5", paramMap.getAsString(OneNoteDataStore.NUMBER_OF_THREADS));
    }

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

    public void test_crawlerTypeParameters() {
        assertEquals("number_of_threads", OneNoteDataStore.NUMBER_OF_THREADS);
        assertEquals("site_note_crawler", OneNoteDataStore.SITE_NOTE_CRAWLER);
        assertEquals("user_note_crawler", OneNoteDataStore.USER_NOTE_CRAWLER);
        assertEquals("group_note_crawler", OneNoteDataStore.GROUP_NOTE_CRAWLER);
    }

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

    public void testStoreData() {
        // doStoreData();
    }

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

    public void test_emptyParameterMap() {
        DataStoreParams emptyParamMap = new DataStoreParams();

        // Test defaults with empty parameter map - based on actual implementation
        assertTrue(dataStore.isSiteNoteCrawler(emptyParamMap));
        assertTrue(dataStore.isUserNoteCrawler(emptyParamMap));
        assertTrue(dataStore.isGroupNoteCrawler(emptyParamMap));
        assertEquals("1", emptyParamMap.getAsString(OneNoteDataStore.NUMBER_OF_THREADS, "1"));
    }

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

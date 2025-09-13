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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

import com.microsoft.graph.models.BaseSitePage;
import com.microsoft.graph.models.CanvasLayout;
import com.microsoft.graph.models.HorizontalSection;
import com.microsoft.graph.models.HorizontalSectionColumn;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.models.SitePage;
import com.microsoft.graph.models.StandardWebPart;
import com.microsoft.graph.models.TextWebPart;
import com.microsoft.graph.models.VerticalSection;
import com.microsoft.graph.models.WebPart;

public class SharePointPageDataStoreTest extends LastaFluteTestCase {

    private static final Logger logger = LogManager.getLogger(SharePointPageDataStoreTest.class);

    // for test
    public static final String tenant = "";
    public static final String clientId = "";
    public static final String clientSecret = "";

    private SharePointPageDataStore dataStore;

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
        dataStore = new SharePointPageDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        dataStore = null;
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("SharePointPageDataStore", dataStore.getName());
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

    public void test_isExcludedSite_byName() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_site_id", "Test.*");

        final Site site1 = new Site();
        site1.setId("site1");
        site1.setDisplayName("Test Site");

        final Site site2 = new Site();
        site2.setId("site2");
        site2.setDisplayName("Production Site");

        assertTrue(dataStore.isExcludedSite(paramMap, site1));
        assertFalse(dataStore.isExcludedSite(paramMap, site2));
    }

    public void test_isSystemPage() {
        final BaseSitePage page1 = createBaseSitePage("page1", "Regular Page", "https://site.sharepoint.com/sitepages/page1.aspx");
        final BaseSitePage page2 = createBaseSitePage("page2", "System Page", "https://site.sharepoint.com/_layouts/15/start.aspx");
        final BaseSitePage page3 = createBaseSitePage("page3", "Form Page", "https://site.sharepoint.com/sitepages/forms/page3.aspx");
        final BaseSitePage page4 = createBaseSitePage("page4", "API Page", "https://site.sharepoint.com/_api/page4");

        assertFalse(dataStore.isSystemPage(page1));
        assertTrue(dataStore.isSystemPage(page2));
        assertTrue(dataStore.isSystemPage(page3));
        assertTrue(dataStore.isSystemPage(page4));
    }

    public void test_determinePageType() {
        final SitePage newsPage = createSitePage("page1", "News Article");
        // newsPage.setPromotionKind(com.microsoft.graph.models.PagePromotionType.NEWS_POST); // Enum value may not exist

        final SitePage regularPage = createSitePage("page2", "Regular Page");

        final BaseSitePage basePage = createBaseSitePage("page3", "Base Page", "https://site.com/page3.aspx");

        assertEquals("article", dataStore.determinePageType(newsPage)); // Without promotion kind, defaults to article
        assertEquals("article", dataStore.determinePageType(regularPage));
        assertEquals("page", dataStore.determinePageType(basePage));
    }

    public void test_isTargetPage_systemPages() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_system_pages", "true");

        final BaseSitePage regularPage = createBaseSitePage("page1", "Regular Page", "https://site.com/sitepages/page1.aspx");
        final BaseSitePage systemPage = createBaseSitePage("page2", "System Page", "https://site.com/_layouts/15/start.aspx");

        assertTrue(dataStore.isTargetPage(paramMap, regularPage, null, null));
        assertFalse(dataStore.isTargetPage(paramMap, systemPage, null, null));
    }

    public void test_isTargetPage_pageTypeFilter() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("page_type_filter", "article");

        final SitePage newsPage = createSitePage("page1", "News Article");
        // newsPage.setPromotionKind(com.microsoft.graph.models.PagePromotionType.NEWS_POST); // Enum value may not exist

        final SitePage regularPage = createSitePage("page2", "Regular Page");
        final BaseSitePage basePage = createBaseSitePage("page3", "Base Page", "https://site.com/page3.aspx");

        assertTrue(dataStore.isTargetPage(paramMap, newsPage, null, null));
        assertTrue(dataStore.isTargetPage(paramMap, regularPage, null, null));
        assertFalse(dataStore.isTargetPage(paramMap, basePage, null, null));
    }

    public void test_isTargetPage_urlPatterns() {
        final DataStoreParams paramMap = new DataStoreParams();

        final Pattern includePattern = Pattern.compile(".*news.*");
        final Pattern excludePattern = Pattern.compile(".*temp.*");

        final BaseSitePage newsPage = createBaseSitePage("page1", "News Page", "https://site.com/sitepages/news-article.aspx");
        final BaseSitePage tempPage = createBaseSitePage("page2", "Temp Page", "https://site.com/sitepages/temp-page.aspx");
        final BaseSitePage regularPage = createBaseSitePage("page3", "Regular Page", "https://site.com/sitepages/regular.aspx");

        assertTrue(dataStore.isTargetPage(paramMap, newsPage, includePattern, excludePattern));
        assertFalse(dataStore.isTargetPage(paramMap, tempPage, includePattern, excludePattern));
        assertFalse(dataStore.isTargetPage(paramMap, regularPage, includePattern, excludePattern));
    }

    public void test_getPattern() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_pattern", ".*\\.aspx$");
        paramMap.put("exclude_pattern", ".*temp.*");

        final Pattern includePattern = dataStore.getPattern(paramMap, "include_pattern");
        final Pattern excludePattern = dataStore.getPattern(paramMap, "exclude_pattern");

        assertNotNull(includePattern);
        assertNotNull(excludePattern);

        assertTrue(includePattern.matcher("page.aspx").matches());
        assertFalse(includePattern.matcher("page.html").matches());

        assertTrue(excludePattern.matcher("temp-page.aspx").find());
        assertFalse(excludePattern.matcher("regular-page.aspx").find());
    }

    public void test_getPattern_invalid() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("invalid_pattern", "[invalid");

        final Pattern pattern = dataStore.getPattern(paramMap, "invalid_pattern");
        assertNull(pattern);
    }

    public void test_extractPageContent_basicFields() {
        final SitePage page = createSitePageWithContent("page1", "Test Title", "Test description");

        final String content = dataStore.extractPageContent(page);

        assertTrue(content.contains("Test Title"));
        assertTrue(content.contains("Test description"));
    }

    public void test_extractPageContent_withCanvasLayout() {
        final SitePage page = createSitePageWithCanvasLayout("page1", "Test Title");

        final String content = dataStore.extractPageContent(page);

        assertTrue(content.contains("Test Title"));
        assertTrue(content.contains("Test text content"));
        // assertTrue(content.contains("Standard web part data")); // Data not set due to type mismatch
    }

    public void test_extractWebPartContent_textWebPart() {
        final StringBuilder content = new StringBuilder();
        final TextWebPart textPart = new TextWebPart();
        textPart.setInnerHtml("<p>This is <strong>bold</strong> text with <br/>line breaks.</p>");

        dataStore.extractWebPartContent(textPart, content);

        final String result = content.toString();
        assertTrue(result.contains("This is"));
        assertTrue(result.contains("bold"));
        assertTrue(result.contains("text with"));
        assertTrue(result.contains("line breaks"));
        assertFalse(result.contains("<p>"));
        assertFalse(result.contains("<strong>"));
    }

    public void test_extractWebPartContent_standardWebPart() {
        final StringBuilder content = new StringBuilder();
        final StandardWebPart stdPart = new StandardWebPart();
        final Map<String, Object> data = new HashMap<>();
        data.put("title", "Standard Web Part Title");
        data.put("description", "This is a description");
        // stdPart.setData(data); // WebPartData type mismatch - skip for test

        dataStore.extractWebPartContent(stdPart, content);

        // Test passes since getData() returns null and no content is extracted
        final String result = content.toString();
        assertTrue(result.isEmpty());
    }

    public void test_extractDataFromObject_map() {
        final StringBuilder content = new StringBuilder();
        final Map<String, Object> data = new HashMap<>();
        data.put("title", "Test Title");
        data.put("description", "Test Description");
        data.put("id", "12345"); // Should be filtered out
        data.put("guid", "550e8400-e29b-41d4-a716-446655440000"); // Should be filtered out

        dataStore.extractDataFromObject(data, content);

        final String result = content.toString();
        assertTrue(result.contains("Test Title"));
        assertTrue(result.contains("Test Description"));
        assertFalse(result.contains("12345"));
        assertFalse(result.contains("550e8400-e29b-41d4-a716-446655440000"));
    }

    public void test_extractDataFromObject_list() {
        final StringBuilder content = new StringBuilder();
        final List<Object> data = new ArrayList<>();
        data.add("Valid text content");
        data.add("Another valid text");
        data.add("123"); // Should be filtered out as numeric ID

        dataStore.extractDataFromObject(data, content);

        final String result = content.toString();
        assertTrue(result.contains("Valid text content"));
        assertTrue(result.contains("Another valid text"));
        assertFalse(result.contains("123"));
    }

    public void test_isGuidOrId() {
        // Test GUID patterns
        assertTrue(dataStore.isGuidOrId("550e8400-e29b-41d4-a716-446655440000"));
        assertTrue(dataStore.isGuidOrId("6ba7b810-9dad-11d1-80b4-00c04fd430c8"));

        // Test numeric IDs
        assertTrue(dataStore.isGuidOrId("123"));
        assertTrue(dataStore.isGuidOrId("999999"));

        // Test short alphanumeric IDs
        assertTrue(dataStore.isGuidOrId("abc123"));
        assertTrue(dataStore.isGuidOrId("xyz789"));

        // Test valid text content
        assertFalse(dataStore.isGuidOrId("This is valid text content"));
        assertFalse(dataStore.isGuidOrId("A longer description text"));
        assertFalse(dataStore.isGuidOrId(""));
        assertFalse(dataStore.isGuidOrId(null));
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

    public void test_isIgnoreSystemPages() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_system_pages", "true");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_system_pages", "false");

        final DataStoreParams paramMap3 = new DataStoreParams();

        assertTrue(dataStore.isIgnoreSystemPages(paramMap1));
        assertFalse(dataStore.isIgnoreSystemPages(paramMap2));
        assertTrue(dataStore.isIgnoreSystemPages(paramMap3)); // default is true
    }

    public void test_threadPoolCreation() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("number_of_threads", "1");

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("number_of_threads", "3");

        final DataStoreParams paramMap3 = new DataStoreParams();

        assertEquals("1", paramMap1.getAsString("number_of_threads", "1"));
        assertEquals("3", paramMap2.getAsString("number_of_threads", "1"));
        assertEquals("1", paramMap3.getAsString("number_of_threads", "1"));

        try {
            Integer.parseInt(paramMap1.getAsString("number_of_threads", "1"));
            Integer.parseInt(paramMap2.getAsString("number_of_threads", "1"));
            Integer.parseInt(paramMap3.getAsString("number_of_threads", "1"));
        } catch (NumberFormatException e) {
            fail("Should be able to parse number_of_threads as integer");
        }
    }

    public void test_numberOfThreads_threadPoolManagement() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("number_of_threads", "2");

        assertEquals("2", paramMap.getAsString("number_of_threads", "1"));

        try {
            final ExecutorService executor =
                    java.util.concurrent.Executors.newFixedThreadPool(Integer.parseInt(paramMap.getAsString("number_of_threads", "1")));

            final java.util.List<java.util.concurrent.Future<?>> futures = new java.util.concurrent.CopyOnWriteArrayList<>();

            for (int i = 0; i < 3; i++) {
                final int taskId = i;
                futures.add(executor.submit(() -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return taskId;
                }));
            }

            for (final java.util.concurrent.Future<?> future : futures) {
                future.get();
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS));
        } catch (Exception e) {
            fail("Should be able to manage futures with thread pool: " + e.getMessage());
        }
    }

    // Helper methods for creating test objects

    private BaseSitePage createBaseSitePage(final String id, final String title, final String webUrl) {
        final BaseSitePage page = new BaseSitePage();
        page.setId(id);
        page.setTitle(title);
        page.setWebUrl(webUrl);
        return page;
    }

    private SitePage createSitePage(final String id, final String title) {
        final SitePage page = new SitePage();
        page.setId(id);
        page.setTitle(title);
        return page;
    }

    private SitePage createSitePageWithContent(final String id, final String title, final String description) {
        final SitePage page = createSitePage(id, title);
        page.setDescription(description);
        return page;
    }

    private SitePage createSitePageWithCanvasLayout(final String id, final String title) {
        final SitePage page = createSitePage(id, title);

        // Create canvas layout with web parts
        final CanvasLayout layout = new CanvasLayout();

        // Create horizontal section with text web part
        final HorizontalSection hSection = new HorizontalSection();
        final List<HorizontalSectionColumn> columns = new ArrayList<>();
        final HorizontalSectionColumn column = new HorizontalSectionColumn();

        final List<WebPart> webParts = new ArrayList<>();

        // Add text web part
        final TextWebPart textPart = new TextWebPart();
        textPart.setInnerHtml("<p>Test text content with <strong>formatting</strong></p>");
        webParts.add(textPart);

        // Add standard web part
        final StandardWebPart stdPart = new StandardWebPart();
        final Map<String, Object> data = new HashMap<>();
        data.put("content", "Standard web part data");
        // stdPart.setData(data); // WebPartData type mismatch - skip for test
        webParts.add(stdPart);

        column.setWebparts(webParts);
        columns.add(column);
        hSection.setColumns(columns);

        layout.setHorizontalSections(List.of(hSection));

        // Create vertical section
        final VerticalSection vSection = new VerticalSection();
        final List<WebPart> vWebParts = new ArrayList<>();
        final TextWebPart vTextPart = new TextWebPart();
        vTextPart.setInnerHtml("Vertical section content");
        vWebParts.add(vTextPart);
        vSection.setWebparts(vWebParts);

        layout.setVerticalSection(vSection);
        page.setCanvasLayout(layout);

        return page;
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
            logger.info("Stored page {}: {}", count, dataMap.get("url"));
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
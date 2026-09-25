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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.GraphMockServer;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.BaseSitePage;
import com.microsoft.graph.models.CanvasLayout;
import com.microsoft.graph.models.HorizontalSection;
import com.microsoft.graph.models.HorizontalSectionColumn;
import com.microsoft.graph.models.MetaDataKeyStringPair;
import com.microsoft.graph.models.PageLayoutType;
import com.microsoft.graph.models.PagePromotionType;
import com.microsoft.graph.models.ServerProcessedContent;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.models.SitePage;
import com.microsoft.graph.models.StandardWebPart;
import com.microsoft.graph.models.TextWebPart;
import com.microsoft.graph.models.VerticalSection;
import com.microsoft.graph.models.WebPart;
import com.microsoft.graph.models.WebPartData;
import com.microsoft.graph.serviceclient.GraphServiceClient;

public class SharePointPageDataStoreTest extends UnitDsTestCase {

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
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new SharePointPageDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        dataStore = null;
        super.tearDown(testInfo);
    }

    @Test
    public void test_getName() {
        assertEquals("SharePointPageDataStore", dataStore.getName());
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

    /**
     * A Graph site ID is {@code hostname,siteCollectionId,webId}, so a comma cannot also separate
     * the entries of {@code exclude_site_id}: several full site IDs are separated by semicolons.
     * Splitting on every comma turned each ID into fragments, and the hostname fragment is
     * contained in the {@code webUrl} of every site on that host, so an unlisted site on the same
     * host was excluded as well.
     */
    @Test
    public void test_isExcludedSite_siteIdsSeparatedBySemicolon() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_site_id", "contoso.sharepoint.com,11111111-1111-4111-8111-111111111111,99999999-9999-4999-8999-999999999999;"
                + "contoso.sharepoint.com,22222222-2222-4222-8222-222222222222,99999999-9999-4999-8999-999999999999");

        final Site listed1 = new Site();
        listed1.setId("contoso.sharepoint.com,11111111-1111-4111-8111-111111111111,99999999-9999-4999-8999-999999999999");
        listed1.setDisplayName("site1");
        listed1.setWebUrl("https://contoso.sharepoint.com/sites/site1");

        final Site listed2 = new Site();
        listed2.setId("contoso.sharepoint.com,22222222-2222-4222-8222-222222222222,99999999-9999-4999-8999-999999999999");
        listed2.setDisplayName("site2");
        listed2.setWebUrl("https://contoso.sharepoint.com/sites/site2");

        final Site unlisted = new Site();
        unlisted.setId("contoso.sharepoint.com,33333333-3333-4333-8333-333333333333,99999999-9999-4999-8999-999999999999");
        unlisted.setDisplayName("site3");
        unlisted.setWebUrl("https://contoso.sharepoint.com/sites/site3");

        assertTrue("the first listed site must be excluded", dataStore.isExcludedSite(paramMap, listed1));
        assertTrue("the second listed site must be excluded", dataStore.isExcludedSite(paramMap, listed2));
        assertFalse("an unlisted site on the same host must not be excluded", dataStore.isExcludedSite(paramMap, unlisted));
    }

    /**
     * Runs {@code storeData} over the real site enumeration: a {@link Microsoft365Client} wired to a
     * {@link GraphMockServer} lists two sites on one host, and {@code exclude_site_id} names the
     * first by its full Graph site ID. Only the second may be crawled. The {@code webUrl} each site
     * is matched against comes out of Graph's JSON, which is what turned the hostname fragment of a
     * comma-split ID into a match for every site, so no site was crawled and the job still finished
     * without an error.
     */
    @Test
    public void test_storeData_excludeSiteIdSkipsOnlyTheListedSite() throws Exception {
        final String excludedSiteId = "contoso.sharepoint.com,11111111-1111-4111-8111-111111111111,99999999-9999-4999-8999-999999999999";
        final String crawledSiteId = "contoso.sharepoint.com,22222222-2222-4222-8222-222222222222,99999999-9999-4999-8999-999999999999";

        try (GraphMockServer server = new GraphMockServer()) {
            // GET /sites, then GET /sites/{id}/sites for each listed site in turn (no sub-sites).
            server.enqueueJson("{\"value\":[" + siteJson(excludedSiteId, "site1") + "," + siteJson(crawledSiteId, "site2") + "]}");
            server.enqueueJson("{\"value\":[]}");
            server.enqueueJson("{\"value\":[]}");

            final List<String> crawledSiteIds = new ArrayList<>();
            final SharePointPageDataStore testDataStore = new SharePointPageDataStore() {
                @Override
                protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                    // getPageWithContent is never reached: storePagesInSite below records the site
                    // instead of listing its pages.
                    final PageContentStubbedMicrosoft365Client client = new PageContentStubbedMicrosoft365Client(dummyParams(), null);
                    client.useServer(server.newGraphClient());
                    return client;
                }

                @Override
                protected void storePagesInSite(final DataConfig dataConfig, final IndexUpdateCallback callback,
                        final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
                        final Map<String, Object> defaultDataMap, final ExecutorService executorService, final Microsoft365Client client,
                        final Site site) {
                    crawledSiteIds.add(site.getId());
                }
            };

            final DataStoreParams paramMap = new DataStoreParams();
            paramMap.put("exclude_site_id", excludedSiteId);

            testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>());

            assertEquals("exactly the site not named in exclude_site_id must be crawled", List.of(crawledSiteId), crawledSiteIds);
        }
    }

    /** One entry of a {@code GET /sites} response, with the properties Graph returns for a site. */
    private static String siteJson(final String id, final String name) {
        return "{\"createdDateTime\":\"2026-01-01T00:00:00Z\",\"description\":\"" + name + "\",\"id\":\"" + id
                + "\",\"lastModifiedDateTime\":\"2026-01-02T00:00:00Z\",\"name\":\"" + name
                + "\",\"webUrl\":\"https://contoso.sharepoint.com/sites/" + name + "\",\"displayName\":\"" + name
                + "\",\"root\":{},\"siteCollection\":{\"hostname\":\"contoso.sharepoint.com\"}}";
    }

    @Test
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

    /**
     * Graph describes a page with two independent properties: {@code promotionKind} ({@code page} or
     * {@code newsPost}) and {@code pageLayout} ({@code article} or {@code home}). The type is
     * {@code news} for a news post, {@code article} for any other page with the article layout, and
     * {@code page} for the rest - a home page, or a page Graph reports no layout for.
     */
    @Test
    public void test_determinePageType() {
        assertEquals("news",
                dataStore.determinePageType(createSitePage("page1", "News", PagePromotionType.NewsPost, PageLayoutType.Article)));
        assertEquals("article",
                dataStore.determinePageType(createSitePage("page2", "Article", PagePromotionType.Page, PageLayoutType.Article)));
        assertEquals("page", dataStore.determinePageType(createSitePage("page3", "Home", PagePromotionType.Page, PageLayoutType.Home)));
        assertEquals("page", dataStore.determinePageType(createSitePage("page4", "No layout", PagePromotionType.Page, null)));
        assertEquals("page", dataStore.determinePageType(createBaseSitePage("page5", "Base Page", "https://site.com/page5.aspx")));
    }

    @Test
    public void test_isTargetPage_systemPages() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_system_pages", "true");

        final BaseSitePage regularPage = createBaseSitePage("page1", "Regular Page", "https://site.com/sitepages/page1.aspx");
        final BaseSitePage systemPage = createBaseSitePage("page2", "System Page", "https://site.com/_layouts/15/start.aspx");

        assertTrue(dataStore.isTargetPage(paramMap, regularPage, null, null));
        assertFalse(dataStore.isTargetPage(paramMap, systemPage, null, null));
    }

    /**
     * Runs {@code storePagesInSite} over a Graph page listing that holds one page of each kind: a news
     * post, an article page and a home page. Every page Graph lists is a {@code sitePage}, so a type
     * derived from {@code promotionKind} alone was never {@code page}: {@code page_type_filter=page}
     * crawled nothing, and {@code article} also took the home page. The {@code pageLayout} each page
     * is classified by comes out of Graph's JSON.
     */
    @Test
    public void test_storePagesInSite_pageTypeFilter() throws Exception {
        final String pages = "{\"value\":[" + sitePageJson("1", "News", "article", "newsPost") + ","
                + sitePageJson("2", "Article", "article", "page") + "," + sitePageJson("3", "Home", "home", "page") + "]}";

        assertEquals("news must select only the news post", List.of("News"), crawledPageTitles(pages, "news"));
        assertEquals("article must select only the article page", List.of("Article"), crawledPageTitles(pages, "article"));
        assertEquals("page must select only the home page", List.of("Home"), crawledPageTitles(pages, "page"));
        assertEquals("a comma-separated filter must select each listed type", List.of("News", "Home"),
                crawledPageTitles(pages, "news, page"));
    }

    /** Titles of the pages {@code storePagesInSite} hands to {@code processPage} for the given filter. */
    private List<String> crawledPageTitles(final String pagesJson, final String pageTypeFilter) throws Exception {
        try (GraphMockServer server = new GraphMockServer();
                PageContentStubbedMicrosoft365Client client = new PageContentStubbedMicrosoft365Client(dummyParams(), null)) {
            server.enqueueJson(pagesJson);
            client.useServer(server.newGraphClient());

            final List<String> titles = new ArrayList<>();
            final SharePointPageDataStore testDataStore = new SharePointPageDataStore() {
                @Override
                protected void processPage(final DataConfig dataConfig, final IndexUpdateCallback callback,
                        final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
                        final Map<String, Object> defaultDataMap, final Microsoft365Client client, final Site site,
                        final BaseSitePage page) {
                    titles.add(page.getTitle());
                }
            };

            final DataStoreParams paramMap = new DataStoreParams();
            paramMap.put("page_type_filter", pageTypeFilter);
            final Site site = new Site();
            site.setId("contoso.sharepoint.com,11111111-1111-4111-8111-111111111111,99999999-9999-4999-8999-999999999999");
            site.setDisplayName("site1");

            final ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                testDataStore.storePagesInSite(new DataConfig(), null, new HashMap<>(), paramMap, new HashMap<>(), new HashMap<>(),
                        executor, client, site);
            } finally {
                executor.shutdown();
                assertTrue("page processing must finish", executor.awaitTermination(10, TimeUnit.SECONDS));
            }
            return titles;
        }
    }

    /** One entry of a {@code GET /sites/{id}/pages/microsoft.graph.sitePage} response, shaped like Graph's documented example. */
    private static String sitePageJson(final String id, final String title, final String pageLayout, final String promotionKind) {
        return "{\"@odata.type\":\"#microsoft.graph.sitePage\",\"@odata.etag\":\"\\\"{" + id + "},1\\\"\",\"eTag\":\"\\\"{" + id
                + "},1\\\"\",\"id\":\"" + id + "\",\"lastModifiedDateTime\":\"2026-01-02T00:00:00Z\",\"name\":\"" + title
                + ".aspx\",\"webUrl\":\"https://contoso.sharepoint.com/sites/site1/SitePages/" + title + ".aspx\",\"title\":\"" + title
                + "\",\"pageLayout\":\"" + pageLayout + "\",\"promotionKind\":\"" + promotionKind
                + "\",\"showComments\":false,\"showRecommendedPages\":false,\"contentType\":{\"id\":\"0x0101009D1CB255DA76424F860D91F20E6C4118\",\"name\":\"Site Page\"}"
                + ",\"parentReference\":{\"siteId\":\"11111111-1111-4111-8111-111111111111\"},\"publishingState\":{\"level\":\"published\",\"versionId\":\"1.0\"}}";
    }

    @Test
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

    @Test
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

    @Test
    public void test_getPattern_invalid() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("invalid_pattern", "[invalid");

        // A malformed pattern is a configuration error, not a document-level one: returning null
        // would read as "no filtering configured" and silently widen the crawl.
        final DataStoreException e = assertThrows(DataStoreException.class, () -> dataStore.getPattern(paramMap, "invalid_pattern"));
        assertTrue("the message must name the parameter, got: " + e.getMessage(), e.getMessage().contains("invalid_pattern"));
    }

    @Test
    public void test_extractPageContent_basicFields() {
        final SitePage page = createSitePageWithContent("page1", "Test Title", "Test description");

        final String content = dataStore.extractPageContent(page);

        assertTrue(content.contains("Test Title"));
        assertTrue(content.contains("Test description"));
    }

    @Test
    public void test_extractPageContent_withCanvasLayout() {
        final SitePage page = createSitePageWithCanvasLayout("page1", "Test Title");

        final String content = dataStore.extractPageContent(page);

        assertTrue(content.contains("Test Title"));
        assertTrue(content.contains("Test text content"));
        assertTrue("StandardWebPart text must reach page content, got: " + content, content.contains("Standard web part data"));
    }

    @Test
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

    @Test
    public void test_extractWebPartContent_standardWebPart_nullData() {
        final StringBuilder content = new StringBuilder();
        final StandardWebPart stdPart = new StandardWebPart();

        dataStore.extractWebPartContent(stdPart, content);

        assertTrue(content.toString().isEmpty());
    }

    @Test
    public void test_extractWebPartContent_standardWebPart_typedFields() {
        final StringBuilder content = new StringBuilder();
        final StandardWebPart stdPart = new StandardWebPart();

        final WebPartData data = new WebPartData();
        data.setTitle("Quick links");
        data.setDescription("Organize the main links to documents and pages.");

        final ServerProcessedContent processedContent = new ServerProcessedContent();

        final MetaDataKeyStringPair plainText = new MetaDataKeyStringPair();
        plainText.setKey("title");
        plainText.setValue("Searchable plain text body");
        processedContent.setSearchablePlainTexts(List.of(plainText));

        final MetaDataKeyStringPair htmlString = new MetaDataKeyStringPair();
        htmlString.setKey("content");
        htmlString.setValue("<p>Html string <strong>body</strong></p>");
        processedContent.setHtmlStrings(List.of(htmlString));

        final MetaDataKeyStringPair link = new MetaDataKeyStringPair();
        link.setKey("link");
        link.setValue("https://contoso.sharepoint.com/sites/marketing");
        processedContent.setLinks(List.of(link));

        data.setServerProcessedContent(processedContent);
        stdPart.setData(data);

        dataStore.extractWebPartContent(stdPart, content);

        final String result = content.toString();
        assertFalse("the web part type's toolbox title must not be indexed, got: " + result, result.contains("Quick links"));
        assertFalse("the web part type's toolbox description must not be indexed, got: " + result,
                result.contains("Organize the main links"));
        assertTrue("expected searchablePlainTexts, got: " + result, result.contains("Searchable plain text body"));
        assertTrue("expected htmlStrings text, got: " + result, result.contains("Html string"));
        assertTrue("expected htmlStrings text, got: " + result, result.contains("body"));
        assertFalse("serverProcessedContent.links must not be indexed, got: " + result,
                result.contains("https://contoso.sharepoint.com/sites/marketing"));
        assertFalse("html markup must be stripped, got: " + result, result.contains("<strong>"));
        assertFalse("html markup must be stripped, got: " + result, result.contains("<p>"));
    }

    @Test
    public void test_extractWebPartContent_standardWebPart_additionalData() {
        final StringBuilder content = new StringBuilder();
        final StandardWebPart stdPart = new StandardWebPart();

        final WebPartData data = new WebPartData();
        data.getAdditionalData().put("caption", "Additional data caption text");
        data.getAdditionalData().put("instanceId", "550e8400-e29b-41d4-a716-446655440000");
        stdPart.setData(data);

        dataStore.extractWebPartContent(stdPart, content);

        final String result = content.toString();
        assertTrue("expected the additionalData caption, got: " + result, result.contains("Additional data caption text"));
        assertFalse("a GUID must still be filtered out by isGuidOrId, got: " + result,
                result.contains("550e8400-e29b-41d4-a716-446655440000"));
    }

    @Test
    public void test_extractWebPartContent_standardWebPart_shortSearchableTextIsKept() {
        // searchablePlainTexts values are explicitly named, typed fields, so the >5-character and
        // isGuidOrId heuristics that extractDataFromObject applies to the untyped bag must NOT
        // apply here: a three-letter web-part heading is real content.
        final StringBuilder content = new StringBuilder();
        final StandardWebPart stdPart = new StandardWebPart();
        final WebPartData data = new WebPartData();
        final ServerProcessedContent processedContent = new ServerProcessedContent();
        final MetaDataKeyStringPair plainText = new MetaDataKeyStringPair();
        plainText.setKey("title");
        plainText.setValue("FAQ");
        processedContent.setSearchablePlainTexts(List.of(plainText));
        data.setServerProcessedContent(processedContent);
        stdPart.setData(data);

        dataStore.extractWebPartContent(stdPart, content);

        assertTrue("a short searchable text must survive, got: " + content, content.toString().contains("FAQ"));
    }

    @Test
    public void test_stripWebPartMarkup_stripsTagsBeforeDecodingEntities() {
        // Decoding "&lt;"/"&gt;" before stripping tags turns them into a literal "<"/">", which
        // the tag-stripping regex then treats as a new tag and eats everything in between,
        // silently deleting real text. Tags must be stripped first.
        assertEquals("5 < 10 and a > b", dataStore.stripWebPartMarkup("<p>5 &lt; 10 and a &gt; b</p>"));
    }

    @Test
    public void test_stripWebPartMarkup_decodesApostropheAndQuote() {
        assertEquals("it's a \"test\"", dataStore.stripWebPartMarkup("it&#39;s a &quot;test&quot;"));
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    private SitePage createSitePage(final String id, final String title, final PagePromotionType promotionKind,
            final PageLayoutType pageLayout) {
        final SitePage page = createSitePage(id, title);
        page.setPromotionKind(promotionKind);
        page.setPageLayout(pageLayout);
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
        final WebPartData data = new WebPartData();
        final ServerProcessedContent processedContent = new ServerProcessedContent();
        final MetaDataKeyStringPair plainText = new MetaDataKeyStringPair();
        plainText.setKey("title");
        plainText.setValue("Standard web part data");
        processedContent.setSearchablePlainTexts(List.of(plainText));
        data.setServerProcessedContent(processedContent);
        stdPart.setData(data);
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

    /**
     * Pages must not touch {@code GET /sites/{id}/permissions}: it needs
     * {@code Sites.FullControl.All} and returns only application grants, never user or group
     * roles. Runs {@link SharePointPageDataStore#processPage} -- which now computes a page's
     * roles inline, the former {@code getPagePermissions} helper is gone -- against a real
     * {@link Microsoft365Client} wired to a {@link GraphMockServer}, so the assertion is on
     * actual HTTP traffic, not a mock's bookkeeping. {@code getPageWithContent} is overridden to
     * return a fixture directly: that call is mandatory and unrelated to permissions, and
     * fabricating the exact Graph JSON shape it expects would add risk without adding coverage.
     *
     * <p>After this branch, {@code default_permissions} is the page's <em>only</em> role source,
     * so this also asserts the roles the page is actually indexed with -- exactly
     * {@code PermissionHelper#encode}'s output for each configured entry, nothing more. Deleting
     * the {@code default_permissions} block from {@link SharePointPageDataStore#processPage}
     * leaves this page findable by nobody while the earlier assertions above stay green, so
     * without this it is uncovered.
     */
    @Test
    public void test_processPage_doesNotRequestSitePermissions() throws Exception {
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
        scriptMap.put(roleField, "page.roles");

        // convertValue's real path goes through ComponentUtil.getScriptEngineFactory(), which
        // this unit test has no business standing up -- see OneNoteDataStoreTest's identical seam.
        // "page.roles" is the only template used here, so it is resolved with a direct nested map
        // lookup instead; processPage itself, including the roles-assembly logic under test, is
        // exercised completely unmodified.
        final SharePointPageDataStore roleAwareDataStore = new SharePointPageDataStore() {
            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                if ("page.roles".equals(template) && resultMap.get(PAGE) instanceof final Map<?, ?> pageDataMap) {
                    return pageDataMap.get(PAGE_ROLES);
                }
                return super.convertValue(scriptType, template, resultMap);
            }
        };

        final SitePage fullPage = new SitePage();
        fullPage.setId("page-1");
        fullPage.setTitle("Test Page");
        fullPage.setWebUrl("https://example.sharepoint.com/sites/site-1/SitePages/test.aspx");

        try (GraphMockServer server = new GraphMockServer();
                PageContentStubbedMicrosoft365Client client = new PageContentStubbedMicrosoft365Client(dummyParams(), fullPage)) {
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

            final SitePage page = new SitePage();
            page.setId("page-1");
            page.setWebUrl("https://example.sharepoint.com/sites/site-1/SitePages/test.aspx");
            page.setTitle("Test Page");

            final DataStoreParams paramMap = new DataStoreParams();
            paramMap.put(SharePointPageDataStore.DEFAULT_PERMISSIONS, "{role}admin,{group}sales");

            final TestCallback callback = new TestCallback();
            roleAwareDataStore.processPage(new DataConfig(), callback, new LinkedHashMap<>(), paramMap, scriptMap, new HashMap<>(), client,
                    site, page);

            assertEquals("processPage must index the page despite there being no site-permission source", 1, callback.getCount());

            final List<String> paths = new ArrayList<>();
            for (int i = 0; i < server.requestCount(); i++) {
                paths.add(server.takePath());
            }
            assertFalse("no request may end in /permissions, but got: " + paths,
                    paths.stream().anyMatch(path -> path.contains("/permissions")));

            @SuppressWarnings("unchecked")
            final List<String> roles = (List<String>) callback.getLastDataMap().get(roleField);
            final java.util.Set<String> expectedRoles =
                    new java.util.HashSet<>(List.of(permissionHelper.encode("{role}admin"), permissionHelper.encode("{group}sales")));
            assertEquals(
                    "default_permissions is the page's only role source, so the indexed roles must be exactly its encoded entries, got "
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
     * own {@code client} package; this subclass exposes it to tests in this package too, so a
     * real {@code Microsoft365Client} can be pointed at a {@link GraphMockServer} instead of
     * stubbing individual methods with Mockito. It also overrides {@code getPageWithContent} to
     * return a fixture directly, bypassing HTTP for that one, permissions-unrelated call: any
     * other call this test does not expect (a resurrected site-permissions lookup among them)
     * still reaches the mock server and shows up in its recorded request paths.
     */
    private static final class PageContentStubbedMicrosoft365Client extends Microsoft365Client {
        private final BaseSitePage fullPage;

        PageContentStubbedMicrosoft365Client(final DataStoreParams params, final BaseSitePage fullPage) {
            super(params);
            this.fullPage = fullPage;
        }

        void useServer(final GraphServiceClient graphClient) {
            this.client = graphClient;
        }

        @Override
        public BaseSitePage getPageWithContent(final String siteId, final String pageId) {
            return fullPage;
        }
    }

    /**
     * The data config's own Permissions field arrives in {@code defaultDataMap} under the role
     * index field, and {@code processPage} folds it into the page's ACL on top of
     * {@code default_permissions}.
     *
     * <p>{@code test_processPage_doesNotRequestSitePermissions} above passes an empty
     * {@code defaultDataMap}, so it pins the {@code default_permissions} half only; dropping the
     * fold left the configured roles off every page with that test still green. Pins both halves
     * and their order.</p>
     */
    @Test
    public void test_processPage_foldsDefaultDataMapRoleIntoPageRoles() throws Exception {
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
        scriptMap.put(roleField, "page.roles");

        // Same convertValue seam as test_processPage_doesNotRequestSitePermissions above: the real
        // path goes through ComponentUtil.getScriptEngineFactory(), which this unit test has no
        // business standing up. processPage itself runs unmodified.
        final SharePointPageDataStore roleAwareDataStore = new SharePointPageDataStore() {
            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                if ("page.roles".equals(template) && resultMap.get(PAGE) instanceof final Map<?, ?> pageDataMap) {
                    return pageDataMap.get(PAGE_ROLES);
                }
                return super.convertValue(scriptType, template, resultMap);
            }
        };

        final SitePage fullPage = new SitePage();
        fullPage.setId("page-1");
        fullPage.setTitle("Test Page");
        fullPage.setWebUrl("https://example.sharepoint.com/sites/site-1/SitePages/test.aspx");

        // getPageWithContent is the only Graph call processPage makes, and this stub answers it
        // directly, so no transport is stood up.
        try (PageContentStubbedMicrosoft365Client client = new PageContentStubbedMicrosoft365Client(dummyParams(), fullPage)) {
            final Site site = new Site();
            site.setId("site-1");
            site.setDisplayName("Site");
            site.setWebUrl("https://example.sharepoint.com/sites/site-1");

            final SitePage page = new SitePage();
            page.setId("page-1");
            page.setWebUrl("https://example.sharepoint.com/sites/site-1/SitePages/test.aspx");
            page.setTitle("Test Page");

            final DataStoreParams paramMap = new DataStoreParams();
            paramMap.put(SharePointPageDataStore.DEFAULT_PERMISSIONS, "{role}admin");

            final Map<String, Object> defaultDataMap = new HashMap<>();
            defaultDataMap.put(roleField, List.of("1config-role"));

            final TestCallback callback = new TestCallback();
            roleAwareDataStore.processPage(new DataConfig(), callback, new LinkedHashMap<>(), paramMap, scriptMap, defaultDataMap, client,
                    site, page);

            assertEquals("processPage must have indexed the page exactly once", 1, callback.getCount());

            @SuppressWarnings("unchecked")
            final List<String> roles = (List<String>) callback.getLastDataMap().get(roleField);
            assertEquals("the page's ACL must hold default_permissions first, then the data config's own roles",
                    List.of(permissionHelper.encode("{role}admin"), "1config-role"), roles);
        }
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
     * A malformed pattern used to be logged and swallowed by {@code getPattern}, and null reads
     * to {@code isTargetPage} as "no filtering" - so a mistyped {@code exclude_pattern} indexed
     * the pages it was meant to keep out while the job reported success. Pins that the crawl
     * fails instead, once, before the first Graph call rather than once per site.
     */
    @Test
    public void test_storeData_malformedExcludePatternFailsBeforeAnyGraphCall() {
        final java.util.concurrent.atomic.AtomicInteger clientsCreated = new java.util.concurrent.atomic.AtomicInteger();
        final SharePointPageDataStore testDataStore = new SharePointPageDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                clientsCreated.incrementAndGet();
                throw new AssertionError("storeData must fail on the malformed pattern before creating a client");
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", ".*private.*[");

        final DataStoreException e = assertThrows(DataStoreException.class,
                () -> testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>()));
        assertTrue("the failure must name the parameter, got: " + e.getMessage(), e.getMessage().contains("exclude_pattern"));
        assertEquals("no Graph client may be created for a crawl that cannot honour its own filter", 0, clientsCreated.get());
    }

    /**
     * {@link PermissionHelper#systemHelper} is {@code protected} and {@code @Resource}-injected
     * by the full LastaFlute container, which this minimal test container does not run;
     * {@link ComponentUtil#register} stores a plain instance without processing that annotation.
     * This subclass exposes a same-package-crossing setter so the field can be wired by hand.
     */
    private static final class TestablePermissionHelper extends PermissionHelper {
        void useSystemHelper(final org.codelibs.fess.helper.SystemHelper systemHelper) {
            this.systemHelper = systemHelper;
        }
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
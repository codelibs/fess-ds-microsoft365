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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.fess.Constants;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.NotebookScope;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.script.ScriptEngineFactory;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.microsoft.graph.models.ChatMessage;
import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.ExternalLink;
import com.microsoft.graph.models.FieldValueSet;
import com.microsoft.graph.models.ListInfo;
import com.microsoft.graph.models.ListItem;
import com.microsoft.graph.models.Notebook;
import com.microsoft.graph.models.NotebookLinks;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.models.SitePage;

/**
 * Pins where the per-document {@link StatsKeyObject} lives, for all six data stores.
 *
 * <p>
 * {@link Constants#CRAWLER_STATS_KEY} identifies one document for statistics and logging. It is
 * not crawl state, so it must not be written to the {@link DataStoreParams} instance that every
 * worker thread shares: each store wrote the key and then read it back one {@code callback.store}
 * later, and between those two points another worker could overwrite it.
 * {@link Microsoft365DataStore#newStatsParams} moves the key onto a per-document copy.
 * </p>
 *
 * <p>
 * These tests do not need {@code number_of_threads > 1} to be meaningful, and deliberately do not
 * use it: a race reproduces unreliably, whereas "the shared map was never written to" and "each
 * document brought its own instance" are exact properties that hold at any thread count and fail
 * deterministically if the direct {@code paramMap.put} ever comes back.
 * </p>
 *
 * <p>
 * Every store test also asserts the second half of the defect, which the copy closes on its own:
 * each store seeds its script scope with {@code new LinkedHashMap<>(paramMap.asMap())}, so while
 * the key was written to the shared map it was copied there too. Groovy could not reach it by
 * name -- {@code "crawler.stats.key"} contains dots, so the name resolves as property navigation
 * rather than as a binding -- but {@code AbstractDataStore#convertValue} returns a value verbatim
 * when a script template matches a resultMap key exactly, which needs no script syntax at all.
 * The {@code stats_leak} entry in every scriptMap below drives exactly that path.
 * </p>
 */
public class Microsoft365DataStoreStatsKeyTest extends UnitDsTestCase {

    /** The scriptMap field name used to observe the exact-match path into the script scope. */
    private static final String STATS_LEAK = "stats_leak";

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);

        // systemHelper, crawlerStatsHelper and permissionHelper are not wired into test_app.xml;
        // the process methods below reach all three through ComponentUtil, and permissionHelper's
        // systemHelper is @Resource-injected, which plain register(...) does not perform.
        final SystemHelper systemHelper = new SystemHelper();
        ComponentUtil.register(systemHelper, "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        final TestablePermissionHelper permissionHelper = new TestablePermissionHelper();
        permissionHelper.useSystemHelper(systemHelper);
        ComponentUtil.register(permissionHelper, "permissionHelper");

        // convertValue falls through to the script engine for every template it cannot match
        // exactly. A dot-path resolver stands in for Groovy so that fall-through is observable as
        // null rather than as a missing-engine exception -- which is what makes "stats_leak came
        // back null" evidence that the exact-match branch did not fire.
        final ScriptEngineFactory scriptEngineFactory = new ScriptEngineFactory();
        scriptEngineFactory.add(Constants.LEGACY_SCRIPT, (template, resultMap) -> {
            Object value = resultMap;
            for (final String part : template.split("\\.")) {
                if (!(value instanceof Map)) {
                    return null;
                }
                value = ((Map<?, ?>) value).get(part);
            }
            return value;
        });
        ComponentUtil.register(scriptEngineFactory, "scriptEngineFactory");
    }

    /**
     * {@code newStatsParams} copies rather than mutates, which is the property every store test
     * below depends on.
     */
    @Test
    public void test_newStatsParams_copiesRatherThanMutating() {
        final Microsoft365DataStore dataStore = new OneDriveDataStore();
        final DataStoreParams paramMap = sharedParams();
        final StatsKeyObject statsKey = new StatsKeyObject("https://example.com/doc-1");

        final DataStoreParams localParams = dataStore.newStatsParams(paramMap, statsKey);

        assertNotSame("a copy, not the same instance", paramMap, localParams);
        assertNull("the original must not gain the key", paramMap.get(Constants.CRAWLER_STATS_KEY));
        assertSame("the copy carries the key given to it", statsKey, localParams.get(Constants.CRAWLER_STATS_KEY));
        assertEquals("the copy carries the original's entries", "tenant-1", localParams.getAsString("tenant"));

        // A second copy of the same original is independent of the first, which is what makes
        // concurrent workers safe rather than merely differently ordered.
        final StatsKeyObject other = new StatsKeyObject("https://example.com/doc-2");
        final DataStoreParams otherParams = dataStore.newStatsParams(paramMap, other);
        assertSame("the first copy keeps its own key", statsKey, localParams.get(Constants.CRAWLER_STATS_KEY));
        assertSame("the second copy carries its own key", other, otherParams.get(Constants.CRAWLER_STATS_KEY));
    }

    /**
     * {@code OneDriveDataStore#processDriveItem} dispatches one task per drive item from four
     * call sites, all onto the shared pool.
     */
    @Test
    public void test_processDriveItem_putsTheStatsKeyOnAPerDocumentCopy() {
        final OneDriveDataStore dataStore = new OneDriveDataStore() {
            @Override
            protected List<String> getDriveItemPermissions(final Microsoft365Client client, final String driveId, final DriveItem item,
                    final DataStoreParams paramMap) {
                return new ArrayList<>();
            }

            @Override
            protected String getDriveItemContents(final Microsoft365Client client, final String driveId, final DriveItem item,
                    final long maxContentLength, final boolean ignoreError) {
                return "content";
            }
        };

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.IGNORE_FOLDER, Boolean.FALSE);
        configMap.put(OneDriveDataStore.IGNORE_ERROR, Boolean.FALSE);
        configMap.put(OneDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });
        configMap.put(OneDriveDataStore.MAX_CONTENT_LENGTH, Long.valueOf(1000000L));

        final DataStoreParams paramMap = sharedParams();
        final RecordingCallback callback = new RecordingCallback();

        for (final String id : List.of("item-1", "item-2")) {
            final DriveItem item = new DriveItem();
            item.setId(id);
            item.setName(id + ".txt");
            item.setWebUrl("https://example.com/" + id);
            dataStore.processDriveItem(new DataConfig(), callback, configMap, paramMap, leakScriptMap(), new HashMap<>(), null, "drive-1",
                    item, new ArrayList<>());
        }

        assertPerDocumentStatsKey(paramMap, callback, List.of("https://example.com/item-1", "https://example.com/item-2"));
    }

    /**
     * {@code OneNoteDataStore#processNotebook} dispatches one task per notebook from three call
     * sites, and is one of the two stores whose script scope was seeded <em>before</em> the write
     * -- so it carried the previously processed document's key rather than the current one.
     */
    @Test
    public void test_processNotebook_putsTheStatsKeyOnAPerDocumentCopy() {
        final OneNoteDataStore dataStore = new OneNoteDataStore();
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.getNotebookContent(NotebookScope.USER, "user-1", "notebook-1")).thenReturn("first");
        when(client.getNotebookContent(NotebookScope.USER, "user-1", "notebook-2")).thenReturn("second");

        final DataStoreParams paramMap = sharedParams();
        final RecordingCallback callback = new RecordingCallback();

        for (final String id : List.of("notebook-1", "notebook-2")) {
            final Notebook notebook = new Notebook();
            notebook.setId(id);
            notebook.setDisplayName(id);
            final NotebookLinks links = new NotebookLinks();
            final ExternalLink webUrl = new ExternalLink();
            webUrl.setHref("https://example.com/" + id);
            links.setOneNoteWebUrl(webUrl);
            notebook.setLinks(links);
            dataStore.processNotebook(new DataConfig(), callback, paramMap, leakScriptMap(), new HashMap<>(), client, NotebookScope.USER,
                    "user-1", notebook, Collections.emptyList());
        }

        // processNotebook keys its stats on the notebook id, not on a URL.
        assertPerDocumentStatsKey(paramMap, callback, List.of("notebook-1", "notebook-2"));
    }

    /**
     * {@code SharePointDocLibDataStore#storeDocumentLibrary} runs one task per document library
     * on the shared pool.
     */
    @Test
    public void test_storeDocumentLibrary_putsTheStatsKeyOnAPerDocumentCopy() {
        final SharePointDocLibDataStore dataStore = new SharePointDocLibDataStore() {
            @Override
            protected List<String> getDrivePermissions(final Microsoft365Client client, final String driveId,
                    final DataStoreParams paramMap) {
                return new ArrayList<>();
            }
        };

        final Site site = new Site();
        site.setId("site-1");
        site.setDisplayName("Site");
        site.setWebUrl("https://example.sharepoint.com/sites/site-1");

        final DataStoreParams paramMap = sharedParams();
        final RecordingCallback callback = new RecordingCallback();

        for (final String id : List.of("drive-1", "drive-2")) {
            final Drive drive = new Drive();
            drive.setId(id);
            drive.setName(id);
            drive.setWebUrl("https://example.sharepoint.com/sites/site-1/" + id);
            dataStore.storeDocumentLibrary(new DataConfig(), callback, new HashMap<>(), paramMap, leakScriptMap(), new HashMap<>(), null,
                    site, drive);
        }

        assertPerDocumentStatsKey(paramMap, callback,
                List.of("https://example.sharepoint.com/sites/site-1/drive-1", "https://example.sharepoint.com/sites/site-1/drive-2"));
    }

    /**
     * {@code SharePointListDataStore#processListItem} runs one task per list item on the shared
     * pool.
     */
    @Test
    public void test_processListItem_putsTheStatsKeyOnAPerDocumentCopy() {
        final SharePointListDataStore dataStore = new SharePointListDataStore();

        final Site site = new Site();
        site.setId("site-1");
        site.setDisplayName("Site");
        site.setWebUrl("https://example.sharepoint.com/sites/site-1");

        final ListInfo info = new ListInfo();
        info.setTemplate("genericList");
        final com.microsoft.graph.models.List list = new com.microsoft.graph.models.List();
        list.setId("list-1");
        list.setDisplayName("List");
        list.setWebUrl("https://example.sharepoint.com/sites/site-1/Lists/List");
        list.setList(info);

        final Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put(SharePointListDataStore.IGNORE_ERROR, Boolean.FALSE);

        final DataStoreParams paramMap = sharedParams();
        final RecordingCallback callback = new RecordingCallback();

        for (final String id : List.of("item-1", "item-2")) {
            final ListItem item = new ListItem();
            item.setId(id);
            item.setWebUrl("https://example.sharepoint.com/sites/site-1/Lists/List/" + id);
            final FieldValueSet fields = new FieldValueSet();
            final Map<String, Object> additionalData = new HashMap<>();
            additionalData.put("Title", id);
            fields.setAdditionalData(additionalData);
            item.setFields(fields);
            // The client is only reached to refresh empty fields; these items carry theirs.
            dataStore.processListItem(new DataConfig(), callback, configMap, paramMap, leakScriptMap(), new HashMap<>(), null, site, list,
                    item);
        }

        assertPerDocumentStatsKey(paramMap, callback, List.of("https://example.sharepoint.com/sites/site-1/Lists/List/item-1",
                "https://example.sharepoint.com/sites/site-1/Lists/List/item-2"));
    }

    /**
     * {@code SharePointPageDataStore#processPage} runs one task per site page on the shared pool.
     */
    @Test
    public void test_processPage_putsTheStatsKeyOnAPerDocumentCopy() {
        final SharePointPageDataStore dataStore = new SharePointPageDataStore();
        final Microsoft365Client client = mock(Microsoft365Client.class);

        final Site site = new Site();
        site.setId("site-1");
        site.setDisplayName("Site");
        site.setWebUrl("https://example.sharepoint.com/sites/site-1");

        final DataStoreParams paramMap = sharedParams();
        final RecordingCallback callback = new RecordingCallback();

        for (final String id : List.of("page-1", "page-2")) {
            final SitePage page = new SitePage();
            page.setId(id);
            page.setTitle(id);
            page.setWebUrl("https://example.sharepoint.com/sites/site-1/SitePages/" + id + ".aspx");
            when(client.getPageWithContent("site-1", id)).thenReturn(page);
            dataStore.processPage(new DataConfig(), callback, new LinkedHashMap<>(), paramMap, leakScriptMap(), new HashMap<>(), client,
                    site, page);
        }

        assertPerDocumentStatsKey(paramMap, callback, List.of("https://example.sharepoint.com/sites/site-1/SitePages/page-1.aspx",
                "https://example.sharepoint.com/sites/site-1/SitePages/page-2.aspx"));
    }

    /**
     * {@code TeamsDataStore#processChatMessage} is reached from the chat and channel dispatch
     * paths, and is the other store whose script scope was seeded before the write.
     */
    @Test
    public void test_processChatMessage_putsTheStatsKeyOnAPerDocumentCopy() {
        final TeamsDataStore dataStore = new TeamsDataStore();

        final DataStoreParams paramMap = sharedParams();
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("ignore_system_events", dataStore.isIgnoreSystemEvents(paramMap));
        configMap.put("append_attachment", Boolean.FALSE);
        configMap.put("title_dateformat", dataStore.getTitleDateformat(paramMap));
        configMap.put("title_timezone_offset", dataStore.getTitleTimezone(paramMap));

        final RecordingCallback callback = new RecordingCallback();

        for (final String id : List.of("message-1", "message-2")) {
            final ChatMessage message = new ChatMessage();
            message.setId(id);
            message.setCreatedDateTime(OffsetDateTime.parse("2026-06-01T00:00:00Z"));
            message.setWebUrl("https://teams.microsoft.com/l/message/" + id);
            dataStore.processChatMessage(new DataConfig(), callback, configMap, paramMap, leakScriptMap(), new HashMap<>(),
                    new ArrayList<>(), message, map -> {}, null);
        }

        assertPerDocumentStatsKey(paramMap, callback,
                List.of("https://teams.microsoft.com/l/message/message-1", "https://teams.microsoft.com/l/message/message-2"));
    }

    /**
     * Asserts the whole contract for one store: the shared map was never written to, each stored
     * document arrived with its own copy carrying its own stats key, that copy still carries the
     * ordinary parameters a callback or an ingester reads, and the key reached no script scope.
     *
     * @param sharedParams the instance every worker thread would share
     * @param callback the callback the store was driven with
     * @param expectedStatsIds the stats key ids expected, in the order the documents were stored
     */
    private void assertPerDocumentStatsKey(final DataStoreParams sharedParams, final RecordingCallback callback,
            final List<String> expectedStatsIds) {
        assertNull("the stats key must never be written to the map the worker threads share",
                sharedParams.get(Constants.CRAWLER_STATS_KEY));

        assertEquals("every stored document contributes one parameter instance", expectedStatsIds.size(), callback.paramMaps.size());
        assertNotSame("the documents must not share one instance", callback.paramMaps.get(0), callback.paramMaps.get(1));

        for (int i = 0; i < expectedStatsIds.size(); i++) {
            final DataStoreParams localParams = callback.paramMaps.get(i);
            assertNotSame("callback.store must receive the copy, not the shared instance", sharedParams, localParams);

            final Object value = localParams.get(Constants.CRAWLER_STATS_KEY);
            assertNotNull("the copy must still carry the stats key the callback contract expects", value);
            assertTrue("the stats key must be a StatsKeyObject, not its toString", value instanceof StatsKeyObject);
            assertEquals("the stats key must identify this document", expectedStatsIds.get(i), ((StatsKeyObject) value).getId());

            // newInstance() copies the contents, so the ordinary parameters a callback or an
            // ingester reads are still present -- the copy is not an empty map with one key.
            assertEquals("the copy must carry the ordinary parameters too", "tenant-1", localParams.getAsString("tenant"));

            assertNull("internal crawl plumbing must not be indexable", callback.dataMaps.get(i).get(STATS_LEAK));
        }
    }

    private DataStoreParams sharedParams() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("tenant", "tenant-1");
        return paramMap;
    }

    private Map<String, String> leakScriptMap() {
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put(STATS_LEAK, Constants.CRAWLER_STATS_KEY);
        return scriptMap;
    }

    /**
     * Records the {@link DataStoreParams} instance each document was stored with, alongside its
     * data map.
     */
    private static final class RecordingCallback implements IndexUpdateCallback {
        // Deliberately the live reference, not a copy: the point of recording it is to let a test
        // assert which instance arrived -- that each document brought its own copy and that the
        // shared one was never handed over. A defensive copy here would make both assertions
        // unwritable.
        private final List<DataStoreParams> paramMaps = new ArrayList<>();

        private final List<Map<String, Object>> dataMaps = new ArrayList<>();

        @Override
        public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
            paramMaps.add(paramMap);
            dataMaps.add(new HashMap<>(dataMap));
        }

        @Override
        public long getDocumentSize() {
            return dataMaps.size();
        }

        @Override
        public long getExecuteTime() {
            return 0;
        }

        @Override
        public void commit() {
            // do nothing
        }
    }

    /**
     * {@link PermissionHelper#systemHelper} is {@code @Resource}-injected, which plain
     * {@code ComponentUtil.register(...)} does not perform in this minimal test container; this
     * subclass exposes a same-package-crossing setter so the field can be wired by hand.
     */
    private static final class TestablePermissionHelper extends PermissionHelper {
        void useSystemHelper(final SystemHelper systemHelper) {
            this.systemHelper = systemHelper;
        }
    }
}

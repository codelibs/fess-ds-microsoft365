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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.ds.ms365.client.NotebookScope;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.Notebook;
import com.microsoft.graph.models.NotebookCollectionResponse;
import com.microsoft.graph.models.Site;
import com.microsoft.kiota.ApiException;

/**
 * This class is a data store for crawling and indexing content from Microsoft OneNote.
 * It supports crawling notebooks from user accounts, groups, and SharePoint sites.
 * It extracts notebook content, metadata, and permissions for indexing.
 */
public class OneNoteDataStore extends Microsoft365DataStore {

    /**
     * Default constructor.
     */
    public OneNoteDataStore() {
    }

    private static final Logger logger = LogManager.getLogger(OneNoteDataStore.class);

    // scripts
    /** Key for the notebook object in the script map. */
    protected static final String NOTEBOOK = "notebook";
    /** Key for the notebook name in the script map. */
    protected static final String NOTEBOOK_NAME = "name";
    /** Key for the notebook contents in the script map. */
    protected static final String NOTEBOOK_CONTENTS = "contents";
    /** Key for the notebook size in the script map. */
    protected static final String NOTEBOOK_SIZE = "size";
    /** Key for the notebook creation date in the script map. */
    protected static final String NOTEBOOK_CREATED = "created";
    /** Key for the notebook last modified date in the script map. */
    protected static final String NOTEBOOK_LAST_MODIFIED = "last_modified";
    /** Key for the notebook web URL in the script map. */
    protected static final String NOTEBOOK_WEB_URL = "web_url";
    /** Key for the notebook roles in the script map. */
    protected static final String NOTEBOOK_ROLES = "roles";
    /** Parameter name for enabling the site note crawler. */
    protected static final String SITE_NOTE_CRAWLER = "site_note_crawler";
    /** Parameter name for enabling the user note crawler. */
    protected static final String USER_NOTE_CRAWLER = "user_note_crawler";
    /** Parameter name for enabling the group note crawler. */
    protected static final String GROUP_NOTE_CRAWLER = "group_note_crawler";
    /** Internal (non-operator-facing) key used to stash a {@link NotebookFilterStats} in
     *  {@code paramMap} for the duration of one {@link #storeData} call, the same trick
     *  {@code Constants.CRAWLER_STATS_KEY} already uses to carry per-crawl state that way. Not a
     *  parameter: never read from configuration, only written and read by this class. */
    protected static final String NOTEBOOK_FILTER_STATS = "_onenote_notebook_filter_stats";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        if (logger.isDebugEnabled()) {
            logger.debug("OneNote crawling started - Threads: {}, Site: {}, User: {}, Group: {}",
                    paramMap.getAsString(NUMBER_OF_THREADS, "1"), isSiteNoteCrawler(paramMap), isUserNoteCrawler(paramMap),
                    isGroupNoteCrawler(paramMap));
        }

        // A mistyped include_pattern/exclude_pattern can silently exclude every notebook: the
        // crawl still finishes without error, it just indexes nothing. filterStats counts seen
        // vs. admitted notebooks across all three scopes so that case gets exactly one WARN
        // below, not silence and not one WARN per skipped notebook.
        final boolean patternConfigured = StringUtil.isNotBlank(paramMap.getAsString(INCLUDE_PATTERN))
                || StringUtil.isNotBlank(paramMap.getAsString(EXCLUDE_PATTERN));
        final NotebookFilterStats filterStats = new NotebookFilterStats();
        paramMap.put(NOTEBOOK_FILTER_STATS, filterStats);

        final ReportingExecutor executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            if (isSiteNoteCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Starting site notebooks crawling");
                }
                storeSiteNotes(dataConfig, callback, paramMap, scriptMap, defaultDataMap, executorService, client);
                if (logger.isDebugEnabled()) {
                    logger.debug("Completed site notebooks crawling");
                }
            }
            if (isUserNoteCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Starting user notebooks crawling");
                }
                storeUsersNotes(dataConfig, callback, paramMap, scriptMap, defaultDataMap, executorService, client);
                if (logger.isDebugEnabled()) {
                    logger.debug("Completed user notebooks crawling");
                }
            }
            if (isGroupNoteCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Starting group notebooks crawling");
                }
                storeGroupsNotes(dataConfig, callback, paramMap, scriptMap, defaultDataMap, executorService, client);
                if (logger.isDebugEnabled()) {
                    logger.debug("Completed group notebooks crawling");
                }
            }
            if (patternConfigured && filterStats.matchedNothing()) {
                logger.warn(
                        "{}/{} matched none of the {} notebook(s) seen in this crawl (site/user/group scopes combined); "
                                + "the crawl succeeded but indexed nothing. Check the pattern against the notebooks' display names.",
                        INCLUDE_PATTERN, EXCLUDE_PATTERN, filterStats.seenCount());
            }
            if (logger.isDebugEnabled()) {
                logger.debug("OneNote crawling completed - shutting down thread executor");
            }
            shutdownExecutor(executorService, paramMap);
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Checks if the group note crawler is enabled.
     *
     * @param paramMap The data store parameters.
     * @return true if the group note crawler is enabled, false otherwise.
     */
    protected boolean isGroupNoteCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(GROUP_NOTE_CRAWLER, Constants.TRUE));
    }

    /**
     * Checks if the user note crawler is enabled.
     *
     * @param paramMap The data store parameters.
     * @return true if the user note crawler is enabled, false otherwise.
     */
    protected boolean isUserNoteCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(USER_NOTE_CRAWLER, Constants.TRUE));
    }

    /**
     * Checks if the site note crawler is enabled.
     *
     * @param paramMap The data store parameters.
     * @return true if the site note crawler is enabled, false otherwise.
     */
    protected boolean isSiteNoteCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(SITE_NOTE_CRAWLER, Constants.TRUE));
    }

    /**
     * Stores the site notes.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param executorService The executor service.
     * @param client The Microsoft365Client.
     */
    protected void storeSiteNotes(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final ExecutorService executorService,
            final Microsoft365Client client) {
        final Site root;
        try {
            root = client.getSite("root");
        } catch (final ApiException e) {
            // storeData runs storeSiteNotes, storeUsersNotes and storeGroupsNotes one after
            // another inside the same try block. Letting this propagate would abort user and
            // group notebook crawling too, not just site notebooks, so it is caught here and
            // only site notebooks are skipped.
            logger.warn("Skipping site notebooks: unable to resolve the root site.", e);
            return;
        }

        // Graph has no user/group role-assignment endpoint for a site that this plugin can call
        // without Sites.FullControl.All (see Microsoft365Client's removed getSitePermissions),
        // so site notebooks carry no owner-derived roles. default_permissions is their only role
        // source.
        final List<String> roles = getDefaultPermissions(paramMap);
        final Pattern includePattern = getPattern(paramMap, INCLUDE_PATTERN);
        final Pattern excludePattern = getPattern(paramMap, EXCLUDE_PATTERN);
        getNotebooks(client, NotebookScope.SITE, root.getId(), notebook -> {
            if (!isTargetNotebookTracked(paramMap, includePattern, excludePattern, notebook)) {
                return;
            }
            executorService.execute(() -> processNotebook(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client,
                    NotebookScope.SITE, root.getId(), notebook, roles));
        });
    }

    /**
     * Stores the users' notes.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param executorService The executor service.
     * @param client The Microsoft365Client.
     */
    protected void storeUsersNotes(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final ExecutorService executorService,
            final Microsoft365Client client) {

        if (logger.isDebugEnabled()) {
            logger.debug("Starting user notebooks processing - retrieving licensed users");
        }

        final Pattern includePattern = getPattern(paramMap, INCLUDE_PATTERN);
        final Pattern excludePattern = getPattern(paramMap, EXCLUDE_PATTERN);
        getLicensedUsers(client, user -> {
            // A user notebook already derives roles from its owner; default_permissions adds to
            // that list, it does not replace it. getUserRoles returns an immutable singleton
            // list, so the combined list is built fresh here rather than mutated in place.
            final List<String> roles = new ArrayList<>(getUserRoles(user));
            roles.addAll(getDefaultPermissions(paramMap));

            if (logger.isDebugEnabled()) {
                logger.debug("Processing notebooks for user: {} (ID: {})", user.getDisplayName(), user.getId());
            }

            try {
                getNotebooks(client, NotebookScope.USER, user.getId(), notebook -> {
                    if (!isTargetNotebookTracked(paramMap, includePattern, excludePattern, notebook)) {
                        return;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing notebook: {} for user: {}", notebook.getDisplayName(), user.getDisplayName());
                    }
                    executorService.execute(() -> processNotebook(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client,
                            NotebookScope.USER, user.getId(), notebook, roles));
                });
            } catch (final ApiException e) {
                logger.warn("Failed to retrieve notebooks for user: {} (ID: {})", user.getDisplayName(), user.getId(), e);
            }
        });
    }

    /**
     * Stores the groups' notes.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param executorService The executor service.
     * @param client The Microsoft365Client.
     */
    protected void storeGroupsNotes(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final ExecutorService executorService,
            final Microsoft365Client client) {

        if (logger.isDebugEnabled()) {
            logger.debug("Starting group notebooks processing - retrieving Microsoft 365 groups");
        }

        final Pattern includePattern = getPattern(paramMap, INCLUDE_PATTERN);
        final Pattern excludePattern = getPattern(paramMap, EXCLUDE_PATTERN);
        getMicrosoft365Groups(client, group -> {
            // A group notebook already derives roles from its owner; default_permissions adds to
            // that list, it does not replace it. getGroupRoles returns an immutable singleton
            // list, so the combined list is built fresh here rather than mutated in place.
            final List<String> roles = new ArrayList<>(getGroupRoles(group));
            roles.addAll(getDefaultPermissions(paramMap));

            if (logger.isDebugEnabled()) {
                logger.debug("Processing notebooks for group: {} (ID: {})", group.getDisplayName(), group.getId());
            }

            try {
                getNotebooks(client, NotebookScope.GROUP, group.getId(), notebook -> {
                    if (!isTargetNotebookTracked(paramMap, includePattern, excludePattern, notebook)) {
                        return;
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing notebook: {} for group: {}", notebook.getDisplayName(), group.getDisplayName());
                    }
                    executorService.execute(() -> processNotebook(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client,
                            NotebookScope.GROUP, group.getId(), notebook, roles));
                });
            } catch (final Exception e) {
                logger.warn("Failed to retrieve notebooks for group: {} (ID: {})", group.getDisplayName(), group.getId(), e);
            }
        });

    }

    /**
     * Processes a notebook.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The Microsoft365Client.
     * @param scope which Graph root the notebook lives under.
     * @param ownerId The user, site or group ID.
     * @param notebook The notebook.
     * @param roles The roles.
     */
    protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
            final NotebookScope scope, final String ownerId, final Notebook notebook, final List<String> roles) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
        final Map<String, Object> notebooksMap = new HashMap<>();
        final StatsKeyObject statsKey = new StatsKeyObject(notebook.getId());
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);

        if (logger.isDebugEnabled()) {
            logger.debug("Processing notebook: {} (ID: {}) for {} {} - Roles: {}", notebook.getDisplayName(), notebook.getId(), scope,
                    ownerId, roles.size());
        }

        // declared outside the try so the catch arms can key the failure-URL row by it: the row
        // is looked up with setUrl_Equal, so two notebooks sharing a display name would otherwise
        // collapse into one row and hide a failure.
        String url = null;
        try {
            crawlerStatsHelper.begin(statsKey);
            url = notebook.getLinks().getOneNoteWebUrl().getHref();
            logger.info("Crawling notebook URL: {} (Name: {})", url, notebook.getDisplayName());

            if (logger.isDebugEnabled()) {
                logger.debug("Retrieving notebook content for notebook: {} (ID: {})", notebook.getDisplayName(), notebook.getId());
            }

            final String contents = client.getNotebookContent(scope, ownerId, notebook.getId());
            final long size = contents != null ? contents.length() : 0L;

            if (logger.isDebugEnabled()) {
                logger.debug("Retrieved notebook content - Name: {}, Size: {} characters, Created: {}, Modified: {}",
                        notebook.getDisplayName(), size, notebook.getCreatedDateTime(), notebook.getLastModifiedDateTime());
            }

            notebooksMap.put(NOTEBOOK_NAME, notebook.getDisplayName());
            notebooksMap.put(NOTEBOOK_CONTENTS, contents);
            notebooksMap.put(NOTEBOOK_SIZE, size);
            notebooksMap.put(NOTEBOOK_CREATED, notebook.getCreatedDateTime());
            notebooksMap.put(NOTEBOOK_LAST_MODIFIED, notebook.getLastModifiedDateTime());
            notebooksMap.put(NOTEBOOK_WEB_URL, url);

            // roles may be shared across concurrent notebook-processing threads for the same
            // owner (storeUsersNotes/storeGroupsNotes build it once per user/group before
            // dispatching one executorService task per notebook), so it must not be mutated in
            // place here. The data config's own Permissions field -- seeded into defaultDataMap
            // under the role index field -- is folded in the same way every sibling data store
            // folds it, so it is not silently discarded when the script maps role=notebook.roles.
            final List<String> finalRoles = mergeDefaultRoles(roles, defaultDataMap).stream().distinct().collect(Collectors.toList());
            notebooksMap.put(NOTEBOOK_ROLES, finalRoles);

            resultMap.put(NOTEBOOK, notebooksMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("Prepared notebook data - Fields: {}, URL: {}", notebooksMap.size(), url);
            }

            final String scriptType = getScriptType(paramMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

            if (logger.isDebugEnabled()) {
                logger.debug("Final data map prepared for indexing - Fields: {}, URL: {}", dataMap.size(), dataMap.get("url"));
            }

            if (dataMap.get("url") instanceof final String statsUrl) {
                statsKey.setUrl(statsUrl);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully indexed notebook: {} (ID: {})", notebook.getDisplayName(), notebook.getId());
            }
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception for notebook: {} (ID: {}) - Data: {}", notebook.getDisplayName(), notebook.getId(),
                    dataMap, e);
            handleCrawlingException(dataConfig, crawlerStatsHelper, statsKey, failureUrlOf(url, notebook), e);
        } catch (final Throwable t) {
            logger.warn("Processing exception for notebook: {} (ID: {}) - Data: {}", notebook.getDisplayName(), notebook.getId(), dataMap,
                    t);
            handleCrawlingThrowable(dataConfig, crawlerStatsHelper, statsKey, failureUrlOf(url, notebook), t);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Chooses the value a failed notebook is recorded under in the Failure URL screen.
     *
     * <p>{@code FailureUrlService.store} looks the row up with {@code setUrl_Equal}, so this value
     * is the row key. The notebook's own web URL is the only one of the three that is unique per
     * notebook; the id is unique too but is not a URL an operator can follow, and the display name
     * is not unique at all -- two notebooks that share one would collapse into a single row and an
     * operator would see one failure where there were two.</p>
     *
     * <p>Package-private and static on purpose: it is pure, it is not part of the subclassing
     * surface, and the tests in this package call it directly.</p>
     *
     * @param url the notebook's web URL, or {@code null}/blank when the failure happened before it
     *            was read
     * @param notebook the notebook being processed
     * @return the web URL when there is one, otherwise the notebook id, otherwise its display name
     */
    static String failureUrlOf(final String url, final Notebook notebook) {
        if (StringUtil.isNotBlank(url)) {
            return url;
        }
        if (StringUtil.isNotBlank(notebook.getId())) {
            return notebook.getId();
        }
        return notebook.getDisplayName();
    }

    /**
     * Gets the notebooks.
     *
     * @param client The Microsoft365Client.
     * @param scope which Graph root the notebooks live under.
     * @param ownerId The user, site or group ID.
     * @param consumer The consumer to process each notebook.
     */
    protected void getNotebooks(final Microsoft365Client client, final NotebookScope scope, final String ownerId,
            final Consumer<Notebook> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieving notebooks for {} {}", scope, ownerId);
        }

        try {
            final NotebookCollectionResponse response = client.getNotebookPage(scope, ownerId);
            if (response.getValue() != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Retrieved {} notebooks for {} {}", response.getValue().size(), scope, ownerId);
                }
                response.getValue().forEach(notebook -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing notebook: {} (ID: {}) for {} {}", notebook.getDisplayName(), notebook.getId(), scope,
                                ownerId);
                    }
                    consumer.accept(notebook);
                });
            } else if (logger.isDebugEnabled()) {
                logger.debug("No notebooks found for {} {}", scope, ownerId);
            }
        } catch (final ApiException e) {
            if (e.getResponseStatusCode() == 404) {
                if (scope == NotebookScope.USER) {
                    // A user without a provisioned personal site 404s here routinely, and USER was never
                    // the path this fix repaired -- logging one line per licensed user buys nothing.
                    if (logger.isDebugEnabled()) {
                        logger.debug("No notebooks found for {} {} (404).", scope, ownerId);
                    }
                } else {
                    // SITE and GROUP used to 404 because the request went to the wrong Graph root, so
                    // these stay visible.
                    logger.warn("No notebooks found for {} {} (404).", scope, ownerId);
                }
            } else {
                logger.warn("Failed to retrieve notebooks for {} {}.", scope, ownerId, e);
            }
        }
    }

    /**
     * Checks whether a notebook passes the configured include/exclude filters.
     *
     * <p>Both patterns are matched against the notebook's display name as a <em>full</em> match
     * ({@link java.util.regex.Matcher#matches()}), the same semantics
     * {@code sharePointListDataStore} applies to a list item's title. A notebook with no display
     * name is matched as the empty string rather than bypassing the filters: an operator who set
     * {@code include_pattern} has said what they want indexed, and an unnamed notebook is not it,
     * so such a notebook is excluded by any {@code include_pattern} that does not match {@code ""}
     * and kept under an {@code exclude_pattern} that does not match {@code ""}.</p>
     *
     * @param includePattern the include pattern, or null for no include filtering
     * @param excludePattern the exclude pattern, or null for no exclude filtering
     * @param notebook the notebook to check
     * @return true if the notebook should be crawled, false otherwise
     */
    protected boolean isTargetNotebook(final Pattern includePattern, final Pattern excludePattern, final Notebook notebook) {
        if (includePattern == null && excludePattern == null) {
            return true;
        }
        // A missing name is matched as "" rather than special-cased into an unconditional pass:
        // the configured patterns decide, with the same full-match semantics as every other name.
        final String displayName = notebook.getDisplayName() != null ? notebook.getDisplayName() : StringUtil.EMPTY;
        if (includePattern != null && !includePattern.matcher(displayName).matches()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping notebook {}: does not match {}", displayName, INCLUDE_PATTERN);
            }
            return false;
        }
        if (excludePattern != null && excludePattern.matcher(displayName).matches()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping notebook {}: matches {}", displayName, EXCLUDE_PATTERN);
            }
            return false;
        }
        return true;
    }

    /**
     * As {@link #isTargetNotebook}, but also records the decision into the {@link
     * NotebookFilterStats} stashed in {@code paramMap} by {@link #storeData}, so a crawl where a
     * configured pattern matched no notebooks at all can be reported once instead of not at all.
     *
     * <p>Called from all three scope consumers ({@code storeSiteNotes}, {@code storeUsersNotes},
     * {@code storeGroupsNotes}) instead of {@link #isTargetNotebook} directly.</p>
     *
     * @param paramMap the data store parameters
     * @param includePattern the include pattern, or null for no include filtering
     * @param excludePattern the exclude pattern, or null for no exclude filtering
     * @param notebook the notebook to check
     * @return true if the notebook should be crawled, false otherwise
     */
    protected boolean isTargetNotebookTracked(final DataStoreParams paramMap, final Pattern includePattern, final Pattern excludePattern,
            final Notebook notebook) {
        final NotebookFilterStats stats = getFilterStats(paramMap);
        stats.recordSeen();
        final boolean target = isTargetNotebook(includePattern, excludePattern, notebook);
        if (target) {
            stats.recordAdmitted();
        }
        return target;
    }

    /**
     * Reads the {@link NotebookFilterStats} {@link #storeData} stashed in {@code paramMap}.
     *
     * <p>Returns a throwaway instance when absent, e.g. when a test or caller invokes {@code
     * storeSiteNotes}/{@code storeUsersNotes}/{@code storeGroupsNotes} directly without going
     * through {@code storeData} first; nothing ever reads that instance back, so counting into it
     * is harmless.</p>
     *
     * @param paramMap the data store parameters
     * @return the crawl's notebook filter counters
     */
    private NotebookFilterStats getFilterStats(final DataStoreParams paramMap) {
        final Object stats = paramMap.get(NOTEBOOK_FILTER_STATS);
        return stats instanceof final NotebookFilterStats notebookFilterStats ? notebookFilterStats : new NotebookFilterStats();
    }

    /**
     * Per-crawl notebook include/exclude filter counters, shared across the SITE, USER and GROUP
     * scopes of a single {@link #storeData} call so it can warn exactly once if a configured
     * pattern matched no notebooks anywhere, instead of once per skipped notebook.
     */
    protected static final class NotebookFilterStats {
        private final AtomicInteger seen = new AtomicInteger();
        private final AtomicInteger admitted = new AtomicInteger();

        /**
         * Default constructor.
         */
        NotebookFilterStats() {
        }

        private void recordSeen() {
            seen.incrementAndGet();
        }

        private void recordAdmitted() {
            admitted.incrementAndGet();
        }

        /**
         * @return the number of notebooks seen (checked against the filter), across every scope
         */
        int seenCount() {
            return seen.get();
        }

        /**
         * @return true if at least one notebook was seen and none of them were admitted
         */
        boolean matchedNothing() {
            return seen.get() > 0 && admitted.get() == 0;
        }
    }

}

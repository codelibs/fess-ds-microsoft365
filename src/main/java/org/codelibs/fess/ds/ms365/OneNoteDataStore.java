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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
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
        super();
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
    /** Parameter name for the number of threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** Parameter name for enabling the site note crawler. */
    protected static final String SITE_NOTE_CRAWLER = "site_note_crawler";
    /** Parameter name for enabling the user note crawler. */
    protected static final String USER_NOTE_CRAWLER = "user_note_crawler";
    /** Parameter name for enabling the group note crawler. */
    protected static final String GROUP_NOTE_CRAWLER = "group_note_crawler";

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

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
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
            if (logger.isDebugEnabled()) {
                logger.debug("OneNote crawling completed - shutting down thread executor");
            }
            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new InterruptedRuntimeException(e);
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
        final Site root = client.getSite("root");
        final List<String> roles = Collections.emptyList();
        getNotebooks(client, "sites/" + root.getId(), notebook -> executorService.execute(() -> processNotebook(dataConfig, callback,
                paramMap, scriptMap, defaultDataMap, client, "sites/" + root.getId(), notebook, roles)));
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

        getLicensedUsers(client, user -> {
            final List<String> roles = getUserRoles(user);

            if (logger.isDebugEnabled()) {
                logger.debug("Processing notebooks for user: {} (ID: {})", user.getDisplayName(), user.getId());
            }

            try {
                getNotebooks(client, user.getId(), notebook -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing notebook: {} for user: {}", notebook.getDisplayName(), user.getDisplayName());
                    }
                    executorService.execute(() -> processNotebook(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client,
                            user.getId(), notebook, roles));
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

        getMicrosoft365Groups(client, group -> {
            final List<String> roles = getGroupRoles(group);
            final String groupPath = "groups/" + group.getId();

            if (logger.isDebugEnabled()) {
                logger.debug("Processing notebooks for group: {} (ID: {}, Path: {})", group.getDisplayName(), group.getId(), groupPath);
            }

            try {
                getNotebooks(client, groupPath, notebook -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing notebook: {} for group: {}", notebook.getDisplayName(), group.getDisplayName());
                    }
                    executorService.execute(() -> processNotebook(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client,
                            groupPath, notebook, roles));
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
     * @param userId The user ID.
     * @param notebook The notebook.
     * @param roles The roles.
     */
    protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Microsoft365Client client,
            final String userId, final Notebook notebook, final List<String> roles) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
        final Map<String, Object> notebooksMap = new HashMap<>();
        final StatsKeyObject statsKey = new StatsKeyObject(notebook.getId());
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);

        if (logger.isDebugEnabled()) {
            logger.debug("Processing notebook: {} (ID: {}) for user/group: {} - Roles: {}", notebook.getDisplayName(), notebook.getId(),
                    userId, roles.size());
        }

        try {
            crawlerStatsHelper.begin(statsKey);
            final String url = notebook.getLinks().getOneNoteWebUrl().getHref();
            logger.info("Crawling notebook URL: {} (Name: {})", url, notebook.getDisplayName());

            if (logger.isDebugEnabled()) {
                logger.debug("Retrieving notebook content for notebook: {} (ID: {})", notebook.getDisplayName(), notebook.getId());
            }

            final String contents = client.getNotebookContent(userId, notebook.getId());
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
            notebooksMap.put(NOTEBOOK_ROLES, roles);

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

            Throwable target = e;
            if (target instanceof final MultipleCrawlingAccessException ex) {
                final Throwable[] causes = ex.getCauses();
                if (causes.length > 0) {
                    target = causes[causes.length - 1];
                }
            }

            String errorName;
            final Throwable cause = target.getCause();
            if (cause != null) {
                errorName = cause.getClass().getCanonicalName();
            } else {
                errorName = target.getClass().getCanonicalName();
            }

            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, notebook.getDisplayName(), target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Processing exception for notebook: {} (ID: {}) - Data: {}", notebook.getDisplayName(), notebook.getId(), dataMap,
                    t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), notebook.getDisplayName(), t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Gets the notebooks.
     *
     * @param client The Microsoft365Client.
     * @param userId The user ID.
     * @param consumer The consumer to process each notebook.
     */
    protected void getNotebooks(final Microsoft365Client client, final String userId, final Consumer<Notebook> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieving notebooks for user/group: {}", userId);
        }

        try {
            NotebookCollectionResponse response = client.getNotebookPage(userId);
            if (response.getValue() != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Retrieved {} notebooks for user/group: {}", response.getValue().size(), userId);
                }
                response.getValue().forEach(notebook -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing notebook: {} (ID: {}) for user/group: {}", notebook.getDisplayName(), notebook.getId(),
                                userId);
                    }
                    consumer.accept(notebook);
                });
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("No notebooks found for user/group: {}", userId);
                }
            }
            // Pagination handling is implemented in the Microsoft365Client methods
        } catch (final ApiException e) {
            if (e.getResponseStatusCode() == 404) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Notebooks not found (404) for user/group: {}", userId, e);
                }
            } else {
                logger.warn("Failed to retrieve notebooks for user/group: {}", userId, e);
            }
        }
    }

}

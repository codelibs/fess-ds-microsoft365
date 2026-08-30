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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.ListItem;
import com.microsoft.graph.models.Site;

/**
 * SharePointListDataStore crawls SharePoint lists and their items.
 *
 * @author shinsuke
 */
public class SharePointListDataStore extends Microsoft365DataStore {

    private static final Logger logger = LogManager.getLogger(SharePointListDataStore.class);

    // Configuration parameters
    /** The parameter name for the list ID. */
    protected static final String LIST_ID = "list_id";
    /** The parameter name for excluded list IDs. */
    protected static final String EXCLUDE_LIST_ID = "exclude_list_id";
    /** The parameter name for the list template filter. */
    protected static final String LIST_TEMPLATE_FILTER = "list_template_filter";

    // Field mappings for list items
    /** The field name for list item. */
    protected static final String LIST_ITEM = "item";
    /** The field name for list item title. */
    protected static final String LIST_ITEM_TITLE = "title";
    /** The field name for list item content. */
    protected static final String LIST_ITEM_CONTENT = "content";
    /** The field name for list item creation date. */
    protected static final String LIST_ITEM_CREATED = "created";
    /** The field name for list item modification date. */
    protected static final String LIST_ITEM_MODIFIED = "modified";
    /** The field name for list item ID. */
    protected static final String LIST_ITEM_ID = "id";
    /** The field name for list item URL. */
    protected static final String LIST_ITEM_URL = "url";
    /** The field name for list item web URL. */
    protected static final String LIST_ITEM_WEB_URL = "web_url";
    /** The field name for list item content type. */
    protected static final String LIST_ITEM_CONTENT_TYPE = "content_type";
    /** The field name for list item fields. */
    protected static final String LIST_ITEM_FIELDS = "fields";
    /** The field name for list item roles. */
    protected static final String LIST_ITEM_ROLES = "roles";

    // Field mappings for list metadata
    /** The field name for list name. */
    protected static final String LIST_NAME = "name";
    /** The field name for list description. */
    protected static final String LIST_DESCRIPTION = "description";
    /** The field name for list URL. */
    protected static final String LIST_URL = "url";
    /** The field name for list template type. */
    protected static final String LIST_TEMPLATE_TYPE = "template_type";
    /** The field name for list item count. */
    protected static final String LIST_ITEM_COUNT = "item_count";

    // Site field mappings
    /** The field name for site ID. */
    protected static final String SITE_ID_FIELD = "id";
    /** The field name for site name. */
    protected static final String SITE_NAME = "name";
    /** The field name for site URL. */
    protected static final String SITE_URL = "url";

    /**
     * Creates a new SharePointListDataStore instance.
     */
    public SharePointListDataStore() {
    }

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final Map<String, Object> configMap = new LinkedHashMap<>();
        configMap.put(IGNORE_ERROR, isIgnoreError(paramMap));

        // Validate list_template_filter exactly once per crawl. isTargetListType is evaluated
        // once per list, and (via isProcessableListItemType, see c01b81f) once per list item, so
        // warning from inside it would repeat once per list item processed.
        validateListTemplateFilter(paramMap);

        // Same reason, one step stronger: isTargetItem compiles include_pattern/exclude_pattern
        // once per list item, so a malformed one has to fail here or it fails tens of thousands
        // of times -- and a malformed exclude_pattern that merely warned would index every item
        // the operator asked to exclude.
        validatePatterns(paramMap);

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "SharePoint lists crawling started - Configuration: SiteID={}, ListID={}, IgnoreError={}, IgnoreSystemLists={}, Threads={}",
                    getSiteId(paramMap), getListId(paramMap), configMap.get(IGNORE_ERROR), isIgnoreSystemLists(paramMap),
                    paramMap.getAsString(NUMBER_OF_THREADS, "1"));
        }

        final ReportingExecutor executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            final String siteId = getSiteId(paramMap);
            if (StringUtil.isBlank(siteId)) {
                client.getSites(site -> {
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Processing site: {} (ID: {})", site.getDisplayName(), site.getId());
                        }
                        storeListBySite(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, executorService, client,
                                site.getId());
                    } catch (final Exception e) {
                        logger.warn("Failed to process site: {} (ID: {})", site.getDisplayName(), site.getId(), e);
                        if (!isIgnoreError(paramMap)) {
                            throw new DataStoreCrawlingException(site.getDisplayName(), "Failed to process site: " + site.getDisplayName(),
                                    e);
                        }
                    }
                });
            } else {
                storeListBySite(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, executorService, client, siteId);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Shutting down thread executor.");
            }
            shutdownExecutor(executorService, paramMap);
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Stores lists for a specific SharePoint site.
     *
     * @param dataConfig the data configuration
     * @param callback the index update callback
     * @param paramMap the data store parameters
     * @param scriptMap the script map
     * @param defaultDataMap the default data map
     * @param configMap the configuration map
     * @param executorService the executor service for parallel processing
     * @param client the Microsoft365 client
     * @param siteId the ID of the SharePoint site to process
     */
    protected void storeListBySite(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final ExecutorService executorService, final Microsoft365Client client, final String siteId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieving site information for site ID: {}", siteId);
        }

        final Site site = client.getSite(siteId);
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieved site: {} (ID: {}, WebUrl: {})", site.getDisplayName(), site.getId(), site.getWebUrl());
        }

        final String listId = getListId(paramMap);

        if (StringUtil.isNotBlank(listId)) {
            // Crawl specific list
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling specific list with ID: {} in site: {}", listId, site.getDisplayName());
            }

            final com.microsoft.graph.models.List list = client.getList(siteId, listId);
            if (logger.isDebugEnabled()) {
                logger.debug("Retrieved list: {} (ID: {}, Template: {}, IsSystem: {})", list.getDisplayName(), list.getId(),
                        list.getList() != null ? list.getList().getTemplate() : "unknown", isSystemList(list));
            }

            // Check ignore_system_lists setting even for specific list ID
            if (!isIgnoreSystemLists(paramMap) || !isSystemList(list)) {
                storeList(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client, site, list);
            } else if (logger.isDebugEnabled()) {
                logger.debug("Skipping system list {} (ID: {}) because ignore_system_lists is enabled", list.getDisplayName(),
                        list.getId());
            }
        } else {
            // Crawl all lists in the site
            if (logger.isDebugEnabled()) {
                logger.debug("Crawling all lists in site: {} with filtering", site.getDisplayName());
            }

            client.getSiteLists(siteId, list -> {

                final boolean excluded = isExcludedList(paramMap, list);
                final boolean targetType = isTargetListType(paramMap, list);
                final boolean systemList = isSystemList(list);
                final boolean ignoreSystem = isIgnoreSystemLists(paramMap);

                if (logger.isDebugEnabled()) {
                    logger.debug(
                            "Evaluating list: {} (ID: {}, Template: {}) - Excluded: {}, TargetType: {}, SystemList: {}, IgnoreSystem: {}",
                            list.getDisplayName(), list.getId(), list.getList() != null ? list.getList().getTemplate() : "unknown",
                            excluded, targetType, systemList, ignoreSystem);
                }

                if (!excluded && targetType && (!ignoreSystem || !systemList)) {
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Processing list: {} (ID: {}) in site: {}", list.getDisplayName(), list.getId(),
                                    site.getDisplayName());
                        }
                        storeList(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client, site,
                                list);
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully processed list: {} (ID: {})", list.getDisplayName(), list.getId());
                        }
                    } catch (final Exception e) {
                        logger.warn("Failed to process list: {} (ID: {}) in site: {}", list.getDisplayName(), list.getId(),
                                site.getDisplayName(), e);
                        if (!isIgnoreError(paramMap)) {
                            throw new DataStoreCrawlingException(site.getDisplayName(), "Failed to process list: " + list.getDisplayName(),
                                    e);
                        }
                    }
                } else if (logger.isDebugEnabled()) {
                    logger.debug("Skipped list: {} (ID: {}) - Excluded: {}, TargetType: {}, SystemList: {}", list.getDisplayName(),
                            list.getId(), excluded, targetType, systemList);
                }
            });
        }
    }

    /**
     * Stores a SharePoint list and its items.
     *
     * @param dataConfig the data configuration
     * @param callback the index update callback
     * @param configMap the configuration map
     * @param paramMap the data store parameters
     * @param scriptMap the script map
     * @param defaultDataMap the default data map
     * @param executorService the executor service for parallel processing
     * @param client the Microsoft365 client
     * @param site the SharePoint site
     * @param list the SharePoint list to store
     */
    protected void storeList(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Microsoft365Client client, final Site site,
            final com.microsoft.graph.models.List list) {
        if (logger.isDebugEnabled()) {
            logger.debug("Processing list: {} in site: {}", list.getDisplayName(), site.getDisplayName());
        }
        client.getListItems(site.getId(), list.getId(), item -> {
            if (isTargetItem(paramMap, item)) {
                executorService.execute(() -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing item ID: {} in list: {}", item.getId(), list.getDisplayName());
                    }
                    try {
                        processListItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, site, list, item);
                    } catch (final Exception e) {
                        logger.warn("Failed to process list item: {} in list: {}", item.getId(), list.getDisplayName(), e);
                        if (!isIgnoreError(paramMap)) {
                            throw new DataStoreCrawlingException(list.getDisplayName(), "Failed to process list item: " + item.getId(), e);
                        }
                    }
                });
            }
        });
    }

    /**
     * Processes a single list item.
     *
     * @param dataConfig the data configuration
     * @param callback the index update callback
     * @param configMap the configuration map
     * @param paramMap the data store parameters
     * @param scriptMap the script map
     * @param defaultDataMap the default data map
     * @param client the Microsoft365 client
     * @param site the SharePoint site
     * @param list the SharePoint list
     * @param item the list item to process
     */
    protected void processListItem(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final Site site, final com.microsoft.graph.models.List list, final ListItem item) {

        final String listTemplate;
        if (list.getList() == null || list.getList().getTemplate() == null) {
            logger.warn("List template type is unknown for list: {} (ID: {}) - skipping item ID: {}", list.getDisplayName(), list.getId(),
                    item.getId());
            return;
        }
        listTemplate = list.getList().getTemplate();

        if (!isProcessableListItemType(paramMap, listTemplate)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping list item whose template does not match {} - List: {} (ID: {}, Template: {}), Item ID: {}",
                        LIST_TEMPLATE_FILTER, list.getDisplayName(), list.getId(), listTemplate, item.getId());

            }
            return;
        }

        // Create URL for the item first for stats tracking
        final String listUrl = list.getWebUrl();
        final String itemUrl = item.getWebUrl();
        final String url;
        if (listUrl != null) {
            url = listUrl + "/DispForm.aspx?ID=" + item.getId();
        } else {
            url = itemUrl;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Processing list item - ID: {}, URL: {}, List: {} ({}), Site: {} ({}), Created: {}, Modified: {}", item.getId(),
                    url, list.getDisplayName(), list.getId(), site.getDisplayName(), site.getId(), item.getCreatedDateTime(),
                    item.getLastModifiedDateTime());
        }

        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);

        final StatsKeyObject statsKey = new StatsKeyObject(itemUrl);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);

        try {
            crawlerStatsHelper.begin(statsKey);

            logger.info("Crawling list item ID: {}, list ID: {}, site ID: {}, URL: {}, WebURL: {}", item.getId(), list.getId(),
                    site.getId(), url, itemUrl);

            final boolean ignoreError = (Boolean) configMap.get(IGNORE_ERROR);
            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> listItemMap = new HashMap<>();
            final Map<String, Object> listMap = new HashMap<>();
            final Map<String, Object> siteMap = new HashMap<>();

            // Add site-specific fields
            siteMap.put(SITE_ID_FIELD, site.getId());
            siteMap.put(SITE_NAME, site.getDisplayName());
            siteMap.put(SITE_URL, site.getWebUrl());

            listItemMap.put("site", siteMap);

            // Add list-specific fields
            listMap.put(LIST_NAME, list.getDisplayName());
            listMap.put(LIST_DESCRIPTION, list.getDescription() != null ? list.getDescription() : StringUtil.EMPTY);
            listMap.put(LIST_URL, listUrl);
            listMap.put(LIST_TEMPLATE_TYPE, listTemplate);

            listItemMap.put("list", listMap);

            // Add list item fields
            listItemMap.put(LIST_ITEM_ID, item.getId());
            listItemMap.put(LIST_ITEM_CREATED, item.getCreatedDateTime());
            listItemMap.put(LIST_ITEM_MODIFIED, item.getLastModifiedDateTime());
            listItemMap.put(LIST_ITEM_URL, url);
            listItemMap.put(LIST_ITEM_WEB_URL, itemUrl);
            listItemMap.put(LIST_ITEM_CONTENT_TYPE, item.getContentType() != null ? item.getContentType().getName() : StringUtil.EMPTY);

            if (logger.isDebugEnabled()) {
                logger.debug("Basic metadata prepared for item {} - Site: {}, List: {}", item.getId(), site.getDisplayName(),
                        list.getDisplayName());
            }

            // Get item fields (this is where SharePoint list data is stored)
            final com.microsoft.graph.models.FieldValueSet fieldValueSet = item.getFields();
            Map<String, Object> fields = fieldValueSet != null ? fieldValueSet.getAdditionalData() : null;

            if (logger.isDebugEnabled()) {
                logger.debug("Initial field extraction for item {} - FieldValueSet: {}, Fields count: {}", item.getId(),
                        fieldValueSet != null, fields != null ? fields.size() : 0);
            }

            // If fields are null or empty, try to fetch the item individually with expanded fields
            if (fields == null || fields.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Fields empty for item {} - attempting to refresh with expanded fields", item.getId());
                }
                try {
                    final ListItem refreshedItem = client.getListItem(site.getId(), list.getId(), item.getId(), true);
                    if (refreshedItem != null && refreshedItem.getFields() != null) {
                        fields = refreshedItem.getFields().getAdditionalData();
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully refreshed fields for item {} - Fields count: {}", item.getId(),
                                    fields != null ? fields.size() : 0);
                        }
                    }
                } catch (final Exception re) {
                    logger.warn("Failed to refresh list item fields for item {} in list {}: {}", item.getId(), list.getDisplayName(),
                            re.getMessage(), re);
                    if (!ignoreError) {
                        throw new DataStoreCrawlingException(list.getDisplayName(),
                                "Failed to refresh list item fields for item: " + item.getId(), re);
                    }
                }
            }

            if (fields != null) {
                listItemMap.put(LIST_ITEM_FIELDS, fields);

                if (logger.isDebugEnabled()) {
                    logger.debug("Fields available for item {} - Total fields: {}, Field names: {}", item.getId(), fields.size(),
                            fields.keySet().toString());
                }

                // Extract common fields
                final String title = extractFieldValue(fields, "Title", "LinkTitle", "FileLeafRef");
                if (StringUtil.isNotBlank(title)) {
                    listItemMap.put(LIST_ITEM_TITLE, title);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Extracted title for item {}: {}", item.getId(), title);
                    }
                }

                // Try to extract content from various content fields
                final String content = extractFieldValue(fields, "Body", "Description", "Comments", "Notes");
                if (StringUtil.isNotBlank(content)) {
                    listItemMap.put(LIST_ITEM_CONTENT, content);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Extracted content for item {} - Content length: {}", item.getId(), content.length());
                    }
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("No fields available for item {} after refresh attempts", item.getId());
            }

            // Graph has no user/group role-assignment endpoint for a site that this plugin can
            // call without Sites.FullControl.All (see Microsoft365Client's removed
            // getSitePermissions), so list items carry no site-derived roles. default_permissions
            // below is their only role source.
            final List<String> roles = new ArrayList<>();

            roles.addAll(getDefaultPermissions(paramMap));

            final List<String> finalPermissions = mergeDefaultRoles(roles, defaultDataMap).stream().distinct().collect(Collectors.toList());
            if (logger.isDebugEnabled()) {
                logger.debug("Final permissions for item {} - Count: {}, Permissions: {}", item.getId(), finalPermissions.size(),
                        finalPermissions);
            }
            listItemMap.put(LIST_ITEM_ROLES, finalPermissions);

            resultMap.put(LIST_ITEM, listItemMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("List item map prepared for processing - Item: {}, Fields: {}, Permissions: {}, URL: {}", item.getId(),
                        listItemMap.size(), finalPermissions.size(), url);
            }

            // Apply script processing for field mapping
            final String scriptType = getScriptType(paramMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

            if (logger.isDebugEnabled()) {
                logger.debug("Data map prepared for storage - DataMap: {}", dataMap);
            }

            if (dataMap.get("url") instanceof final String statsUrl) {
                statsKey.setUrl(statsUrl);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully indexed list item: {} (ID: {}, List: {})", itemUrl, item.getId(), list.getDisplayName());
            }

        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception for list item: {} (ID: {}) in list: {} - Data: {}", itemUrl, item.getId(),
                    list.getDisplayName(), dataMap, e);
            handleCrawlingException(dataConfig, crawlerStatsHelper, statsKey, itemUrl, e);
        } catch (final Throwable t) {
            logger.warn("Processing exception for list item: {} (ID: {}) in list: {} - Data: {}", itemUrl, item.getId(),
                    list.getDisplayName(), dataMap, t);
            handleCrawlingThrowable(dataConfig, crawlerStatsHelper, statsKey, itemUrl, t);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Extract field value from SharePoint list item fields.
     * Tries multiple field names in order and returns the first non-empty value.
     *
     * @param fields the map of field values
     * @param fieldNames the field names to extract (in order of preference)
     * @return the extracted field value or null if not found
     */
    protected String extractFieldValue(final Map<String, Object> fields, final String... fieldNames) {
        if (fields == null || fieldNames == null) {
            return null;
        }

        for (final String fieldName : fieldNames) {
            final Object value = fields.get(fieldName);
            if (value != null) {
                final String stringValue = value.toString().trim();
                if (StringUtil.isNotBlank(stringValue)) {
                    return stringValue;
                }
            }
        }
        return null;
    }

    // Configuration helper methods
    /**
     * Gets the site ID from the parameter map.
     *
     * @param paramMap the data store parameters
     * @return the site ID or null if not specified
     */
    protected String getSiteId(final DataStoreParams paramMap) {
        return paramMap.getAsString(SITE_ID, null);
    }

    /**
     * Gets the list ID from the parameter map.
     *
     * @param paramMap the data store parameters
     * @return the list ID or null if not specified
     */
    protected String getListId(final DataStoreParams paramMap) {
        return paramMap.getAsString(LIST_ID, null);
    }

    /**
     * Checks if the list should be excluded from crawling.
     *
     * @param paramMap the data store parameters
     * @param list the SharePoint list to check
     * @return true if the list should be excluded, false otherwise
     */
    protected boolean isExcludedList(final DataStoreParams paramMap, final com.microsoft.graph.models.List list) {
        final String excludeIds = paramMap.getAsString(EXCLUDE_LIST_ID, null);
        if (StringUtil.isBlank(excludeIds)) {
            return false;
        }
        final String[] ids = excludeIds.split(",");
        for (final String id : ids) {
            if (list.getId().equals(id.trim())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if the list matches the target template type filter.
     *
     * @param paramMap the data store parameters
     * @param list the SharePoint list to check
     * @return true if the list matches the template filter, false otherwise
     */
    protected boolean isTargetListType(final DataStoreParams paramMap, final com.microsoft.graph.models.List list) {
        final String listTemplate = list.getList() != null ? list.getList().getTemplate() : null;
        return isTargetListType(paramMap, listTemplate);
    }

    /**
     * Checks if the given list template name matches the target template type filter.
     *
     * @param paramMap the data store parameters
     * @param listTemplate the Graph template name of the list, or {@code null} if unknown
     * @return true if the template matches the template filter, false otherwise
     */
    protected boolean isTargetListType(final DataStoreParams paramMap, final String listTemplate) {
        final String templateFilter = paramMap.getAsString(LIST_TEMPLATE_FILTER, null);
        if (StringUtil.isBlank(templateFilter)) {
            return true;
        }

        if (listTemplate != null) {
            final String[] templates = templateFilter.split(",");
            for (final String t : templates) {
                final String candidate = t.trim();
                if (candidate.isEmpty()) {
                    // e.g. "100,,101" - nothing to look up, and nothing a list template could
                    // ever equal.
                    continue;
                }
                final String mapped = Microsoft365Constants.templateNameForId(candidate);
                if (listTemplate.equals(mapped != null ? mapped : candidate)) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    /**
     * Validates {@link #LIST_TEMPLATE_FILTER} once per crawl and warns about any token that
     * looks like a numeric template ID but has no documented mapping.
     *
     * <p>{@link #isTargetListType(DataStoreParams, String)} is evaluated once per list while
     * enumerating a site's lists, and (via {@link #isProcessableListItemType}, see c01b81f) once
     * per list item while processing a list. Warning from inside that method would therefore
     * repeat once per list, or once per list item processed, for the same misconfigured filter.
     * This method performs the same lookup exactly once, when the crawl starts, instead.</p>
     *
     * @param paramMap the data store parameters
     */
    protected void validateListTemplateFilter(final DataStoreParams paramMap) {
        final String templateFilter = paramMap.getAsString(LIST_TEMPLATE_FILTER, null);
        if (StringUtil.isBlank(templateFilter)) {
            return;
        }

        for (final String t : templateFilter.split(",")) {
            final String candidate = t.trim();
            if (candidate.isEmpty()) {
                // A blank entry (e.g. "100,,101") is not an unknown numeric ID - it is nothing
                // at all - so it must not be reported as one.
                continue;
            }
            if (Microsoft365Constants.templateNameForId(candidate) == null && candidate.chars().allMatch(Character::isDigit)) {
                logger.warn("Unknown list template ID '{}' in {}; use the Graph template name instead.", candidate, LIST_TEMPLATE_FILTER);
            }
        }
    }

    /**
     * Decides whether items of the given list template should be processed.
     *
     * <p>Without an explicit {@code list_template_filter} this keeps the historical
     * behaviour of handling generic lists only. Setting the filter used to have no effect
     * here: items of any other template were dropped further down regardless of it.</p>
     *
     * @param paramMap the data store parameters
     * @param listTemplate the Graph template name of the list owning the item
     * @return true if items of this template should be processed
     */
    protected boolean isProcessableListItemType(final DataStoreParams paramMap, final String listTemplate) {
        final String templateFilter = paramMap.getAsString(LIST_TEMPLATE_FILTER, null);
        if (StringUtil.isBlank(templateFilter)) {
            return Microsoft365Constants.GENERIC_LIST.equals(listTemplate);
        }
        return isTargetListType(paramMap, listTemplate);
    }

    /**
     * Checks if the list item should be crawled based on include/exclude patterns.
     *
     * @param paramMap the data store parameters
     * @param item the list item to check
     * @return true if the item should be crawled, false otherwise
     */
    protected boolean isTargetItem(final DataStoreParams paramMap, final ListItem item) {
        if (logger.isDebugEnabled()) {
            logger.debug("Checking if list item is target - FieldValueSet: {}, Fields: {}", item.getFields() != null,
                    item.getFields() != null ? item.getFields().getAdditionalData().size() : 0);
        }
        if (item.getFields() != null) {
            final com.microsoft.graph.models.FieldValueSet fieldValueSet = item.getFields();
            final Map<String, Object> fields = fieldValueSet != null ? fieldValueSet.getAdditionalData() : null;
            final String title = extractFieldValue(fields, "Title", "LinkTitle", "FileLeafRef");
            if (StringUtil.isNotBlank(title)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("List item title for filtering: {}", title);
                }
                final Pattern includePattern = getPattern(paramMap, INCLUDE_PATTERN);
                if (includePattern != null && !includePattern.matcher(title).matches()) {
                    return false;
                }

                final Pattern excludePattern = getPattern(paramMap, EXCLUDE_PATTERN);
                if (excludePattern != null && excludePattern.matcher(title).matches()) {
                    return false;
                }
            }
        }

        return true;
    }
}
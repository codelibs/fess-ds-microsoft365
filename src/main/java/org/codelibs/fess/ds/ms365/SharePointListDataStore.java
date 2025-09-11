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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
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
    /** The parameter name for the site ID. */
    protected static final String SITE_ID = "site_id";
    /** The parameter name for the list ID. */
    protected static final String LIST_ID = "list_id";
    /** The parameter name for excluded list IDs. */
    protected static final String EXCLUDE_LIST_ID = "exclude_list_id";
    /** The parameter name for the list template filter. */
    protected static final String LIST_TEMPLATE_FILTER = "list_template_filter";
    /** The parameter name for the number of threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** The parameter name for default permissions. */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** The parameter name for ignoring system lists. */
    protected static final String IGNORE_SYSTEM_LISTS = "ignore_system_lists";
    /** The parameter name for ignoring errors. */
    protected static final String IGNORE_ERROR = "ignore_error";
    /** The parameter name for the include pattern. */
    protected static final String INCLUDE_PATTERN = "include_pattern";
    /** The parameter name for the exclude pattern. */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";

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
    /** The field name for list item fields. */
    protected static final String LIST_ITEM_FIELDS = "fields";
    /** The field name for list item attachments. */
    protected static final String LIST_ITEM_ATTACHMENTS = "attachments";
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

    // Configuration constants
    /** The parameter name for URL filter. */
    protected static final String URL_FILTER = "url_filter";

    /** The name of the extractor for SharePoint lists. */
    protected String extractorName = "sharePointListExtractor";

    /**
     * Creates a new SharePointListDataStore instance.
     */
    public SharePointListDataStore() {
        super();
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
        configMap.put(URL_FILTER, getUrlFilter(paramMap));

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "SharePoint lists crawling started - Configuration: SiteID={}, ListID={}, IgnoreError={}, IgnoreSystemLists={}, Threads={}",
                    getSiteId(paramMap), getListId(paramMap), configMap.get(IGNORE_ERROR), isIgnoreSystemLists(paramMap),
                    paramMap.getAsString(NUMBER_OF_THREADS, "1"));
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            final String siteId = getSiteId(paramMap);
            if (StringUtil.isBlank(siteId)) {
                logger.error("site_id parameter is required for SharePoint list crawling - operation aborted");
                return;
            }

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
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Skipping system list {} (ID: {}) because ignore_system_lists is enabled", list.getDisplayName(),
                                list.getId());
                    }
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
                                throw new DataStoreCrawlingException(site.getDisplayName(),
                                        "Failed to process list: " + list.getDisplayName(), e);
                            }
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Skipped list: {} (ID: {}) - Excluded: {}, TargetType: {}, SystemList: {}", list.getDisplayName(),
                                    list.getId(), excluded, targetType, systemList);
                        }
                    }
                });
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Shutting down thread executor.");
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
        executorService.execute(() -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing list: {} in site: {}", list.getDisplayName(), site.getDisplayName());
            }
            client.getListItems(site.getId(), list.getId(), item -> {
                if (isTargetItem(paramMap, item)) {
                    try {
                        processListItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, site, list, item);
                    } catch (final Exception e) {
                        logger.warn("Failed to process list item: {} in list: {}", item.getId(), list.getDisplayName(), e);
                        if (!isIgnoreError(paramMap)) {
                            throw new DataStoreCrawlingException(list.getDisplayName(), "Failed to process list item: " + item.getId(), e);
                        }
                    }
                }
            });
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

        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);

        // Create URL for the item first for stats tracking
        String itemUrl = item.getWebUrl();
        if (StringUtil.isBlank(itemUrl) && list.getWebUrl() != null) {
            itemUrl = list.getWebUrl() + "/DispForm.aspx?ID=" + item.getId();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Processing list item - ID: {}, URL: {}, List: {} ({}), Site: {} ({}), Created: {}, Modified: {}", item.getId(),
                    itemUrl, list.getDisplayName(), list.getId(), site.getDisplayName(), site.getId(), item.getCreatedDateTime(),
                    item.getLastModifiedDateTime());
        }

        final StatsKeyObject statsKey = new StatsKeyObject(itemUrl);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);

        try {
            crawlerStatsHelper.begin(statsKey);

            // Apply URL filter if configured
            final String url = itemUrl;
            final UrlFilter urlFilter = (UrlFilter) configMap.get(URL_FILTER);
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("URL filter rejected item: {} - Original URL: {}", url, item.getWebUrl());
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            logger.info("Crawling list item URL: {} (ID: {}, List: {})", url, item.getId(), list.getDisplayName());

            final boolean ignoreError = ((Boolean) configMap.get(IGNORE_ERROR));
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
            listMap.put(LIST_URL, list.getWebUrl());
            if (list.getList() != null && list.getList().getTemplate() != null) {
                listMap.put(LIST_TEMPLATE_TYPE, list.getList().getTemplate());
            }

            listItemMap.put("list", listMap);

            // Add list item fields
            listItemMap.put(LIST_ITEM_ID, item.getId());
            listItemMap.put(LIST_ITEM_CREATED, item.getCreatedDateTime());
            listItemMap.put(LIST_ITEM_MODIFIED, item.getLastModifiedDateTime());
            listItemMap.put(LIST_ITEM_URL, url);

            if (logger.isDebugEnabled()) {
                logger.debug("Basic metadata prepared for item {} - Site: {}, List: {}", item.getId(), site.getDisplayName(),
                        list.getDisplayName());
            }

            // Get item fields (this is where SharePoint list data is stored)
            com.microsoft.graph.models.FieldValueSet fieldValueSet = item.getFields();
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
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("No fields available for item {} after refresh attempts", item.getId());
                }
            }

            // Handle permissions properly
            final List<String> roles = getSitePermissions(client, site.getId());
            if (logger.isDebugEnabled()) {
                logger.debug("Initial permissions for site {} - Count: {}, Permissions: {}", site.getDisplayName(), roles.size(), roles);
            }

            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(roles::add));
            if (defaultDataMap.get(fessConfig.getIndexFieldRole()) instanceof List<?> roleTypeList) {
                roleTypeList.stream().map(s -> (String) s).forEach(roles::add);
            }

            final List<String> finalPermissions = roles.stream().distinct().collect(Collectors.toList());
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
            failureUrlService.store(dataConfig, errorName, itemUrl, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Processing exception for list item: {} (ID: {}) in list: {} - Data: {}", itemUrl, item.getId(),
                    list.getDisplayName(), dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), itemUrl, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
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

    /**
     * Build content string from all fields for indexing.
     *
     * @param fields the map of field values
     * @return the concatenated content string
     */
    protected String buildContentFromFields(final Map<String, Object> fields) {
        if (fields == null || fields.isEmpty()) {
            return "";
        }

        final StringBuilder content = new StringBuilder();
        for (final Map.Entry<String, Object> entry : fields.entrySet()) {
            if (entry.getValue() != null && !isSystemField(entry.getKey())) {
                final String value = entry.getValue().toString().trim();
                if (StringUtil.isNotBlank(value) && !value.equals("null")) {
                    if (content.length() > 0) {
                        content.append(" ");
                    }
                    content.append(value);
                }
            }
        }
        return content.toString();
    }

    /**
     * Check if a field is a system field that should not be included in content.
     *
     * @param fieldName the name of the field to check
     * @return true if the field is a system field, false otherwise
     */
    protected boolean isSystemField(final String fieldName) {
        if (StringUtil.isBlank(fieldName)) {
            return true;
        }
        final String lowerField = fieldName.toLowerCase();
        return lowerField.startsWith("_") || lowerField.startsWith("ows") || lowerField.equals("id") || lowerField.equals("contenttype")
                || lowerField.equals("version") || lowerField.equals("attachments");
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
        final String templateFilter = paramMap.getAsString(LIST_TEMPLATE_FILTER, null);
        if (StringUtil.isBlank(templateFilter)) {
            return true;
        }

        if (list.getList() != null && list.getList().getTemplate() != null) {
            final String template = list.getList().getTemplate();
            final String[] templates = templateFilter.split(",");
            for (final String t : templates) {
                if (template.equals(t.trim())) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    /**
     * Checks if the list is a system list.
     *
     * @param list the SharePoint list to check
     * @return true if the list is a system list, false otherwise
     */
    protected boolean isSystemList(final com.microsoft.graph.models.List list) {
        if (logger.isDebugEnabled()) {
            logger.debug("Checking if list is system list - Name: {}, ID: {}, Template: {}, WebUrl: {}", list.getDisplayName(),
                    list.getId(), list.getList() != null ? list.getList().getTemplate() : "unknown", list.getWebUrl());
        }

        // Use URL-based detection for better reliability when available
        if (list.getWebUrl() != null) {
            final String url = list.getWebUrl().toLowerCase();
            return url.contains("/_catalogs/") || url.contains("/lists/userinformationlist") || url.contains("/lists/workflowtasks")
                    || url.contains("/lists/accessrequests") || url.contains("/sitepages/") || url.contains("/siteassets/")
                    || url.contains("/lists/masterpage") || url.contains("/lists/stylelibrary") || url.contains("/lists/formtemplates")
                    || url.contains("/_layouts/") || url.contains("/workflowhistory") || url.contains("/_private/");
        }

        // Fallback to name-based detection when URL is not available
        if (list.getDisplayName() == null) {
            return false;
        }
        final String name = list.getDisplayName().toLowerCase();
        return name.contains("master page") || name.contains("style library") || name.contains("_catalogs") || name.contains("workflow")
                || name.contains("user information") || name.contains("access requests") || name.startsWith("_")
                || name.contains("form templates");
    }

    /**
     * Checks if the list item should be crawled based on include/exclude patterns.
     *
     * @param paramMap the data store parameters
     * @param item the list item to check
     * @return true if the item should be crawled, false otherwise
     */
    protected boolean isTargetItem(final DataStoreParams paramMap, final ListItem item) {
        // Apply include/exclude patterns if configured
        final String includePattern = paramMap.getAsString(INCLUDE_PATTERN, null);
        final String excludePattern = paramMap.getAsString(EXCLUDE_PATTERN, null);

        if (item.getFields() != null) {
            final com.microsoft.graph.models.FieldValueSet fieldValueSet = item.getFields();
            final Map<String, Object> fields = fieldValueSet != null ? fieldValueSet.getAdditionalData() : null;
            final String title = extractFieldValue(fields, "Title", "LinkTitle", "FileLeafRef");
            if (StringUtil.isNotBlank(title)) {
                if (StringUtil.isNotBlank(includePattern)) {
                    final Pattern pattern = Pattern.compile(includePattern);
                    if (!pattern.matcher(title).matches()) {
                        return false;
                    }
                }

                if (StringUtil.isNotBlank(excludePattern)) {
                    final Pattern pattern = Pattern.compile(excludePattern);
                    if (pattern.matcher(title).matches()) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    /**
     * Checks if errors should be ignored during crawling.
     *
     * @param paramMap the data store parameters
     * @return true if errors should be ignored, false otherwise
     */
    protected boolean isIgnoreError(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.FALSE));
    }

    /**
     * Checks if system lists should be ignored during crawling.
     *
     * @param paramMap the data store parameters
     * @return true if system lists should be ignored, false otherwise
     */
    protected boolean isIgnoreSystemLists(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_SYSTEM_LISTS, Constants.TRUE));
    }

    /**
     * Gets the URL filter for crawling.
     *
     * @param paramMap the data store parameters
     * @return the configured URL filter
     */
    protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
        final UrlFilter urlFilter = ComponentUtil.getComponent(UrlFilter.class);
        final String include = paramMap.getAsString(INCLUDE_PATTERN);
        if (StringUtil.isNotBlank(include)) {
            urlFilter.addInclude(include);
        }
        final String exclude = paramMap.getAsString(EXCLUDE_PATTERN);
        if (StringUtil.isNotBlank(exclude)) {
            urlFilter.addExclude(exclude);
        }
        urlFilter.init(paramMap.getAsString(Constants.CRAWLING_INFO_ID));
        if (logger.isDebugEnabled()) {
            logger.debug("urlFilter: {}", urlFilter);
        }
        return urlFilter;
    }

    /**
     * Sets the extractor name for SharePoint lists.
     *
     * @param extractorName the extractor name to set
     */
    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }
}
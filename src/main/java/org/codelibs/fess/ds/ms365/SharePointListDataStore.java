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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
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
    protected static final String SITE_ID = "site_id";
    protected static final String LIST_ID = "list_id";
    protected static final String EXCLUDE_LIST_ID = "exclude_list_id";
    protected static final String LIST_TEMPLATE_FILTER = "list_template_filter";
    protected static final String INCLUDE_ATTACHMENTS = "include_attachments";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    protected static final String IGNORE_SYSTEM_LISTS = "ignore_system_lists";
    protected static final String IGNORE_ERROR = "ignore_error";
    protected static final String INCLUDE_PATTERN = "include_pattern";
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";

    // Field mappings for list items
    protected static final String LIST_ITEM = "list_item";
    protected static final String LIST_ITEM_TITLE = "list_item_title";
    protected static final String LIST_ITEM_CONTENT = "list_item_content";
    protected static final String LIST_ITEM_CREATED = "list_item_created";
    protected static final String LIST_ITEM_MODIFIED = "list_item_modified";
    protected static final String LIST_ITEM_ID = "list_item_id";
    protected static final String LIST_ITEM_URL = "list_item_url";
    protected static final String LIST_ITEM_FIELDS = "list_item_fields";
    protected static final String LIST_ITEM_ATTACHMENTS = "list_item_attachments";
    protected static final String LIST_ITEM_ROLES = "list_item_roles";

    // Field mappings for list metadata
    protected static final String LIST_NAME = "list_name";
    protected static final String LIST_DESCRIPTION = "list_description";
    protected static final String LIST_URL = "list_url";
    protected static final String LIST_TEMPLATE_TYPE = "list_template_type";
    protected static final String LIST_ITEM_COUNT = "list_item_count";

    // Site field mappings
    protected static final String SITE_ID_FIELD = "site_id";
    protected static final String SITE_NAME = "site_name";
    protected static final String SITE_URL = "site_url";

    protected String extractorName = "sharePointListExtractor";

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
        if (logger.isDebugEnabled()) {
            logger.debug("configMap: {}", configMap);
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            final String siteId = getSiteId(paramMap);
            if (StringUtil.isBlank(siteId)) {
                logger.warn("site_id parameter is required for SharePoint list crawling");
                return;
            }

            final Site site = client.getSite(siteId);
            final String listId = getListId(paramMap);

            if (StringUtil.isNotBlank(listId)) {
                // Crawl specific list
                final com.microsoft.graph.models.List list = client.getList(siteId, listId);
                // Check ignore_system_lists setting even for specific list ID
                if (!isIgnoreSystemLists(paramMap) || !isSystemList(list)) {
                    storeList(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client, site, list);
                } else {
                    logger.info("Skipping system list {} (ID: {}) because ignore_system_lists is enabled", list.getDisplayName(),
                            list.getId());
                }
            } else {
                // Crawl all lists in the site
                final List<Future<?>> listProcessingFutures = new java.util.concurrent.CopyOnWriteArrayList<>();
                client.getSiteLists(siteId, list -> {
                    if (!isExcludedList(paramMap, list) && isTargetListType(paramMap, list)
                            && (!isIgnoreSystemLists(paramMap) || !isSystemList(list))) {
                        listProcessingFutures.add(executorService.submit(() -> {
                            try {
                                storeList(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client,
                                        site, list);
                            } catch (final Exception e) {
                                logger.warn("Failed to process list: {} in site: {}", list.getDisplayName(), site.getDisplayName(), e);
                                if (!isIgnoreError(paramMap)) {
                                    throw new DataStoreCrawlingException(site.getDisplayName(),
                                            "Failed to process list: " + list.getDisplayName(), e);
                                }
                            }
                        }));
                    }
                });

                // Wait for all list processing tasks to complete
                for (final Future<?> future : listProcessingFutures) {
                    try {
                        future.get();
                    } catch (final Exception e) {
                        logger.warn("A list processing task for site {} was interrupted/failed.", site.getDisplayName(), e);
                        if (!isIgnoreError(paramMap)) {
                            throw new DataStoreCrawlingException(site.getDisplayName(),
                                    "A list processing task failed for site: " + site.getDisplayName(), e);
                        }
                    }
                }
            }
        } finally {
            executorService.shutdown();
            try {
                // Wait for all tasks to complete
                if (!executorService.awaitTermination(30, TimeUnit.MINUTES)) {
                    logger.warn("Executor did not terminate in the specified time. Forcing shutdownNow()");
                    executorService.shutdownNow();
                }
            } catch (final InterruptedException ie) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            try {
                // Commit remaining documents in buffer
                callback.commit();
            } catch (final Exception e) {
                logger.warn("Failed to commit index update callback.", e);
            }
        }
    }

    protected Microsoft365Client createClient(final DataStoreParams paramMap) {
        return new Microsoft365Client(paramMap);
    }

    protected void storeList(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Microsoft365Client client, final Site site,
            final com.microsoft.graph.models.List list) {

        if (logger.isDebugEnabled()) {
            logger.debug("Processing list: {} in site: {}", list.getDisplayName(), site.getDisplayName());
        }

        try {
            // Get list items and process them
            final List<Future<?>> futures = new CopyOnWriteArrayList<>();
            client.getListItems(site.getId(), list.getId(), item -> {
                if (isTargetItem(paramMap, item)) {
                    futures.add(executorService.submit(() -> {
                        try {
                            processListItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, site, list, item);
                        } catch (final Exception e) {
                            logger.warn("Failed to process list item: {} in list: {}", item.getId(), list.getDisplayName(), e);
                            if (!isIgnoreError(paramMap)) {
                                throw new DataStoreCrawlingException(list.getDisplayName(), "Failed to process list item: " + item.getId(),
                                        e);
                            }
                        }
                    }));
                }
            });

            // Wait for all item processing tasks to complete
            for (final Future<?> future : futures) {
                try {
                    future.get();
                } catch (final Exception e) {
                    logger.warn("A task for list {} was interrupted/failed.", list.getDisplayName(), e);
                    if (!isIgnoreError(paramMap)) {
                        throw new DataStoreCrawlingException(list.getDisplayName(), "A task failed for list: " + list.getDisplayName(), e);
                    }
                }
            }
        } catch (final Exception e) {
            logger.warn("Failed to get list items for list: {} in site: {}", list.getDisplayName(), site.getDisplayName(), e);
            if (!isIgnoreError(paramMap)) {
                throw new DataStoreCrawlingException(site.getDisplayName(), "Failed to get list items for list: " + list.getDisplayName(),
                        e);
            }
        }
    }

    protected void processListItem(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final Site site, final com.microsoft.graph.models.List list, final ListItem item) {

        final Map<String, Object> dataMap = new LinkedHashMap<>(defaultDataMap);

        try {
            // Add site-specific fields
            dataMap.put(SITE_ID_FIELD, site.getId());
            dataMap.put(SITE_NAME, site.getDisplayName());
            dataMap.put(SITE_URL, site.getWebUrl());

            // Add list-specific fields
            dataMap.put(LIST_NAME, list.getDisplayName());
            dataMap.put(LIST_DESCRIPTION, list.getDescription());
            dataMap.put(LIST_URL, list.getWebUrl());
            if (list.getList() != null && list.getList().getTemplate() != null) {
                dataMap.put(LIST_TEMPLATE_TYPE, list.getList().getTemplate());
            }

            // Add list item fields
            dataMap.put(LIST_ITEM_ID, item.getId());
            dataMap.put(LIST_ITEM_CREATED, item.getCreatedDateTime());
            dataMap.put(LIST_ITEM_MODIFIED, item.getLastModifiedDateTime());

            // Get item fields (this is where SharePoint list data is stored)
            final com.microsoft.graph.models.FieldValueSet fieldValueSet = item.getFields();
            Map<String, Object> fields = fieldValueSet != null ? fieldValueSet.getAdditionalData() : null;

            // If fields are null or empty, try to fetch the item individually with expanded fields
            if (fields == null || fields.isEmpty()) {
                try {
                    final ListItem refreshedItem = client.getListItem(site.getId(), list.getId(), item.getId(), true);
                    if (refreshedItem != null && refreshedItem.getFields() != null) {
                        fields = refreshedItem.getFields().getAdditionalData();
                    }
                } catch (final Exception re) {
                    logger.debug("Failed to refresh list item fields for item {}: {}", item.getId(), re.getMessage());
                }
            }

            if (fields != null) {
                dataMap.put(LIST_ITEM_FIELDS, fields);

                // Extract common fields
                final String title = extractFieldValue(fields, "Title", "LinkTitle", "FileLeafRef");
                if (StringUtil.isNotBlank(title)) {
                    dataMap.put(LIST_ITEM_TITLE, title);
                }

                // Try to extract content from various content fields
                final String content = extractFieldValue(fields, "Body", "Description", "Comments", "Notes");
                if (StringUtil.isNotBlank(content)) {
                    dataMap.put(LIST_ITEM_CONTENT, content);
                }
            }

            // Create URL for the item
            String itemUrl = item.getWebUrl();
            if (StringUtil.isBlank(itemUrl) && list.getWebUrl() != null) {
                itemUrl = list.getWebUrl() + "/DispForm.aspx?ID=" + item.getId();
            }

            // Set roles/permissions (simplified)
            final List<String> roles = Collections.emptyList();
            dataMap.put(LIST_ITEM_ROLES, roles);

            // Set standard Fess fields
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldUrl(), itemUrl);
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldTitle(),
                    StringUtil.isNotBlank((String) dataMap.get(LIST_ITEM_TITLE)) ? dataMap.get(LIST_ITEM_TITLE)
                            : list.getDisplayName() + " - Item " + item.getId());
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldContent(),
                    StringUtil.isNotBlank((String) dataMap.get(LIST_ITEM_CONTENT)) ? dataMap.get(LIST_ITEM_CONTENT)
                            : buildContentFromFields(fields));
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldLastModified(), item.getLastModifiedDateTime());
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldMimetype(), "text/html");

            callback.store(paramMap, dataMap);

        } catch (final Exception e) {
            logger.warn("Failed to process list item: {} in list: {}", item.getId(), list.getDisplayName(), e);
            if (!isIgnoreError(paramMap)) {
                throw new DataStoreCrawlingException(list.getDisplayName(), "Failed to process list item: " + item.getId(), e);
            }
        }
    }

    /**
     * Extract field value from SharePoint list item fields.
     * Tries multiple field names in order and returns the first non-empty value.
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
    protected String getSiteId(final DataStoreParams paramMap) {
        return paramMap.getAsString(SITE_ID, null);
    }

    protected String getListId(final DataStoreParams paramMap) {
        return paramMap.getAsString(LIST_ID, null);
    }

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

    protected boolean isSystemList(final com.microsoft.graph.models.List list) {
        // Check for system facet according to Microsoft Graph API documentation
        // https://learn.microsoft.com/en-us/graph/api/resources/systemfacet?view=graph-rest-1.0
        if (list.getSystem() != null) {
            return true;
        }

        // Fallback to name-based detection for compatibility
        if (list.getDisplayName() == null) {
            return false;
        }
        final String name = list.getDisplayName().toLowerCase();
        return name.contains("master page") || name.contains("style library") || name.contains("_catalogs") || name.contains("workflow")
                || name.contains("user information") || name.contains("access requests") || name.startsWith("_")
                || name.contains("form templates");
    }

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

    protected boolean isIgnoreError(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.FALSE));
    }

    protected boolean isIgnoreSystemLists(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_SYSTEM_LISTS, Constants.TRUE));
    }

    protected boolean isIncludeAttachments(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(INCLUDE_ATTACHMENTS, Constants.FALSE));
    }

    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }
}
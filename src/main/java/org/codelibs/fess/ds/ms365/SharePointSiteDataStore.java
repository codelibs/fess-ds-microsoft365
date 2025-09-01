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

import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.Site;

/**
 * SharePointSiteDataStore crawls SharePoint sites and their document libraries.
 *
 * @author shinsuke
 */
public class SharePointSiteDataStore extends Microsoft365DataStore {

    private static final Logger logger = LogManager.getLogger(SharePointSiteDataStore.class);

    // Configuration parameters
    protected static final String SITE_ID = "site_id";
    protected static final String EXCLUDE_SITE_ID = "exclude_site_id";
    protected static final String SITE_TYPE_FILTER = "site_type_filter";
    protected static final String INCLUDE_SUBSITES = "include_subsites";
    protected static final String IGNORE_SYSTEM_LIBRARIES = "ignore_system_libraries";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    protected static final String MAX_CONTENT_LENGTH = "max_content_length";
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    protected static final String INCLUDE_PATTERN = "include_pattern";
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    protected static final String URL_FILTER = "url_filter";
    protected static final String IGNORE_FOLDER = "ignore_folder";
    protected static final String IGNORE_ERROR = "ignore_error";

    // Field mappings
    protected static final String SITE = "site";
    protected static final String SITE_NAME = "site_name";
    protected static final String SITE_DESCRIPTION = "site_description";
    protected static final String SITE_URL = "site_url";
    protected static final String SITE_CREATED = "site_created";
    protected static final String SITE_MODIFIED = "site_modified";
    protected static final String SITE_TYPE = "site_type";
    protected static final String SITE_ROLES = "site_roles";
    protected static final String SITE_ID_FIELD = "site_id";

    // File field mappings (reuse OneDrive patterns)
    protected static final String FILE = "file";
    protected static final String FILE_NAME = "file_name";
    protected static final String FILE_DESCRIPTION = "file_description";
    protected static final String FILE_CONTENTS = "file_contents";
    protected static final String FILE_MIMETYPE = "file_mimetype";
    protected static final String FILE_FILETYPE = "file_filetype";
    protected static final String FILE_CREATED = "file_created";
    protected static final String FILE_LAST_MODIFIED = "file_last_modified";
    protected static final String FILE_SIZE = "file_size";
    protected static final String FILE_WEB_URL = "file_web_url";
    protected static final String FILE_URL = "file_url";
    protected static final String FILE_ROLES = "file_roles";
    protected static final String FILE_ID = "file_id";
    protected static final String FILE_PARENT_PATH = "file_parent_path";

    protected String extractorName = "sharePointSiteExtractor";

    public SharePointSiteDataStore() {
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
            if (StringUtil.isNotBlank(siteId)) {
                // Crawl specific site, but check if it should be excluded
                final Site site = client.getSite(siteId);
                if (!isExcludedSite(paramMap, site)) {
                    storeSite(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client, site);
                } else {
                    if (logger.isInfoEnabled()) {
                        logger.info("Skipping excluded site: {} ({})", site.getDisplayName(), site.getId());
                    }
                }
            } else {
                // Crawl all sites using parallel processing
                final List<Future<?>> siteProcessingFutures = new java.util.concurrent.CopyOnWriteArrayList<>();
                client.getSites(site -> {
                    if (!isExcludedSite(paramMap, site) && isTargetSiteType(paramMap, site)) {
                        final Future<?> future = executorService.submit(() -> {
                            try {
                                storeSite(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client,
                                        site);
                            } catch (final Exception e) {
                                logger.warn("Failed to process site: {}", site.getDisplayName(), e);
                                if (!isIgnoreError(paramMap)) {
                                    throw new DataStoreCrawlingException(site.getDisplayName(),
                                            "Failed to process site: " + site.getDisplayName(), e);
                                }
                            }
                        });
                        siteProcessingFutures.add(future);
                    }
                });

                // Wait for all site processing tasks to complete
                for (final Future<?> future : siteProcessingFutures) {
                    try {
                        future.get();
                    } catch (final Exception e) {
                        logger.warn("A site processing task was interrupted/failed.", e);
                        if (!isIgnoreError(paramMap)) {
                            throw new DataStoreCrawlingException("site processing", "A site processing task failed", e);
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

    protected void storeSite(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Microsoft365Client client, final Site site) {

        if (logger.isDebugEnabled()) {
            logger.debug("Processing site: {} ({})", site.getDisplayName(), site.getId());
        }

        // Store site metadata as a document
        storeSiteDocument(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, site);

        // Crawl document libraries in the site
        try {
            final List<Future<?>> driveProcessingFutures = new java.util.concurrent.CopyOnWriteArrayList<>();
            client.getDrives(drive -> {
                if (drive.getDriveType() != null && "documentLibrary".equals(drive.getDriveType()) && !isSystemLibrary(drive)) {
                    if (!isIgnoreSystemLibraries(paramMap) || !isSystemLibrary(drive)) {
                        driveProcessingFutures.add(executorService.submit(() -> {
                            try {
                                storeDriveItems(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, site, drive);
                            } catch (final Exception e) {
                                logger.warn("Failed to process drive: {} in site: {}", drive.getName(), site.getDisplayName(), e);
                                if (!isIgnoreError(paramMap)) {
                                    throw new DataStoreCrawlingException(site.getDisplayName(),
                                            "Failed to process drive: " + drive.getName(), e);
                                }
                            }
                        }));
                    }
                }
            });

            // Wait for all drive processing tasks to complete
            for (final Future<?> future : driveProcessingFutures) {
                try {
                    future.get();
                } catch (final Exception e) {
                    logger.warn("A drive processing task for site {} was interrupted/failed.", site.getDisplayName(), e);
                    if (!isIgnoreError(paramMap)) {
                        throw new DataStoreCrawlingException(site.getDisplayName(),
                                "A drive processing task failed for site: " + site.getDisplayName(), e);
                    }
                }
            }
        } catch (final Exception e) {
            logger.warn("Failed to get drives for site: {}", site.getDisplayName(), e);
            if (!isIgnoreError(paramMap)) {
                throw new DataStoreCrawlingException(site.getDisplayName(), "Failed to get drives for site: " + site.getDisplayName(), e);
            }
        }
    }

    protected void storeSiteDocument(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final Site site) {

        final Map<String, Object> dataMap = new LinkedHashMap<>(defaultDataMap);

        try {
            final String siteUrl = site.getWebUrl();
            dataMap.put(SITE_ID_FIELD, site.getId());
            dataMap.put(SITE_NAME, site.getDisplayName());
            dataMap.put(SITE_DESCRIPTION, site.getDescription());
            dataMap.put(SITE_URL, siteUrl);
            dataMap.put(SITE_CREATED, site.getCreatedDateTime());
            dataMap.put(SITE_MODIFIED, site.getLastModifiedDateTime());

            if (site.getSiteCollection() != null && site.getSiteCollection().getRoot() != null) {
                dataMap.put(SITE_TYPE, "root");
            } else {
                dataMap.put(SITE_TYPE, "subsite");
            }

            // Set roles/permissions (simplified - SharePoint permissions are complex)
            final List<String> roles = Collections.emptyList();
            dataMap.put(SITE_ROLES, roles);

            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldUrl(), siteUrl);
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldTitle(), site.getDisplayName());
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldContent(),
                    StringUtil.isNotBlank(site.getDescription()) ? site.getDescription() : "");
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldLastModified(), site.getLastModifiedDateTime());
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldMimetype(), "text/html");

            callback.store(paramMap, dataMap);

        } catch (final Exception e) {
            logger.warn("Failed to store site document: {}", site.getDisplayName(), e);
            if (!isIgnoreError(paramMap)) {
                throw new DataStoreCrawlingException(site.getDisplayName(), "Failed to store site document: " + site.getDisplayName(), e);
            }
        }
    }

    protected void storeDriveItems(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final Site site, final Drive drive) {

        if (logger.isDebugEnabled()) {
            logger.debug("Processing drive: {} in site: {}", drive.getName(), site.getDisplayName());
        }

        try {
            // Get items from the drive and process them (reuse OneDrive patterns)
            getDriveItems(client, drive.getId(), item -> {
                if (isTargetItem(paramMap, item)) {
                    try {
                        processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, site, drive, item,
                                Collections.emptyList());
                    } catch (final Exception e) {
                        logger.warn("Failed to process drive item: {} in drive: {}", item.getName(), drive.getName(), e);
                        if (!isIgnoreError(paramMap)) {
                            throw new DataStoreCrawlingException(drive.getName(), "Failed to process drive item: " + item.getName(), e);
                        }
                    }
                }
            });
        } catch (final Exception e) {
            logger.warn("Failed to get drive items from drive: {}", drive.getName(), e);
            if (!isIgnoreError(paramMap)) {
                throw new DataStoreCrawlingException(drive.getName(), "Failed to get drive items from drive: " + drive.getName(), e);
            }
        }
    }

    protected void getDriveItems(final Microsoft365Client client, final String driveId,
            final java.util.function.Consumer<DriveItem> consumer) {
        client.getDriveItemsInDrive(driveId, consumer);
    }

    protected void processDriveItem(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final Site site, final Drive drive, final DriveItem item, final List<String> roles) {

        // Reuse OneDrive's processDriveItem logic but add site context
        final Map<String, Object> dataMap = new LinkedHashMap<>(defaultDataMap);

        try {
            // Add site-specific fields
            dataMap.put(SITE_ID_FIELD, site.getId());
            dataMap.put(SITE_NAME, site.getDisplayName());
            dataMap.put(SITE_URL, site.getWebUrl());

            // Add file fields (following OneDrive patterns)
            dataMap.put(FILE_ID, item.getId());
            dataMap.put(FILE_NAME, item.getName());
            dataMap.put(FILE_WEB_URL, item.getWebUrl());
            dataMap.put(FILE_CREATED, item.getCreatedDateTime());
            dataMap.put(FILE_LAST_MODIFIED, item.getLastModifiedDateTime());

            if (item.getSize() != null) {
                dataMap.put(FILE_SIZE, item.getSize());
            }

            if (item.getFile() != null && item.getFile().getMimeType() != null) {
                dataMap.put(FILE_MIMETYPE, item.getFile().getMimeType());
            }

            // Set parent path with site context
            if (item.getParentReference() != null && item.getParentReference().getPath() != null) {
                dataMap.put(FILE_PARENT_PATH, site.getDisplayName() + "/" + drive.getName() + "/" + item.getParentReference().getPath());
            } else {
                dataMap.put(FILE_PARENT_PATH, site.getDisplayName() + "/" + drive.getName());
            }

            // Set roles
            dataMap.put(FILE_ROLES, roles);

            // Set standard Fess fields
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldUrl(), item.getWebUrl());
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldTitle(), item.getName());
            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldLastModified(), item.getLastModifiedDateTime());

            // Get file content if it's not a folder
            if (item.getFile() != null) {
                final long maxSize = getMaxSize(paramMap);

                // Check file size before attempting to download and extract content
                if (item.getSize() != null && item.getSize() > maxSize) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Skipping content extraction for file {} (size: {} bytes) - exceeds max_content_length: {} bytes",
                                item.getName(), item.getSize(), maxSize);
                    }
                    dataMap.put(ComponentUtil.getFessConfig().getIndexFieldContent(), "");
                    dataMap.put(FILE_CONTENTS, "");
                } else {
                    // Check supported MIME types
                    if (isSupportedMimeType(paramMap, item)) {
                        try {
                            final String content = getDriveItemContents(client, drive.getId(), item, maxSize, isIgnoreError(paramMap));
                            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldContent(), content);
                            dataMap.put(FILE_CONTENTS, content);
                        } catch (final Exception e) {
                            logger.warn("Failed to get content for item: {}", item.getName(), e);
                            dataMap.put(ComponentUtil.getFessConfig().getIndexFieldContent(), "");
                            dataMap.put(FILE_CONTENTS, "");
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Skipping content extraction for file {} - unsupported MIME type: {}", item.getName(),
                                    item.getFile() != null ? item.getFile().getMimeType() : "unknown");
                        }
                        dataMap.put(ComponentUtil.getFessConfig().getIndexFieldContent(), "");
                        dataMap.put(FILE_CONTENTS, "");
                    }
                }

                if (item.getFile().getMimeType() != null) {
                    dataMap.put(ComponentUtil.getFessConfig().getIndexFieldMimetype(), item.getFile().getMimeType());
                } else {
                    dataMap.put(ComponentUtil.getFessConfig().getIndexFieldMimetype(), "application/octet-stream");
                }
            } else {
                dataMap.put(ComponentUtil.getFessConfig().getIndexFieldContent(), "");
                dataMap.put(ComponentUtil.getFessConfig().getIndexFieldMimetype(), "text/html");
            }

            callback.store(paramMap, dataMap);

        } catch (final Exception e) {
            logger.warn("Failed to process drive item: {} in site: {}", item.getName(), site.getDisplayName(), e);
            if (!isIgnoreError(paramMap)) {
                throw new DataStoreCrawlingException(site.getDisplayName(), "Failed to process drive item: " + item.getName(), e);
            }
        }
    }

    // Configuration helper methods
    protected String getSiteId(final DataStoreParams paramMap) {
        return paramMap.getAsString(SITE_ID, null);
    }

    protected boolean isExcludedSite(final DataStoreParams paramMap, final Site site) {
        final String excludeIds = paramMap.getAsString(EXCLUDE_SITE_ID, null);
        if (StringUtil.isBlank(excludeIds)) {
            return false;
        }

        // Handle different delimiter scenarios for SharePoint site IDs
        final String[] ids;
        if (excludeIds.contains(";")) {
            // Multiple SharePoint site IDs separated by semicolon
            ids = excludeIds.split(";");
        } else if (excludeIds.contains(".sharepoint.com,")
                && excludeIds.matches(".*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.*")) {
            // Single SharePoint site ID containing commas (format: hostname,siteCollectionId,siteId)
            // Don't split - treat entire string as one ID
            ids = new String[] { excludeIds };
        } else {
            // Legacy format: comma-separated simple site IDs (for backward compatibility)
            ids = excludeIds.split(",");
        }

        for (final String id : ids) {
            if (site.getId().equals(id.trim())) {
                return true;
            }
        }
        return false;
    }

    protected boolean isTargetSiteType(final DataStoreParams paramMap, final Site site) {
        final String typeFilter = paramMap.getAsString(SITE_TYPE_FILTER, null);
        if (StringUtil.isBlank(typeFilter)) {
            return true;
        }
        // Simple type filtering - can be enhanced based on requirements
        return true;
    }

    protected boolean isSystemLibrary(final Drive drive) {
        if (drive.getName() == null) {
            return false;
        }
        final String name = drive.getName().toLowerCase();
        return name.contains("form") || name.contains("style") || name.contains("_catalogs") || name.equals("formservertemplates");
    }

    protected boolean isIgnoreSystemLibraries(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_SYSTEM_LIBRARIES, Constants.TRUE));
    }

    protected boolean isTargetItem(final DataStoreParams paramMap, final DriveItem item) {
        // Apply include/exclude patterns (reuse OneDrive logic)
        final String includePattern = paramMap.getAsString(INCLUDE_PATTERN, null);
        final String excludePattern = paramMap.getAsString(EXCLUDE_PATTERN, null);

        if (StringUtil.isNotBlank(includePattern)) {
            final Pattern pattern = Pattern.compile(includePattern);
            if (!pattern.matcher(item.getName()).matches()) {
                return false;
            }
        }

        if (StringUtil.isNotBlank(excludePattern)) {
            final Pattern pattern = Pattern.compile(excludePattern);
            if (pattern.matcher(item.getName()).matches()) {
                return false;
            }
        }

        return true;
    }

    protected long getMaxSize(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(MAX_CONTENT_LENGTH, "10485760"); // 10MB default
        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException e) {
            logger.warn("Invalid max content length: {}", value);
            return 10485760L;
        }
    }

    protected boolean isSupportedMimeType(final DataStoreParams paramMap, final DriveItem item) {
        final String supportedMimetypes = paramMap.getAsString(SUPPORTED_MIMETYPES, null);

        // If no supported mime types are specified, allow all
        if (StringUtil.isBlank(supportedMimetypes)) {
            return true;
        }

        // If item doesn't have a file or mime type, skip content extraction
        if (item.getFile() == null || item.getFile().getMimeType() == null) {
            return false;
        }

        final String itemMimeType = item.getFile().getMimeType().toLowerCase();
        final String[] supportedTypes = supportedMimetypes.toLowerCase().split(",");

        for (final String supportedType : supportedTypes) {
            final String trimmedType = supportedType.trim();
            if (trimmedType.equals("*") || itemMimeType.equals(trimmedType)
                    || (trimmedType.endsWith("/*") && itemMimeType.startsWith(trimmedType.substring(0, trimmedType.length() - 1)))) {
                return true;
            }
        }

        return false;
    }

    protected boolean isIgnoreError(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.FALSE));
    }

    protected boolean isIgnoreFolder(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_FOLDER, Constants.FALSE));
    }

    protected String getDriveItemContents(final Microsoft365Client client, final String driveId, final DriveItem item,
            final long maxContentLength, final boolean ignoreError) {
        if (item.getFile() != null) {
            try (final java.io.InputStream in = client.getDriveContent(driveId, item.getId())) {
                return ComponentUtil.getExtractorFactory()
                        .builder(in, Collections.emptyMap())
                        .filename(item.getName())
                        .maxContentLength(maxContentLength)
                        .extractorName(extractorName)
                        .extract()
                        .getContent();
            } catch (final Exception e) {
                if (!ignoreError && !ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                    throw new DataStoreCrawlingException(item.getWebUrl(), "Failed to get contents: " + item.getName(), e);
                }
                if (logger.isDebugEnabled()) {
                    logger.warn("Failed to get contents: {}", item.getName(), e);
                } else {
                    logger.warn("Failed to get contents: {}. {}", item.getName(), e.getMessage());
                }
                return StringUtil.EMPTY;
            }
        }
        return StringUtil.EMPTY;
    }

    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }
}
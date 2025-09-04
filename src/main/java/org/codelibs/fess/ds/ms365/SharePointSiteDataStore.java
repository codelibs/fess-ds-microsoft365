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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    protected static final String IGNORE_ERROR = "ignore_error";

    // Field mappings
    protected static final String SITE = "site";
    protected static final String SITE_NAME = "name";
    protected static final String SITE_DESCRIPTION = "description";
    protected static final String SITE_URL = "web_url";
    protected static final String SITE_CREATED = "created";
    protected static final String SITE_MODIFIED = "modified";
    protected static final String SITE_TYPE = "type";
    protected static final String SITE_ROLES = "roles";
    protected static final String SITE_CONTENT = "content";
    protected static final String SITE_ID_FIELD = "id";

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
        configMap.put(MAX_CONTENT_LENGTH, getMaxSize(paramMap));
        configMap.put(IGNORE_ERROR, isIgnoreError(paramMap));
        configMap.put(SUPPORTED_MIMETYPES, getSupportedMimeTypes(paramMap));
        configMap.put(URL_FILTER, getUrlFilter(paramMap));
        if (logger.isDebugEnabled()) {
            logger.debug("configMap: {}", configMap);
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            final String siteId = getSiteId(paramMap);
            if (StringUtil.isNotBlank(siteId)) {
                // Crawl specific site, but check if it should be excluded
                final Site site = client.getSite(siteId);
                if (logger.isDebugEnabled()) {
                    logger.debug("Crawling site: {} ({})", site.getDisplayName(), site.getId());
                }
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
                    if (logger.isDebugEnabled()) {
                        logger.debug("Crawling site: {} ({})", site.getDisplayName(), site.getId());
                    }
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
    }

    protected void storeSiteDocument(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final Site site) {

        final String siteUrl = site.getWebUrl();
        final Map<String, Object> dataMap = new LinkedHashMap<>(defaultDataMap);
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final StatsKeyObject statsKey = new StatsKeyObject(siteUrl);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            logger.info("Crawling URL: {}", siteUrl);
            crawlerStatsHelper.begin(statsKey);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> siteMap = new HashMap<>();

            siteMap.put(SITE_ID_FIELD, site.getId());
            siteMap.put(SITE_NAME, site.getDisplayName());
            siteMap.put(SITE_DESCRIPTION, site.getDescription());
            siteMap.put(SITE_URL, siteUrl);
            siteMap.put(SITE_CREATED, site.getCreatedDateTime());
            siteMap.put(SITE_MODIFIED, site.getLastModifiedDateTime());

            if (site.getSiteCollection() != null && site.getSiteCollection().getRoot() != null) {
                siteMap.put(SITE_TYPE, "root");
            } else {
                siteMap.put(SITE_TYPE, "subsite");
            }

            // Build site content with basic information and drives metadata
            final StringBuilder contentBuilder = new StringBuilder();

            // Add site basic information
            if (StringUtil.isNotBlank(site.getDisplayName())) {
                contentBuilder.append(site.getDisplayName()).append(" ");
            }
            if (StringUtil.isNotBlank(site.getDescription())) {
                contentBuilder.append(site.getDescription()).append(" ");
            }
            if (StringUtil.isNotBlank(site.getWebUrl())) {
                contentBuilder.append(site.getWebUrl()).append(" ");
            }

            // Add document libraries information for richer content
            try {
                client.getDrives(drive -> {
                    if ("documentLibrary".equals(drive.getDriveType()) && !isSystemLibrary(drive)) {
                        if (StringUtil.isNotBlank(drive.getName())) {
                            contentBuilder.append(drive.getName()).append(" ");
                        }
                        if (StringUtil.isNotBlank(drive.getDescription())) {
                            contentBuilder.append(drive.getDescription()).append(" ");
                        }
                    }
                });
            } catch (Exception e) {
                logger.debug("Failed to get drives for content building, using basic site info only", e);
            }

            siteMap.put(SITE_CONTENT, contentBuilder.toString().trim());

            // Improved roles/permissions handling based on processDriveItem
            final List<String> roles = new ArrayList<>();
            try {
                if (site.getPermissions() != null) {
                    site.getPermissions().stream().forEach(x -> {
                        if (x.getRoles() != null) {
                            roles.addAll(x.getRoles());
                        }
                    });
                }
            } catch (Exception e) {
                logger.debug("Failed to get site permissions, continuing with empty roles", e);
            }

            // Add PermissionHelper usage and default permissions handling
            final FessConfig fessConfig = ComponentUtil.getFessConfig();
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();

            // Handle default permissions from paramMap
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(roles::add));

            // Handle default permissions from defaultDataMap
            if (defaultDataMap.get(fessConfig.getIndexFieldRole()) instanceof List<?> roleTypeList) {
                roleTypeList.stream().map(s -> (String) s).forEach(roles::add);
            }

            // Set deduplicated roles
            siteMap.put(SITE_ROLES, roles.stream().distinct().collect(Collectors.toList()));

            resultMap.put("site", siteMap);

            if (logger.isDebugEnabled()) {
                logger.debug("siteMap: {}", siteMap);
            }

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            final String scriptType = getScriptType(paramMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);
            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : {}", dataMap, e);

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
            failureUrlService.store(dataConfig, errorName, siteUrl, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), siteUrl, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    // Configuration helper methods
    protected String getSiteId(final DataStoreParams paramMap) {
        return paramMap.getAsString(SITE_ID, null);
    }

    protected boolean isExcludedSite(final DataStoreParams paramMap, final Site site) {
        final String excludeIds = paramMap.getAsString(EXCLUDE_SITE_ID, null);
        if (logger.isDebugEnabled()) {
            logger.debug("excludeIds: {}", excludeIds);
        }
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
            // Single SharePoint site ID containing commas (format:
            // hostname,siteCollectionId,siteId)
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
        final String siteType = (site.getSiteCollection() != null && site.getSiteCollection().getRoot() != null) ? "root" : "subsite";
        final String[] filters = typeFilter.split(",");
        for (final String filter : filters) {
            if (siteType.equalsIgnoreCase(filter.trim())) {
                return true;
            }
        }
        return false;
    }

    protected boolean isSystemLibrary(final Drive drive) {
        if (drive.getName() == null) {
            return false;
        }
        final String name = drive.getName().toLowerCase();
        if (logger.isDebugEnabled()) {
            logger.debug("Checking if drive is a system library: {}", name);
        }
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

    protected String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
        return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                .get(stream -> stream.map(String::trim).toArray(n -> new String[n]));
    }

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

    protected String getUrl(final Map<String, Object> configMap, final DataStoreParams paramMap, final DriveItem item) {
        return item.getWebUrl();
    }

    void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }
}
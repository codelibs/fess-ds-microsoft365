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
    /** The parameter name for the site ID. */
    protected static final String SITE_ID = "site_id";
    /** The parameter name for excluded site IDs. */
    protected static final String EXCLUDE_SITE_ID = "exclude_site_id";
    /** The parameter name for the site type filter. */
    protected static final String SITE_TYPE_FILTER = "site_type_filter";
    /** The parameter name for including subsites. */
    protected static final String INCLUDE_SUBSITES = "include_subsites";
    /** The parameter name for ignoring system libraries. */
    protected static final String IGNORE_SYSTEM_LIBRARIES = "ignore_system_libraries";
    /** The parameter name for the number of threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** The parameter name for default permissions. */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** The parameter name for maximum content length. */
    protected static final String MAX_CONTENT_LENGTH = "max_content_length";
    /** The parameter name for supported MIME types. */
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    /** The parameter name for the include pattern. */
    protected static final String INCLUDE_PATTERN = "include_pattern";
    /** The parameter name for the exclude pattern. */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    /** The parameter name for URL filter. */
    protected static final String URL_FILTER = "url_filter";
    /** The parameter name for ignoring errors. */
    protected static final String IGNORE_ERROR = "ignore_error";

    // Field mappings
    /** The field name for site. */
    protected static final String SITE = "site";
    /** The field name for site name. */
    protected static final String SITE_NAME = "name";
    /** The field name for site description. */
    protected static final String SITE_DESCRIPTION = "description";
    /** The field name for site URL. */
    protected static final String SITE_URL = "web_url";
    /** The field name for site creation date. */
    protected static final String SITE_CREATED = "created";
    /** The field name for site modification date. */
    protected static final String SITE_MODIFIED = "modified";
    /** The field name for site type. */
    protected static final String SITE_TYPE = "type";
    /** The field name for site roles. */
    protected static final String SITE_ROLES = "roles";
    /** The field name for site content. */
    protected static final String SITE_CONTENT = "content";
    /** The field name for site ID. */
    protected static final String SITE_ID_FIELD = "id";

    /** The name of the extractor for SharePoint sites. */
    protected String extractorName = "sharePointSiteExtractor";

    /**
     * Creates a new SharePointSiteDataStore instance.
     */
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
            logger.debug("SharePoint sites crawling started - Configuration: MaxSize={}, IgnoreError={}, MimeTypes={}, Threads={}",
                    configMap.get(MAX_CONTENT_LENGTH), configMap.get(IGNORE_ERROR),
                    java.util.Arrays.toString((String[]) configMap.get(SUPPORTED_MIMETYPES)), paramMap.getAsString(NUMBER_OF_THREADS, "1"));
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            final String siteId = getSiteId(paramMap);
            if (StringUtil.isNotBlank(siteId)) {
                // Crawl specific site, but check if it should be excluded
                if (logger.isDebugEnabled()) {
                    logger.debug("Crawling specific site with ID: {}", siteId);
                }

                final Site site = client.getSite(siteId);
                if (logger.isDebugEnabled()) {
                    logger.debug("Retrieved site: {} (ID: {}, WebUrl: {})", site.getDisplayName(), site.getId(), site.getWebUrl());
                }
                if (!isExcludedSite(paramMap, site)) {
                    storeSite(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client, site);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Skipping excluded site: {} (ID: {})", site.getDisplayName(), site.getId());
                    }
                }
            } else {
                // Crawl all sites using parallel processing
                if (logger.isDebugEnabled()) {
                    logger.debug("Crawling all sites with parallel processing");
                }

                client.getSites(site -> {

                    if (logger.isDebugEnabled()) {
                        logger.debug("Evaluating site: {} (ID: {}, Type: {}, Excluded: {}, TargetType: {})", site.getDisplayName(),
                                site.getId(), site.getSiteCollection() != null ? "SiteCollection" : "Site", isExcludedSite(paramMap, site),
                                isTargetSiteType(paramMap, site));
                    }

                    if (!isExcludedSite(paramMap, site) && isTargetSiteType(paramMap, site)) {
                        try {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Processing site: {} (ID: {})", site.getDisplayName(), site.getId());
                            }
                            storeSite(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client, site);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Successfully processed site: {} (ID: {})", site.getDisplayName(), site.getId());
                            }
                        } catch (final Exception e) {
                            logger.warn("Failed to process site: {} (ID: {})", site.getDisplayName(), site.getId(), e);
                            if (!isIgnoreError(paramMap)) {
                                throw new DataStoreCrawlingException(site.getDisplayName(),
                                        "Failed to process site: " + site.getDisplayName(), e);
                            }
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Skipped site: {} (ID: {}) - Excluded: {}, TargetType: {}", site.getDisplayName(), site.getId(),
                                    isExcludedSite(paramMap, site), isTargetSiteType(paramMap, site));
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
     * Stores a SharePoint site and its document libraries.
     *
     * @param dataConfig the data configuration
     * @param callback the index update callback
     * @param configMap the configuration map
     * @param paramMap the data store parameters
     * @param scriptMap the script map
     * @param defaultDataMap the default data map
     * @param executorService the executor service for parallel processing
     * @param client the Microsoft365 client
     * @param site the SharePoint site to store
     */
    protected void storeSite(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Microsoft365Client client, final Site site) {
        executorService.execute(() -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing site: {} ({})", site.getDisplayName(), site.getId());
            }
            // Store site metadata as a document
            storeSiteDocument(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, site);
        });
    }

    /**
     * Stores a SharePoint site document for indexing.
     *
     * @param dataConfig the data configuration
     * @param callback the index update callback
     * @param configMap the configuration map
     * @param paramMap the data store parameters
     * @param scriptMap the script map
     * @param defaultDataMap the default data map
     * @param client the Microsoft365 client
     * @param site the SharePoint site
     */
    protected void storeSiteDocument(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final Site site) {

        final String siteUrl = site.getWebUrl();
        final Map<String, Object> dataMap = new LinkedHashMap<>(defaultDataMap);
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final StatsKeyObject statsKey = new StatsKeyObject(siteUrl);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);

        if (logger.isDebugEnabled()) {
            logger.debug("Processing site document - Name: {}, ID: {}, URL: {}, Type: {}, Created: {}, Modified: {}", site.getDisplayName(),
                    site.getId(), siteUrl,
                    site.getSiteCollection() != null && site.getSiteCollection().getRoot() != null ? "root" : "subsite",
                    site.getCreatedDateTime(), site.getLastModifiedDateTime());
        }

        try {
            logger.info("Crawling site URL: {} (Name: {})", siteUrl, site.getDisplayName());
            crawlerStatsHelper.begin(statsKey);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> siteMap = new HashMap<>();

            siteMap.put(SITE_ID_FIELD, site.getId());
            siteMap.put(SITE_NAME, site.getDisplayName());
            siteMap.put(SITE_DESCRIPTION, site.getDescription());
            siteMap.put(SITE_URL, siteUrl);
            siteMap.put(SITE_CREATED, site.getCreatedDateTime());
            siteMap.put(SITE_MODIFIED, site.getLastModifiedDateTime());

            final String siteType;
            if (site.getSiteCollection() != null && site.getSiteCollection().getRoot() != null) {
                siteType = "root";
            } else {
                siteType = "subsite";
            }
            siteMap.put(SITE_TYPE, siteType);

            if (logger.isDebugEnabled()) {
                logger.debug("Site metadata prepared - Type: {}, Description length: {}", siteType,
                        site.getDescription() != null ? site.getDescription().length() : 0);
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

            if (logger.isDebugEnabled()) {
                logger.debug("Building site content - retrieving document libraries for site: {}", site.getDisplayName());
            }

            try {
                client.getDrives(drive -> {
                    if ("documentLibrary".equals(drive.getDriveType()) && !isSystemLibrary(drive)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Adding document library to content - Name: {}, ID: {}, System: {}", drive.getName(),
                                    drive.getId(), isSystemLibrary(drive));
                        }
                        if (StringUtil.isNotBlank(drive.getName())) {
                            contentBuilder.append(drive.getName()).append(" ");
                        }
                        if (StringUtil.isNotBlank(drive.getDescription())) {
                            contentBuilder.append(drive.getDescription()).append(" ");
                        }
                    }
                });
            } catch (Exception e) {
                logger.warn("Failed to get drives for content building for site: {} - using basic site info only", site.getDisplayName(),
                        e);
            }

            final String siteContent = contentBuilder.toString().trim();
            siteMap.put(SITE_CONTENT, siteContent);

            if (logger.isDebugEnabled()) {
                logger.debug("Site content built - Content length: {} characters", siteContent.length());
            }

            final List<String> roles = getSitePermissions(client, site.getId());
            if (logger.isDebugEnabled()) {
                logger.debug("Initial permissions for site {} - Count: {}, Permissions: {}", site.getDisplayName(), roles.size(), roles);
            }

            // Add PermissionHelper usage and default permissions handling
            final FessConfig fessConfig = ComponentUtil.getFessConfig();
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(roles::add));
            if (defaultDataMap.get(fessConfig.getIndexFieldRole()) instanceof List<?> roleTypeList) {
                roleTypeList.stream().map(s -> (String) s).forEach(roles::add);
            }

            // Set deduplicated roles
            final List<String> finalRoles = roles.stream().distinct().collect(Collectors.toList());
            siteMap.put(SITE_ROLES, finalRoles);

            if (logger.isDebugEnabled()) {
                logger.debug("Final permissions for site {} - Count: {}, Permissions: {}", site.getDisplayName(), finalRoles.size(),
                        finalRoles);
            }

            resultMap.put("site", siteMap);

            if (logger.isDebugEnabled()) {
                logger.debug("Site map prepared for processing - Fields: {}, Content size: {}, Roles: {}", siteMap.size(),
                        siteContent.length(), finalRoles.size());
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
                logger.debug("Final data map prepared for indexing - Fields: {}, URL: {}", dataMap.size(), dataMap.get("url"));
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully indexed site: {} (ID: {})", site.getDisplayName(), site.getId());
            }
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception for site: {} (ID: {}, URL: {}) - Data: {}", site.getDisplayName(), site.getId(), siteUrl,
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
            failureUrlService.store(dataConfig, errorName, siteUrl, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Processing exception for site: {} (ID: {}, URL: {}) - Data: {}", site.getDisplayName(), site.getId(), siteUrl,
                    dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), siteUrl, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
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
     * Checks if the site should be excluded from crawling.
     *
     * @param paramMap the data store parameters
     * @param site the SharePoint site to check
     * @return true if the site should be excluded, false otherwise
     */
    protected boolean isExcludedSite(final DataStoreParams paramMap, final Site site) {
        final String excludeIds = paramMap.getAsString(EXCLUDE_SITE_ID, null);
        if (logger.isDebugEnabled()) {
            logger.debug("Checking site exclusion - Site: {} (ID: {}), Exclude pattern: {}", site.getDisplayName(), site.getId(),
                    excludeIds);
        }

        if (StringUtil.isBlank(excludeIds)) {
            if (logger.isDebugEnabled()) {
                logger.debug("No exclusion pattern configured - site not excluded: {}", site.getDisplayName());
            }
            return false;
        }

        // Handle different delimiter scenarios for SharePoint site IDs
        final String[] ids;
        if (excludeIds.contains(";")) {
            // Multiple SharePoint site IDs separated by semicolon
            ids = excludeIds.split(";");
            if (logger.isDebugEnabled()) {
                logger.debug("Using semicolon delimiter - parsing {} exclusion IDs", ids.length);
            }
        } else if (excludeIds.contains(".sharepoint.com,")
                && excludeIds.matches(".*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.*")) {
            // Single SharePoint site ID containing commas (format:
            // hostname,siteCollectionId,siteId)
            // Don't split - treat entire string as one ID
            ids = new String[] { excludeIds };
            if (logger.isDebugEnabled()) {
                logger.debug("Using SharePoint composite format - single exclusion ID: {}", excludeIds);
            }
        } else {
            // Legacy format: comma-separated simple site IDs (for backward compatibility)
            ids = excludeIds.split(",");
            if (logger.isDebugEnabled()) {
                logger.debug("Using comma delimiter (legacy) - parsing {} exclusion IDs", ids.length);
            }
        }

        for (final String id : ids) {
            final String trimmedId = id.trim();
            if (site.getId().equals(trimmedId)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Site excluded - Site: {} (ID: {}) matches exclusion ID: {}", site.getDisplayName(), site.getId(),
                            trimmedId);
                }
                return true;
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Site not excluded - Site: {} (ID: {}) does not match any exclusion IDs", site.getDisplayName(), site.getId());
        }
        return false;
    }

    /**
     * Checks if the site matches the target site type filter.
     *
     * @param paramMap the data store parameters
     * @param site the SharePoint site to check
     * @return true if the site matches the type filter, false otherwise
     */
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

    /**
     * Checks if the drive is a system library.
     *
     * @param drive the drive to check
     * @return true if the drive is a system library, false otherwise
     */
    protected boolean isSystemLibrary(final Drive drive) {
        if (drive.getName() == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Drive name is null - not considered system library: {}", drive.getId());
            }
            return false;
        }
        final String name = drive.getName().toLowerCase();
        final boolean isSystem =
                name.contains("form") || name.contains("style") || name.contains("_catalogs") || name.equals("formservertemplates");

        if (logger.isDebugEnabled()) {
            logger.debug("System library check - Drive: {} (ID: {}), IsSystem: {}, Patterns matched: {}", drive.getName(), drive.getId(),
                    isSystem,
                    java.util.Arrays
                            .toString(new String[] { name.contains("form") ? "form" : null, name.contains("style") ? "style" : null,
                                    name.contains("_catalogs") ? "_catalogs" : null,
                                    name.equals("formservertemplates") ? "formservertemplates" : null })
                            .replaceAll("null,?", "")
                            .replaceAll("\\[,", "[")
                            .replaceAll(",\\]", "]"));
        }
        return isSystem;
    }

    /**
     * Checks if system libraries should be ignored during crawling.
     *
     * @param paramMap the data store parameters
     * @return true if system libraries should be ignored, false otherwise
     */
    protected boolean isIgnoreSystemLibraries(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_SYSTEM_LIBRARIES, Constants.TRUE));
    }

    /**
     * Checks if the drive item should be crawled based on include/exclude patterns.
     *
     * @param paramMap the data store parameters
     * @param item the drive item to check
     * @return true if the item should be crawled, false otherwise
     */
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

    /**
     * Gets the maximum content size for files.
     *
     * @param paramMap the data store parameters
     * @return the maximum content size in bytes
     */
    protected long getMaxSize(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(MAX_CONTENT_LENGTH, "10485760"); // 10MB default
        try {
            return Long.parseLong(value);
        } catch (final NumberFormatException e) {
            logger.warn("Invalid max content length: {}", value);
            return 10485760L;
        }
    }

    /**
     * Checks if the drive item has a supported MIME type.
     *
     * @param paramMap the data store parameters
     * @param item the drive item to check
     * @return true if the MIME type is supported, false otherwise
     */
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
     * Gets the supported MIME types from the parameter map.
     *
     * @param paramMap the data store parameters
     * @return an array of supported MIME types
     */
    protected String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
        return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                .get(stream -> stream.map(String::trim).toArray(n -> new String[n]));
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
        final String exclude = paramMap.getAsString(EXCLUDE_PATTERN);

        if (logger.isDebugEnabled()) {
            logger.debug("Setting up URL filter - Include pattern: {}, Exclude pattern: {}", include, exclude);
        }

        if (StringUtil.isNotBlank(include)) {
            urlFilter.addInclude(include);
            if (logger.isDebugEnabled()) {
                logger.debug("Added include pattern to URL filter: {}", include);
            }
        }

        if (StringUtil.isNotBlank(exclude)) {
            urlFilter.addExclude(exclude);
            if (logger.isDebugEnabled()) {
                logger.debug("Added exclude pattern to URL filter: {}", exclude);
            }
        }

        urlFilter.init(paramMap.getAsString(Constants.CRAWLING_INFO_ID));

        if (logger.isDebugEnabled()) {
            logger.debug("URL filter initialized - CrawlingInfoId: {}, HasInclude: {}, HasExclude: {}",
                    paramMap.getAsString(Constants.CRAWLING_INFO_ID), StringUtil.isNotBlank(include), StringUtil.isNotBlank(exclude));
        }
        return urlFilter;
    }

    /**
     * Gets the URL for a drive item.
     *
     * @param configMap the configuration map
     * @param paramMap the data store parameters
     * @param item the drive item
     * @return the URL of the drive item
     */
    protected String getUrl(final Map<String, Object> configMap, final DataStoreParams paramMap, final DriveItem item) {
        return item.getWebUrl();
    }

    /**
     * Sets the extractor name for SharePoint sites.
     *
     * @param extractorName the extractor name to set
     */
    void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }
}
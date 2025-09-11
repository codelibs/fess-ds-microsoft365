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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
 * SharePointDocLibDataStore crawls SharePoint document libraries as metadata entities.
 * File content crawling within document libraries is handled by OneDriveDataStore.
 *
 * @author shinsuke
 */
public class SharePointDocLibDataStore extends Microsoft365DataStore {

    private static final Logger logger = LogManager.getLogger(SharePointDocLibDataStore.class);

    // Configuration parameters
    /** Site ID parameter name for specifying which SharePoint site to crawl */
    protected static final String SITE_ID = "site_id";
    /** Comma-separated list of site IDs to exclude from crawling */
    protected static final String EXCLUDE_SITE_ID = "exclude_site_id";
    /** Flag to ignore system document libraries */
    protected static final String IGNORE_SYSTEM_LIBRARIES = "ignore_system_libraries";
    /** Number of concurrent threads for processing */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** Default permissions to assign to crawled documents */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** Maximum content length in bytes for file extraction */
    protected static final String MAX_CONTENT_LENGTH = "max_content_length";
    /** Comma-separated list of supported MIME types */
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    /** Flag to continue crawling on errors */
    protected static final String IGNORE_ERROR = "ignore_error";
    /** Flag to skip folder documents */
    protected static final String IGNORE_FOLDER = "ignore_folder";
    /** Regular expression pattern for files to include */
    protected static final String INCLUDE_PATTERN = "include_pattern";
    /** Regular expression pattern for files to exclude */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";

    // Field mappings for document libraries
    /** Document library prefix for field mappings */
    protected static final String DOCLIB = "doclib";
    /** Field mapping for document library name */
    protected static final String DOCLIB_NAME = "name";
    /** Field mapping for document library description */
    protected static final String DOCLIB_DESCRIPTION = "description";
    /** Field mapping for document library web URL */
    protected static final String DOCLIB_URL = "web_url";
    /** Field mapping for document library creation date */
    protected static final String DOCLIB_CREATED = "created";
    /** Field mapping for document library modification date */
    protected static final String DOCLIB_MODIFIED = "modified";
    /** Field mapping for document library type */
    protected static final String DOCLIB_TYPE = "type";
    /** Field mapping for document library access roles */
    protected static final String DOCLIB_ROLES = "roles";
    /** Field mapping for document library content */
    protected static final String DOCLIB_CONTENT = "content";
    /** Field mapping for document library ID */
    protected static final String DOCLIB_ID = "id";
    /** Field mapping for parent site name */
    protected static final String DOCLIB_SITE_NAME = "site_name";
    /** Field mapping for parent site URL */
    protected static final String DOCLIB_SITE_URL = "site_url";
    /** Field mapping for canonical URL */
    protected static final String DOCLIB_CANONICAL_URL = "url";

    /** Name of the extractor to use for file content extraction */
    protected String extractorName = "sharePointDocLibExtractor";

    /**
     * Default constructor for SharePointDocLibDataStore.
     */
    public SharePointDocLibDataStore() {
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
        configMap.put(IGNORE_FOLDER, isIgnoreFolder(paramMap));
        configMap.put(SUPPORTED_MIMETYPES, getSupportedMimeTypes(paramMap));

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "SharePoint Document Library crawling started - Configuration: MaxSize={}, IgnoreError={}, IgnoreFolder={}, MimeTypes={}, Threads={}",
                    configMap.get(MAX_CONTENT_LENGTH), configMap.get(IGNORE_ERROR), configMap.get(IGNORE_FOLDER),
                    java.util.Arrays.toString((String[]) configMap.get(SUPPORTED_MIMETYPES)), paramMap.getAsString(NUMBER_OF_THREADS, "1"));
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            final String siteId = getSiteId(paramMap);
            if (StringUtil.isNotBlank(siteId)) {
                // Crawl document libraries in specific site
                if (logger.isDebugEnabled()) {
                    logger.debug("Crawling document libraries in specific site with ID: {}", siteId);
                }
                final Site site = client.getSite(siteId);
                if (logger.isDebugEnabled()) {
                    logger.debug("Retrieved site: {} (ID: {}, WebUrl: {})", site.getDisplayName(), site.getId(), site.getWebUrl());
                }
                if (!isExcludedSite(paramMap, site)) {
                    storeDocumentLibrariesInSite(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService,
                            client, site);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Skipping excluded site: {} (ID: {})", site.getDisplayName(), site.getId());
                    }
                }
            } else {
                // Crawl document libraries in all sites
                if (logger.isDebugEnabled()) {
                    logger.debug("Crawling document libraries in all sites");
                }
                client.getSites(site -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Evaluating site: {} (ID: {}, Excluded: {})", site.getDisplayName(), site.getId(),
                                isExcludedSite(paramMap, site));
                    }

                    if (!isExcludedSite(paramMap, site)) {
                        try {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Processing document libraries in site: {} (ID: {})", site.getDisplayName(), site.getId());
                            }
                            storeDocumentLibrariesInSite(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                    executorService, client, site);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Successfully processed document libraries in site: {} (ID: {})", site.getDisplayName(),
                                        site.getId());
                            }
                        } catch (final Exception e) {
                            logger.warn("Failed to process document libraries in site: {} (ID: {})", site.getDisplayName(), site.getId(),
                                    e);
                            if (!isIgnoreError(paramMap)) {
                                throw new DataStoreCrawlingException(site.getDisplayName(),
                                        "Failed to process document libraries in site: " + site.getDisplayName(), e);
                            }
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Skipped site: {} (ID: {}) - Excluded", site.getDisplayName(), site.getId());
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
     * Stores document libraries and their files in a SharePoint site.
     *
     * @param dataConfig the data configuration
     * @param callback the index update callback for storing documents
     * @param configMap configuration map containing crawl settings
     * @param paramMap data store parameters
     * @param scriptMap script mappings for field transformation
     * @param defaultDataMap default data values for documents
     * @param executorService executor service for concurrent processing
     * @param client Microsoft 365 client for API calls
     * @param site SharePoint site to process
     */
    protected void storeDocumentLibrariesInSite(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final ExecutorService executorService, final Microsoft365Client client,
            final Site site) {

        if (logger.isDebugEnabled()) {
            logger.debug("Processing document libraries for site: {} ({})", site.getDisplayName(), site.getId());
        }

        // Get all drives (document libraries) for the site
        getSiteDrives(client, site.getId(), drive -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Evaluating drive: {} - Type: {}, System: {}", drive.getName(), drive.getDriveType(), isSystemLibrary(drive));
            }
            if ("documentLibrary".equals(drive.getDriveType()) && !(isIgnoreSystemLibraries(paramMap) && isSystemLibrary(drive))) {

                executorService.execute(() -> {
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Processing document library: {} in site: {}", drive.getName(), site.getDisplayName());
                        }

                        // Store document library metadata
                        storeDocumentLibrary(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, site, drive);

                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully processed document library: {} in site: {}", drive.getName(), site.getDisplayName());
                        }
                    } catch (final Exception e) {
                        logger.warn("Failed to process document library: {} in site: {}", drive.getName(), site.getDisplayName(), e);
                        if (!isIgnoreError(paramMap)) {
                            throw new DataStoreCrawlingException(drive.getName(), "Failed to process document library: " + drive.getName(),
                                    e);
                        }
                    }
                });
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Skipping drive: {} - Type: {}, System: {}", drive.getName(), drive.getDriveType(),
                            isSystemLibrary(drive));
                }
            }
        });
    }

    /**
     * Stores a document library as a document for indexing.
     *
     * @param dataConfig the data configuration
     * @param callback the index update callback for storing documents
     * @param configMap configuration map containing crawl settings
     * @param paramMap data store parameters
     * @param scriptMap script mappings for field transformation
     * @param defaultDataMap default data values for documents
     * @param client Microsoft 365 client for API calls
     * @param site SharePoint site containing the document library
     * @param drive document library drive to process
     */
    protected void storeDocumentLibrary(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final Microsoft365Client client, final Site site, final Drive drive) {

        final String docLibUrl = drive.getWebUrl();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final StatsKeyObject statsKey = new StatsKeyObject(docLibUrl);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);

        if (logger.isDebugEnabled()) {
            logger.debug("Processing document library - Name: {}, ID: {}, URL: {}, Created: {}, Modified: {}", drive.getName(),
                    drive.getId(), docLibUrl, drive.getCreatedDateTime(), drive.getLastModifiedDateTime());
        }

        try {
            logger.info("Crawling document library URL: {} (Name: {}) in site: {}", docLibUrl, drive.getName(), site.getDisplayName());
            crawlerStatsHelper.begin(statsKey);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> docLibMap = new HashMap<>();

            docLibMap.put(DOCLIB_ID, drive.getId());
            docLibMap.put(DOCLIB_NAME, drive.getName());
            docLibMap.put(DOCLIB_DESCRIPTION, drive.getDescription());
            docLibMap.put(DOCLIB_URL, docLibUrl); // Original Graph API webUrl
            docLibMap.put(DOCLIB_CANONICAL_URL, generateDocumentLibraryUrl(site, drive)); // Standardized SharePoint URL
            docLibMap.put(DOCLIB_CREATED, drive.getCreatedDateTime());
            docLibMap.put(DOCLIB_MODIFIED, drive.getLastModifiedDateTime());
            docLibMap.put(DOCLIB_TYPE, drive.getDriveType());
            docLibMap.put(DOCLIB_SITE_NAME, site.getDisplayName());
            docLibMap.put(DOCLIB_SITE_URL, site.getWebUrl());

            // Build content for document library
            final StringBuilder contentBuilder = new StringBuilder();
            if (StringUtil.isNotBlank(drive.getName())) {
                contentBuilder.append(drive.getName()).append(' ');
            }
            if (StringUtil.isNotBlank(drive.getDescription())) {
                contentBuilder.append(drive.getDescription()).append(' ');
            }
            if (StringUtil.isNotBlank(site.getDisplayName())) {
                contentBuilder.append(site.getDisplayName()).append(' ');
            }
            final String docLibContent = contentBuilder.toString().trim();
            docLibMap.put(DOCLIB_CONTENT, docLibContent);

            // Get permissions for document library
            final List<String> roles = getDrivePermissions(client, drive.getId());
            if (logger.isDebugEnabled()) {
                logger.debug("Initial permissions for document library {} - Count: {}, Permissions: {}", drive.getName(), roles.size(),
                        roles);
            }

            // Add default permissions
            final FessConfig fessConfig = ComponentUtil.getFessConfig();
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(roles::add));
            if (defaultDataMap.get(fessConfig.getIndexFieldRole()) instanceof List<?> roleTypeList) {
                roleTypeList.stream().map(s -> (String) s).forEach(roles::add);
            }

            final List<String> finalRoles = roles.stream().distinct().collect(Collectors.toList());
            docLibMap.put(DOCLIB_ROLES, finalRoles);

            if (logger.isDebugEnabled()) {
                logger.debug("Final permissions for document library {} - Count: {}, Permissions: {}", drive.getName(), finalRoles.size(),
                        finalRoles);
            }

            resultMap.put(DOCLIB, docLibMap);

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
                logger.debug("Storing document library data: {}", dataMap);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully indexed document library: {} (ID: {})", drive.getName(), drive.getId());
            }
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception for document library: {} (ID: {}, URL: {}) - Data: {}", drive.getName(), drive.getId(),
                    docLibUrl, dataMap, e);

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
            failureUrlService.store(dataConfig, errorName, docLibUrl, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Processing exception for document library: {} (ID: {}, URL: {}) - Data: {}", drive.getName(), drive.getId(),
                    docLibUrl, dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), docLibUrl, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Gets drives (document libraries) for a specific site.
     *
     * @param client Microsoft 365 client for API calls
     * @param siteId ID of the SharePoint site
     * @param consumer consumer to process each drive found
     */
    protected void getSiteDrives(final Microsoft365Client client, final String siteId, final Consumer<Drive> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting drives for site: {}", siteId);
        }

        try {
            // Use the general getDrives method and filter by site
            client.getSiteDrives(siteId, drive -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Found drive: {} - Type: {}, WebUrl: {}", drive.getName(), drive.getDriveType(), drive.getWebUrl());
                }
                // For now, we'll accept all document library drives
                // In a more sophisticated implementation, we could check if the drive belongs to this site
                if ("documentLibrary".equals(drive.getDriveType())) {
                    consumer.accept(drive);
                }
            });
        } catch (final Exception e) {
            logger.warn("Failed to get drives for site: {}", siteId, e);
            throw e;
        }
    }

    /**
     * Gets permissions for a document library (drive).
     *
     * @param client Microsoft 365 client for API calls
     * @param driveId ID of the document library drive
     * @return list of user emails/IDs with access permissions
     */
    protected List<String> getDrivePermissions(final Microsoft365Client client, final String driveId) {
        final List<String> permissions = new ArrayList<>();
        try {
            // Get permissions for the drive root item
            var response = client.getDrivePermissions(driveId, "root");

            // Handle pagination for permissions
            while (response != null && response.getValue() != null) {
                response.getValue().forEach(permission -> {
                    assignPermission(client, permissions, permission);
                });

                if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                    response = client.getDrivePermissionsByNextLink(driveId, "root", response.getOdataNextLink());
                } else {
                    break;
                }
            }
        } catch (final Exception e) {
            logger.warn("Failed to get permissions for drive: {}", driveId, e);
        }
        return permissions;
    }

    /**
     * Gets the user email from a permission.
     *
     * @param permission the permission object containing user information
     * @return user email or display name, or null if not found
     */
    protected String getUserEmail(final com.microsoft.graph.models.Permission permission) {
        if (permission.getGrantedToV2() != null && permission.getGrantedToV2().getUser() != null) {
            final var user = permission.getGrantedToV2().getUser();

            if (user.getId() != null && !user.getId().isEmpty()) {
                if (user.getId().contains("@")) {
                    return user.getId();
                }
            }

            if (user.getDisplayName() != null && !user.getDisplayName().isEmpty()) {
                return user.getDisplayName();
            }
        }
        return null;
    }

    /**
     * Gets permissions for a drive item (file).
     */
    protected List<String> getDriveItemPermissions(final Microsoft365Client client, final String driveId, final DriveItem item) {
        final List<String> roles = new ArrayList<>();
        try {
            var response = client.getDrivePermissions(driveId, item.getId());

            // Handle pagination for permissions
            while (response != null && response.getValue() != null) {
                response.getValue().forEach(permission -> {
                    if (permission.getGrantedToV2() != null && permission.getGrantedToV2().getUser() != null) {
                        String email = getUserEmail(permission);
                        if (StringUtil.isNotBlank(email)) {
                            roles.add(email);
                        }
                    }
                });

                if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                    response = client.getDrivePermissionsByNextLink(driveId, item.getId(), response.getOdataNextLink());
                } else {
                    break;
                }
            }
        } catch (final Exception e) {
            logger.warn("Failed to get permissions for drive item: {} in drive: {}", item.getId(), driveId, e);
        }
        return roles;
    }

    // Configuration helper methods
    /**
     * Gets the site ID from configuration parameters.
     *
     * @param paramMap data store parameters
     * @return site ID or null if not specified
     */
    protected String getSiteId(final DataStoreParams paramMap) {
        return paramMap.getAsString(SITE_ID, null);
    }

    /**
     * Checks if a site is excluded from crawling.
     *
     * @param paramMap data store parameters containing exclusion list
     * @param site SharePoint site to check
     * @return true if the site should be excluded, false otherwise
     */
    protected boolean isExcludedSite(final DataStoreParams paramMap, final Site site) {
        final String excludeIds = paramMap.getAsString(EXCLUDE_SITE_ID, null);
        if (StringUtil.isBlank(excludeIds)) {
            return false;
        }

        final String[] ids;
        if (excludeIds.contains(";")) {
            ids = excludeIds.split(";");
        } else if (excludeIds.contains(".sharepoint.com,")
                && excludeIds.matches(".*[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}.*")) {
            ids = new String[] { excludeIds };
        } else {
            ids = excludeIds.split(",");
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

        return false;
    }

    /**
     * Checks if a drive is a system library.
     *
     * @param drive document library drive to check
     * @return true if the drive is a system library, false otherwise
     */
    protected boolean isSystemLibrary(final Drive drive) {
        if (drive.getWebUrl() == null) {
            return false;
        }

        final String webUrl = drive.getWebUrl().toLowerCase();
        return webUrl.contains("/_catalogs/") || webUrl.contains("/forms/") || webUrl.contains("/style%20library/")
                || webUrl.contains("/style library/") || webUrl.contains("/formservertemplates/");
    }

    /**
     * Checks if system libraries should be ignored.
     *
     * @param paramMap data store parameters
     * @return true if system libraries should be ignored, false otherwise
     */
    protected boolean isIgnoreSystemLibraries(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_SYSTEM_LIBRARIES, Constants.TRUE));
    }

    /**
     * Checks if errors should be ignored during crawling.
     *
     * @param paramMap data store parameters
     * @return true if errors should be ignored, false otherwise
     */
    protected boolean isIgnoreError(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.FALSE));
    }

    /**
     * Checks if folder documents should be ignored.
     *
     * @param paramMap data store parameters
     * @return true if folders should be ignored, false otherwise
     */
    protected boolean isIgnoreFolder(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_FOLDER, Constants.TRUE));
    }

    /**
     * Gets the maximum content size for file extraction.
     *
     * @param paramMap data store parameters
     * @return maximum content size in bytes
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
     * Gets the array of supported MIME types.
     *
     * @param paramMap data store parameters
     * @return array of supported MIME type patterns
     */
    protected String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
        return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                .get(stream -> stream.map(String::trim).toArray(n -> new String[n]));
    }

    /**
     * Gets the contents of a drive item.
     * Reuses OneDrive pattern for content extraction.
     *
     * @param client Microsoft 365 client for API calls
     * @param driveId ID of the document library drive
     * @param item drive item to extract content from
     * @param maxContentLength maximum content length to extract
     * @param ignoreError whether to ignore extraction errors
     * @return extracted text content or empty string on error
     */
    protected String getDriveItemContents(final Microsoft365Client client, final String driveId, final DriveItem item,
            final long maxContentLength, final boolean ignoreError) {
        if (item.getFile() != null) {
            try (final var in = client.getDriveContent(driveId, item.getId())) {
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
                return "";
            }
        }
        return "";
    }

    /**
     * Generates a standardized SharePoint URL for a document library.
     *
     * @param site The SharePoint site containing the document library
     * @param drive The document library drive
     * @return A standardized SharePoint URL for the document library
     */
    protected String generateDocumentLibraryUrl(final Site site, final Drive drive) {
        final String siteUrl = site.getWebUrl();
        final String driveName = drive.getName();

        if (logger.isDebugEnabled()) {
            logger.debug("Generating canonical URL for document library - Site: {}, Drive: {}", siteUrl, driveName);
        }

        // Handle standard document libraries
        if ("Documents".equals(driveName) || "Shared Documents".equals(driveName)) {
            return siteUrl + "/Shared%20Documents";
        } else {
            // For custom document libraries, encode the name
            return siteUrl + "/" + encodeUrlComponent(driveName);
        }
    }

    /**
     * Encodes a URL component for safe use in URLs.
     *
     * @param component The component to encode
     * @return The encoded component
     */
    protected String encodeUrlComponent(final String component) {
        if (StringUtil.isEmpty(component)) {
            return component;
        }
        try {
            return java.net.URLEncoder.encode(component, "UTF-8").replace("+", "%20");
        } catch (final java.io.UnsupportedEncodingException e) {
            logger.warn("Failed to encode URL component: {}", component, e);
            return component;
        }
    }

    void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }
}
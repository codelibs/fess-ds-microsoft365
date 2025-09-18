/*
 * Copyright 2012-2024 CodeLibs Project and the Others.
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

import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.CoreLibConstants;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.crawler.helper.ContentLengthHelper;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.DriveItemCollectionResponse;
import com.microsoft.graph.models.Hashes;
import com.microsoft.kiota.ApiException;

/**
 * This class is a data store for crawling and indexing files in Microsoft OneDrive.
 * It supports crawling user drives, group drives, and shared document libraries.
 * It also handles file metadata, permissions, and content extraction.
 */
public class OneDriveDataStore extends Microsoft365DataStore {

    private static final Logger logger = LogManager.getLogger(OneDriveDataStore.class);

    /**
     * Default constructor.
     */
    public OneDriveDataStore() {
    }

    /** Default maximum size of a file to be crawled. */
    protected static final long DEFAULT_MAX_SIZE = -1L;

    /** Cache for the current user's drive ID to avoid repeated expensive API calls */
    protected volatile String cachedUserDriveId = null;
    /** Lock object for thread-safe cache initialization */
    protected final Object driveIdCacheLock = new Object();

    /** Key for the current crawler type in the configuration map. */
    protected static final String CURRENT_CRAWLER = "current_crawler";
    /** Crawler type for group drives. */
    protected static final String CRAWLER_TYPE_GROUP = "group";
    /** Crawler type for user drives. */
    protected static final String CRAWLER_TYPE_USER = "user";
    /** Crawler type for shared drives. */
    protected static final String CRAWLER_TYPE_SHARED = "shared";
    /** Crawler type for a specific drive. */
    protected static final String CRAWLER_TYPE_DRIVE = "drive";
    /** Key for drive information in the configuration map. */
    protected static final String DRIVE_INFO = "drive_info";

    // parameters
    /** Parameter name for the maximum content length. */
    protected static final String MAX_CONTENT_LENGTH = "max_content_length";
    /** Parameter name for ignoring folders. */
    protected static final String IGNORE_FOLDER = "ignore_folder";
    /** Parameter name for ignoring errors. */
    protected static final String IGNORE_ERROR = "ignore_error";
    /** Parameter name for supported MIME types. */
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    /** Parameter name for the include pattern for URLs. */
    protected static final String INCLUDE_PATTERN = "include_pattern";
    /** Parameter name for the exclude pattern for URLs. */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    /** Parameter name for the URL filter. */
    protected static final String URL_FILTER = "url_filter";
    /** Parameter name for the drive ID. */
    protected static final String DRIVE_ID = "drive_id";
    /** Parameter name for default permissions. */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** Parameter name for the number of threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** Parameter name for enabling the shared documents drive crawler. */
    protected static final String SHARED_DOCUMENTS_DRIVE_CRAWLER = "shared_documents_drive_crawler";
    /** Parameter name for enabling the user drive crawler. */
    protected static final String USER_DRIVE_CRAWLER = "user_drive_crawler";
    /** Parameter name for enabling the group drive crawler. */
    protected static final String GROUP_DRIVE_CRAWLER = "group_drive_crawler";

    // scripts
    /** Key for the file object in the script map. */
    protected static final String FILE = "file";
    /** Key for the file name in the script map. */
    protected static final String FILE_NAME = "name";
    /** Key for the file description in the script map. */
    protected static final String FILE_DESCRIPTION = "description";
    /** Key for the file contents in the script map. */
    protected static final String FILE_CONTENTS = "contents";
    /** Key for the file MIME type in the script map. */
    protected static final String FILE_MIMETYPE = "mimetype";
    /** Key for the file type in the script map. */
    protected static final String FILE_FILETYPE = "filetype";
    /** Key for the file creation date in the script map. */
    protected static final String FILE_CREATED = "created";
    /** Key for the file last modified date in the script map. */
    protected static final String FILE_LAST_MODIFIED = "last_modified";
    /** Key for the file size in the script map. */
    protected static final String FILE_SIZE = "size";
    /** Key for the file web URL in the script map. */
    protected static final String FILE_WEB_URL = "web_url";
    /** Key for the file URL in the script map. */
    protected static final String FILE_URL = "url";
    /** Key for the file roles in the script map. */
    protected static final String FILE_ROLES = "roles";
    /** Key for the file cTag in the script map. */
    protected static final String FILE_CTAG = "ctag";
    /** Key for the file eTag in the script map. */
    protected static final String FILE_ETAG = "etag";
    /** Key for the file ID in the script map. */
    protected static final String FILE_ID = "id";
    /** Key for the file WebDAV URL in the script map. */
    protected static final String FILE_WEBDAV_URL = "webdav_url";
    /** Key for the file location in the script map. */
    protected static final String FILE_LOCATION = "location";
    /** Key for the application that created the file in the script map. */
    protected static final String FILE_CREATEDBY_APPLICATION = "createdby_application";
    /** Key for the device that created the file in the script map. */
    protected static final String FILE_CREATEDBY_DEVICE = "createdby_device";
    /** Key for the user who created the file in the script map. */
    protected static final String FILE_CREATEDBY_USER = "createdby_user";
    /** Key for the deleted status of the file in the script map. */
    protected static final String FILE_DELETED = "deleted";
    /** Key for the file hashes in the script map. */
    protected static final String FILE_HASHES = "hashes";
    /** Key for the application that last modified the file in the script map. */
    protected static final String FILE_LAST_MODIFIEDBY_APPLICATION = "last_modifiedby_application";
    /** Key for the device that last modified the file in the script map. */
    protected static final String FILE_LAST_MODIFIEDBY_DEVICE = "last_modifiedby_device";
    /** Key for the user who last modified the file in the script map. */
    protected static final String FILE_LAST_MODIFIEDBY_USER = "last_modifiedby_user";
    /** Key for the file image in the script map. */
    protected static final String FILE_IMAGE = "image";
    /** Key for the file parent in the script map. */
    protected static final String FILE_PARENT = "parent";
    /** Key for the file parent ID in the script map. */
    protected static final String FILE_PARENT_ID = "parent_id";
    /** Key for the file parent name in the script map. */
    protected static final String FILE_PARENT_NAME = "parent_name";
    /** Key for the file parent path in the script map. */
    protected static final String FILE_PARENT_PATH = "parent_path";
    /** Key for the file photo in the script map. */
    protected static final String FILE_PHOTO = "photo";
    /** Key for the file publication in the script map. */
    protected static final String FILE_PUBLICATION = "publication";
    /** Key for the file search result in the script map. */
    protected static final String FILE_SEARCH_RESULT = "search_result";
    /** Key for the file special folder in the script map. */
    protected static final String FILE_SPECIAL_FOLDER = "special_folder";
    /** Key for the file video in the script map. */
    protected static final String FILE_VIDEO = "video";

    /** The name of the extractor to use for file content. */
    protected String extractorName = "tikaExtractor";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(MAX_CONTENT_LENGTH, getMaxSize(paramMap));
        configMap.put(IGNORE_FOLDER, isIgnoreFolder(paramMap));
        configMap.put(IGNORE_ERROR, isIgnoreError(paramMap));
        configMap.put(SUPPORTED_MIMETYPES, getSupportedMimeTypes(paramMap));
        configMap.put(URL_FILTER, getUrlFilter(paramMap));
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "OneDrive crawling started with configuration - MaxSize: {}, IgnoreFolder: {}, IgnoreError: {}, MimeTypes: {}, Threads: {}, IgnoreSystemLists: {}, IgnoreSystemLibraries: {}",
                    configMap.get(MAX_CONTENT_LENGTH), configMap.get(IGNORE_FOLDER), configMap.get(IGNORE_ERROR),
                    java.util.Arrays.toString((String[]) configMap.get(SUPPORTED_MIMETYPES)), paramMap.getAsString(NUMBER_OF_THREADS, "1"),
                    isIgnoreSystemLists(paramMap), isIgnoreSystemLibraries(paramMap));
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            if (isSharedDocumentsDriveCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Starting shared documents drive crawling");
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_SHARED);
                try {
                    storeSharedDocumentsDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client,
                            null);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Completed shared documents drive crawling");
                    }
                } catch (final Exception e) {
                    logger.warn("Failed to crawl shared documents drive", e);
                    throw e;
                }
            }

            if (isUserDriveCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Starting user drives crawling");
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_USER);
                try {
                    storeUsersDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Completed user drives crawling");
                    }
                } catch (final Exception e) {
                    logger.warn("Failed to crawl user drives", e);
                    throw e;
                }
            }

            if (isGroupDriveCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Starting group drives crawling");
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_GROUP);
                try {
                    storeGroupsDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Completed group drives crawling");
                    }
                } catch (final Exception e) {
                    logger.warn("Failed to crawl group drives", e);
                    throw e;
                }
            }

            final String driveId = paramMap.getAsString(DRIVE_ID);
            if (StringUtil.isNotBlank(driveId)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Starting specific drive crawling for drive ID: {}", driveId);
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_DRIVE);
                try {
                    final Drive drive = client.getDrive(driveId);
                    configMap.put(DRIVE_INFO, drive);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Retrieved drive info - Name: {}, DriveType: {}, WebUrl: {}", drive.getName(), drive.getDriveType(),
                                drive.getWebUrl());
                    }
                    storeSharedDocumentsDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client,
                            driveId);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Completed specific drive crawling for drive ID: {}", driveId);
                    }
                } catch (final Exception e) {
                    logger.warn("Failed to crawl drive with ID: {}", driveId, e);
                    throw e;
                }
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
     * Gets the URL filter from the data store parameters.
     *
     * @param paramMap The data store parameters.
     * @return The URL filter.
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
     * Checks if the shared documents drive crawler is enabled.
     *
     * @param paramMap The data store parameters.
     * @return true if the shared documents drive crawler is enabled, false otherwise.
     */
    protected boolean isSharedDocumentsDriveCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(SHARED_DOCUMENTS_DRIVE_CRAWLER, Constants.TRUE));
    }

    /**
     * Checks if the user drive crawler is enabled.
     *
     * @param paramMap The data store parameters.
     * @return true if the user drive crawler is enabled, false otherwise.
     */
    protected boolean isUserDriveCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(USER_DRIVE_CRAWLER, Constants.TRUE));
    }

    /**
     * Checks if the group drive crawler is enabled.
     *
     * @param paramMap The data store parameters.
     * @return true if the group drive crawler is enabled, false otherwise.
     */
    protected boolean isGroupDriveCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(GROUP_DRIVE_CRAWLER, Constants.TRUE));
    }

    /**
     * Checks if folders should be ignored.
     *
     * @param paramMap The data store parameters.
     * @return true if folders should be ignored, false otherwise.
     */
    protected boolean isIgnoreFolder(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_FOLDER, Constants.TRUE));
    }

    /**
     * Gets the maximum content length from the data store parameters.
     *
     * @param paramMap The data store parameters.
     * @return The maximum content length.
     */
    protected long getMaxSize(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(MAX_CONTENT_LENGTH);
        try {
            return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
        } catch (final NumberFormatException e) {
            return DEFAULT_MAX_SIZE;
        }
    }

    /**
     * Gets the supported MIME types from the data store parameters.
     *
     * @param paramMap The data store parameters.
     * @return An array of supported MIME types.
     */
    protected String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
        return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                .get(stream -> stream.map(String::trim).toArray(n -> new String[n]));
    }

    /**
     * Stores the shared documents drive.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param executorService The executor service.
     * @param client The Microsoft365Client.
     * @param driveId The drive ID.
     */
    protected void storeSharedDocumentsDrive(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final ExecutorService executorService, final Microsoft365Client client,
            final String driveId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Processing shared documents drive - requested driveId: {}", driveId);
        }

        if (driveId != null) {
            getDriveItemsInDrive(client, driveId, item -> executorService.execute(() -> processDriveItem(dataConfig, callback, configMap,
                    paramMap, scriptMap, defaultDataMap, client, driveId, item, Collections.emptyList())));
        } else {
            client.getSites(site -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Processing site - Name: {}, ID: {}, WebUrl: {}", site.getName(), site.getId(), site.getWebUrl());
                }
                try {
                    client.getSiteDrives(site.getId(), drive -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Processing drive in site {} - Name: {}, ID: {}, DriveType: {}, WebUrl: {}", site.getName(),
                                    drive.getName(), drive.getId(), drive.getDriveType(), drive.getWebUrl());
                        }
                        getDriveItemsInDrive(client, drive.getId(), item -> {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Processing drive item in drive {} - Name: {}, ID: {}, WebUrl: {}", drive.getName(),
                                        item.getName(), item.getId(), item.getWebUrl());
                            }
                            executorService.execute(() -> {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Starting to process drive item: {} - Name: {}", item.getWebUrl(), item.getName());
                                }
                                processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client,
                                        drive.getId(), item, Collections.emptyList());
                            });
                        });
                    });
                } catch (final ApiException e) {
                    logger.warn("Failed to process drive for site: {} (ID: {})", site.getName(), site.getId(), e);
                }
            });
        }
    }

    /**
     * Stores the users' drives.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param executorService The executor service.
     * @param client The Microsoft365Client.
     */
    protected void storeUsersDrive(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Microsoft365Client client) {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting user drives crawling - retrieving licensed users");
        }

        getLicensedUsers(client, user -> {
            final String userId = user.getId();
            final String displayName = user.getDisplayName();
            if (logger.isDebugEnabled()) {
                logger.debug("Processing user drive for: {} (ID: {})", displayName, userId);
            }

            try {
                final Drive userDrive = client.getUserDrive(userId);
                if (logger.isDebugEnabled()) {
                    logger.debug("Retrieved drive for user {} - Drive Name: {}, Drive ID: {}, DriveType: {}, System: {}", displayName,
                            userDrive.getName(), userDrive.getId(), userDrive.getDriveType(), isSystemLibrary(userDrive));
                }
                getDriveItemsInDrive(client, userDrive.getId(), item -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing drive item in user {}'s drive - Name: {}, ID: {}, WebUrl: {}", displayName, item.getName(),
                                item.getId(), item.getWebUrl());
                    }
                    executorService.execute(() -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Starting to process drive item: {} - Name: {}", item.getWebUrl(), item.getName());
                        }
                        processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, userDrive.getId(),
                                item, getUserRoles(user));
                    });
                });

                if (logger.isDebugEnabled()) {
                    logger.debug("Successfully initiated processing for user {}'s drive", displayName);
                }
            } catch (final ApiException e) {
                logger.warn("Failed to process drive for user: {} (ID: {})", displayName, userId, e);
            }
        });
    }

    /**
     * Checks if the current thread is interrupted.
     *
     * @param e The exception to check.
     */
    protected void isInterrupted(final Exception e) {
        if (e instanceof InterruptedException) {
            throw new InterruptedRuntimeException((InterruptedException) e);
        }
    }

    /**
     * Stores the groups' drives.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param executorService The executor service.
     * @param client The Microsoft365Client.
     */
    protected void storeGroupsDrive(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Microsoft365Client client) {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting group drives crawling - retrieving Microsoft 365 groups");
        }

        getMicrosoft365Groups(client, group -> {
            final String groupId = group.getId();
            final String displayName = group.getDisplayName();
            if (logger.isDebugEnabled()) {
                logger.debug("Processing group drive for: {} (ID: {})", displayName, groupId);
            }

            try {
                final Drive groupDrive = client.getGroupDrive(groupId);
                if (logger.isDebugEnabled()) {
                    logger.debug("Retrieved drive for group {} - Drive Name: {}, Drive ID: {}, DriveType: {}, System: {}", displayName,
                            groupDrive.getName(), groupDrive.getId(), groupDrive.getDriveType(), isSystemLibrary(groupDrive));
                }
                getDriveItemsInDrive(client, groupDrive.getId(), item -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing drive item in group {}'s drive - Name: {}, ID: {}, WebUrl: {}", displayName,
                                item.getName(), item.getId(), item.getWebUrl());
                    }
                    executorService.execute(() -> {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Starting to process drive item: {} - Name: {}", item.getWebUrl(), item.getName());
                        }
                        processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, groupDrive.getId(),
                                item, getGroupRoles(group));
                    });
                });

                if (logger.isDebugEnabled()) {
                    logger.debug("Successfully initiated processing for group {}'s drive", displayName);
                }
            } catch (final ApiException e) {
                logger.warn("Failed to process drive for group: {} (ID: {})", displayName, groupId, e);
            }
        });
    }

    /**
     * Processes a drive item.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The Microsoft365Client.
     * @param driveId The drive ID.
     * @param item The drive item.
     * @param roles The roles.
     */
    protected void processDriveItem(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final String driveId, final DriveItem item, final List<String> roles) {
        final boolean isFolder = item.getFolder() != null;
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        final String mimetype;
        final Hashes hashes;
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final StatsKeyObject statsKey = new StatsKeyObject(item.getWebUrl());
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            crawlerStatsHelper.begin(statsKey);
            if (((Boolean) configMap.get(IGNORE_FOLDER)).booleanValue() && isFolder) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Ignoring folder item (IGNORE_FOLDER=true): {} - Name: {}", item.getWebUrl(), item.getName());
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            if (item.getFile() != null) {
                mimetype = item.getFile().getMimeType();
                hashes = item.getFile().getHashes();
            } else {
                mimetype = "application/octet-stream";
                hashes = null;
            }

            final String[] supportedMimeTypes = (String[]) configMap.get(SUPPORTED_MIMETYPES);
            if (!Stream.of(supportedMimeTypes).anyMatch(mimetype::matches)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Mimetype {} not supported for indexing - Item: {} ({})", mimetype, item.getName(), item.getWebUrl());
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final String url = getUrl(configMap, paramMap, item);
            final UrlFilter urlFilter = (UrlFilter) configMap.get(URL_FILTER);
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("URL filter rejected item: {} - Original URL: {}", url, item.getWebUrl());
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final Long size = item.getSize();
            logger.info("Crawling OneDrive item - URL: {}, Name: {}, Size: {} bytes, MimeType: {}", url, item.getName(), size, mimetype);

            final boolean ignoreError = (Boolean) configMap.get(IGNORE_ERROR);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> filesMap = new HashMap<>();

            long maxContentLength = (Long) configMap.get(MAX_CONTENT_LENGTH);
            if (maxContentLength < 0) {
                try {
                    final ContentLengthHelper contentLengthHelper = ComponentUtil.getComponent("contentLengthHelper");
                    maxContentLength = contentLengthHelper.getMaxLength(mimetype);
                } catch (final Exception e) {
                    logger.warn("Failed to get maxContentLength.", e);
                }
            }
            if (maxContentLength >= 0 && size != null && size.longValue() > maxContentLength) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Content length exceeded for item: {} - Size: {} bytes, Max: {} bytes", item.getName(), size,
                            maxContentLength);
                }
                throw new MaxLengthExceededException(
                        "The content length (" + size + " byte) is over " + maxContentLength + " byte. The url is " + item.getWebUrl());
            }

            final String filetype = ComponentUtil.getFileTypeHelper().get(mimetype);
            filesMap.put(FILE_NAME, item.getName());
            filesMap.put(FILE_DESCRIPTION, item.getDescription() != null ? item.getDescription() : StringUtil.EMPTY);

            filesMap.put(FILE_CONTENTS, getDriveItemContents(client, driveId, item, maxContentLength, ignoreError));
            filesMap.put(FILE_MIMETYPE, mimetype);
            filesMap.put(FILE_FILETYPE, filetype);
            filesMap.put(FILE_CREATED, item.getCreatedDateTime());
            filesMap.put(FILE_LAST_MODIFIED, item.getLastModifiedDateTime());
            filesMap.put(FILE_SIZE, size);
            filesMap.put(FILE_WEB_URL, item.getWebUrl());
            filesMap.put(FILE_URL, url);
            filesMap.put(FILE_CTAG, item.getCTag());
            filesMap.put(FILE_ETAG, item.getETag());
            filesMap.put(FILE_ID, item.getId());
            filesMap.put(FILE_WEBDAV_URL, item.getWebDavUrl());
            filesMap.put(FILE_LOCATION, item.getLocation());
            filesMap.put(FILE_CREATEDBY_APPLICATION, item.getCreatedBy() != null ? item.getCreatedBy().getApplication() : null);
            filesMap.put(FILE_CREATEDBY_DEVICE, item.getCreatedBy() != null ? item.getCreatedBy().getDevice() : null);
            filesMap.put(FILE_CREATEDBY_USER, item.getCreatedBy() != null ? item.getCreatedBy().getUser() : null);
            filesMap.put(FILE_DELETED, item.getDeleted());
            filesMap.put(FILE_HASHES, hashes);
            filesMap.put(FILE_LAST_MODIFIEDBY_APPLICATION,
                    item.getLastModifiedBy() != null ? item.getLastModifiedBy().getApplication() : null);
            filesMap.put(FILE_LAST_MODIFIEDBY_DEVICE, item.getLastModifiedBy() != null ? item.getLastModifiedBy().getDevice() : null);
            filesMap.put(FILE_LAST_MODIFIEDBY_USER, item.getLastModifiedBy() != null ? item.getLastModifiedBy().getUser() : null);
            filesMap.put(FILE_IMAGE, item.getImage());
            filesMap.put(FILE_PARENT, item.getParentReference());
            filesMap.put(FILE_PARENT_ID, item.getParentReference() != null ? item.getParentReference().getId() : null);
            filesMap.put(FILE_PARENT_NAME, item.getParentReference() != null ? item.getParentReference().getName() : null);
            filesMap.put(FILE_PARENT_PATH, item.getParentReference() != null ? item.getParentReference().getPath() : null);
            filesMap.put(FILE_PHOTO, item.getPhoto());
            filesMap.put(FILE_PUBLICATION, item.getPublication());
            filesMap.put(FILE_SEARCH_RESULT, item.getSearchResult());
            filesMap.put(FILE_SPECIAL_FOLDER, item.getSpecialFolder() != null ? item.getSpecialFolder().getName() : null);
            filesMap.put(FILE_VIDEO, item.getVideo());

            final List<String> fileRoles = getDriveItemPermissions(client, driveId, item);
            roles.forEach(fileRoles::add);
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(fileRoles::add));
            if (defaultDataMap.get(fessConfig.getIndexFieldRole()) instanceof final List<?> roleTypeList) {
                roleTypeList.stream().map(s -> (String) s).forEach(fileRoles::add);
            }
            filesMap.put(FILE_ROLES, fileRoles.stream().distinct().collect(Collectors.toList()));

            resultMap.put(FILE, filesMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("Prepared file data for indexing - Name: {}, Size: {} bytes, Permissions count: {}, URL: {}",
                        filesMap.get(FILE_NAME), filesMap.get(FILE_SIZE),
                        filesMap.get(FILE_ROLES) instanceof List ? ((List<?>) filesMap.get(FILE_ROLES)).size() : 0, filesMap.get(FILE_URL));
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
                logger.debug("Final data map prepared for indexing - Fields count: {}, URL: {}", dataMap.size(), dataMap.get("url"));
            }

            if (dataMap.get("url") instanceof final String statsUrl) {
                statsKey.setUrl(statsUrl);
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
            failureUrlService.store(dataConfig, errorName, item.getWebUrl(), target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), item.getWebUrl(), t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Gets the URL for a drive item.
     *
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param item The drive item.
     * @return The URL.
     */
    protected String getUrl(final Map<String, Object> configMap, final DataStoreParams paramMap, final DriveItem item) {
        if (item.getWebUrl() == null) {
            return null;
        }
        if (!item.getWebUrl().contains("/_layouts/")) {
            return item.getWebUrl();
        }

        final String baseUrl = item.getWebUrl().substring(0, item.getWebUrl().indexOf("/_layouts/"));
        final List<String> pathList = new ArrayList<>();
        if (item.getParentReference() != null && item.getParentReference().getPath() != null) {
            final String[] values = item.getParentReference().getPath().split(":", 2);
            if (values.length == 2) {
                for (final String s : values[1].split("/")) {
                    pathList.add(encodeUrl(s));
                }
            }
        }
        pathList.add(encodeUrl(item.getName()));
        final String path = pathList.stream().filter(StringUtil::isNotBlank).collect(Collectors.joining("/"));
        if (CRAWLER_TYPE_SHARED.equals(configMap.get(CURRENT_CRAWLER)) || CRAWLER_TYPE_GROUP.equals(configMap.get(CURRENT_CRAWLER))) {
            return baseUrl + "/Shared%20Documents/" + path;
        }
        if (CRAWLER_TYPE_DRIVE.equals(configMap.get(CURRENT_CRAWLER))) {
            final Drive drive = (Drive) configMap.get(DRIVE_INFO);
            return baseUrl + "/" + drive.getName() + "/" + path;
        }
        return baseUrl + "/Documents/" + path;
    }

    /**
     * Encodes a URL string.
     *
     * @param s The string to encode.
     * @return The encoded string.
     */
    protected String encodeUrl(final String s) {
        if (StringUtil.isEmpty(s)) {
            return s;
        }
        try {
            return URLEncoder.encode(s, CoreLibConstants.UTF_8).replace("+", "%20");
        } catch (final UnsupportedEncodingException e) {
            // ignore
            return s;
        }
    }

    /**
     * Gets the contents of a drive item.
     *
     * @param client The Microsoft365Client.
     * @param driveId The drive ID.
     * @param item The drive item.
     * @param maxContentLength The maximum content length.
     * @param ignoreError true to ignore errors.
     * @return The contents of the drive item.
     */
    protected String getDriveItemContents(final Microsoft365Client client, final String driveId, final DriveItem item,
            final long maxContentLength, final boolean ignoreError) {
        // Only process real DriveItems with file content
        if (item.getFile() != null) {
            try (final InputStream in = client.getDriveContent(driveId, item.getId())) {
                return ComponentUtil.getExtractorFactory().builder(in, Collections.emptyMap()).filename(item.getName())
                        .maxContentLength(maxContentLength).extractorName(extractorName).extract().getContent();
            } catch (final Exception e) {
                if (!ignoreError && !ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                    throw new DataStoreCrawlingException(item.getWebUrl(), "Failed to get contents: " + item.getName(), e);
                }
                if (logger.isDebugEnabled()) {
                    logger.warn("Failed to get contents: {}", item.getName(), e);
                } else {
                    logger.warn("Failed to get contents: {}. {}", item.getName(), e.getMessage());
                }
            }
        }
        return StringUtil.EMPTY;
    }

    /**
     * Gets the drive items in a drive.
     *
     * @param client The Microsoft365Client.
     * @param driveId The drive ID.
     * @param consumer The consumer to process each drive item.
     */
    protected void getDriveItemsInDrive(final Microsoft365Client client, final String driveId, final Consumer<DriveItem> consumer) {
        getDriveItemChildren(client, driveId, consumer, null);
    }

    /**
     * Gets the children of a drive item.
     *
     * @param client The Microsoft365Client.
     * @param driveId The drive ID.
     * @param consumer The consumer to process each drive item.
     * @param item The drive item.
     */
    protected void getDriveItemChildren(final Microsoft365Client client, final String driveId, final Consumer<DriveItem> consumer,
            final DriveItem item) {
        if (logger.isDebugEnabled()) {
            if (item != null) {
                logger.debug("Processing drive item - Name: {}, Type: {}, URL: {}", item.getName(),
                        item.getFolder() != null ? "folder" : "file", item.getWebUrl());
            } else {
                logger.debug("Processing root drive items for drive: {}", driveId);
            }
        }
        DriveItemCollectionResponse response;
        try {
            if (item != null) {
                consumer.accept(item);
                if (item.getFolder() == null) {
                    return;
                }
            }
            response = client.getDriveItemPage(driveId, item != null ? item.getId() : null);

            // Handle pagination with odata.nextLink
            while (response != null && response.getValue() != null) {
                response.getValue().forEach(child -> getDriveItemChildren(client, driveId, consumer, child));

                // Check if there's a next page
                if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
                    // No more pages, exit loop
                    break;
                }
                // Request the next page using a helper method in Microsoft365Client
                final String itemIdToUse = item != null ? item.getId() : "root";
                try {
                    response = client.getDriveItemsByNextLink(driveId, itemIdToUse, response.getOdataNextLink());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Retrieved next page of drive items for drive: {}, item: {}", driveId, itemIdToUse);
                    }
                } catch (final Exception e) {
                    logger.warn("Failed to get next page of drive items for drive: {}, item: {} - {}", driveId, itemIdToUse,
                            e.getMessage());
                    break;
                }
            }
        } catch (final ApiException e) {
            if (e.getResponseStatusCode() == 404) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Drive item not found (404) - Drive: {}, Item: {}", driveId, item != null ? item.getName() : "root", e);
                }
            } else {
                logger.warn("Failed to access drive item - Drive: {}, Item: {}, Status: {}", driveId,
                        item != null ? item.getName() : "root", e.getResponseStatusCode(), e);
            }
        }
    }

    /**
     * Gets the current user's drive ID with caching to avoid expensive repeated API calls.
     * Thread-safe implementation using double-checked locking pattern.
     *
     * @param client The Microsoft365Client to use for API calls.
     * @return The cached user drive ID, or null if unable to retrieve.
     */
    protected String getCachedUserDriveId(final Microsoft365Client client) {
        // Double-checked locking pattern for thread-safe lazy initialization
        if (cachedUserDriveId == null) {
            synchronized (driveIdCacheLock) {
                if (cachedUserDriveId == null) {
                    try {
                        // Make the expensive API call only once
                        cachedUserDriveId = client.getDrive(null).getId();
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully cached user drive ID: {}", cachedUserDriveId);
                        }
                    } catch (final Exception e) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Exception occurred while retrieving user drive ID", e);
                        } else {
                            logger.warn("Exception occurred while retrieving user drive ID: {}", e.getMessage());
                        }
                        return null;
                    }
                }
            }
        }
        return cachedUserDriveId;
    }

    /**
     * Sets the name of the extractor to use for file content.
     *
     * @param extractorName The name of the extractor.
     */
    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }

}

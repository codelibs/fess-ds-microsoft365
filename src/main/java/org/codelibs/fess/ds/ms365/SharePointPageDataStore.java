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

import com.microsoft.graph.models.BaseSitePage;
import com.microsoft.graph.models.CanvasLayout;
import com.microsoft.graph.models.HorizontalSection;
import com.microsoft.graph.models.HorizontalSectionColumn;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.models.SitePage;
import com.microsoft.graph.models.StandardWebPart;
import com.microsoft.graph.models.TextWebPart;
import com.microsoft.graph.models.VerticalSection;
import com.microsoft.graph.models.WebPart;

/**
 * SharePointPageDataStore crawls SharePoint pages (including news, wiki, and article pages).
 * It extracts page content, metadata, and permissions for indexing in Fess.
 *
 * @author shinsuke
 */
public class SharePointPageDataStore extends Microsoft365DataStore {

    private static final Logger logger = LogManager.getLogger(SharePointPageDataStore.class);

    // Configuration parameters
    /** Site ID parameter name for specifying which SharePoint site to crawl */
    protected static final String SITE_ID = "site_id";
    /** Comma-separated list of site IDs to exclude from crawling */
    protected static final String EXCLUDE_SITE_ID = "exclude_site_id";
    /** Flag to ignore system pages */
    protected static final String IGNORE_SYSTEM_PAGES = "ignore_system_pages";
    /** Number of concurrent threads for processing */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** Default permissions to assign to crawled pages */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** Flag to continue crawling on errors */
    protected static final String IGNORE_ERROR = "ignore_error";
    /** Regular expression pattern for pages to include */
    protected static final String INCLUDE_PATTERN = "include_pattern";
    /** Regular expression pattern for pages to exclude */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    /** Page type filter (news, wiki, article) */
    protected static final String PAGE_TYPE_FILTER = "page_type_filter";

    // Field mappings for pages
    /** Page prefix for field mappings */
    protected static final String PAGE = "page";
    /** Field mapping for page title */
    protected static final String PAGE_TITLE = "title";
    /** Field mapping for page content */
    protected static final String PAGE_CONTENT = "content";
    /** Field mapping for page web URL */
    protected static final String PAGE_URL = "web_url";
    /** Field mapping for page creation date */
    protected static final String PAGE_CREATED = "created";
    /** Field mapping for page modification date */
    protected static final String PAGE_MODIFIED = "modified";
    /** Field mapping for page author */
    protected static final String PAGE_AUTHOR = "author";
    /** Field mapping for page type */
    protected static final String PAGE_TYPE = "type";
    /** Field mapping for page access roles */
    protected static final String PAGE_ROLES = "roles";
    /** Field mapping for page ID */
    protected static final String PAGE_ID = "id";
    /** Field mapping for page description */
    protected static final String PAGE_DESCRIPTION = "description";
    /** Field mapping for parent site name */
    protected static final String PAGE_SITE_NAME = "site_name";
    /** Field mapping for parent site URL */
    protected static final String PAGE_SITE_URL = "site_url";
    /** Field mapping for canonical URL */
    protected static final String PAGE_CANONICAL_URL = "url";
    /** Field mapping for page promotion state */
    protected static final String PAGE_PROMOTION_STATE = "promotion_state";

    /** Name of the extractor to use for page content extraction */
    protected String extractorName = "sharePointPageExtractor";

    /**
     * Default constructor for SharePointPageDataStore.
     */
    public SharePointPageDataStore() {
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
        configMap.put(IGNORE_SYSTEM_PAGES, isIgnoreSystemPages(paramMap));

        if (logger.isDebugEnabled()) {
            logger.debug("SharePoint Pages crawling started - Configuration: IgnoreError={}, IgnoreSystemPages={}, Threads={}",
                    configMap.get(IGNORE_ERROR), configMap.get(IGNORE_SYSTEM_PAGES), paramMap.getAsString(NUMBER_OF_THREADS, "1"));
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            final String siteId = getSiteId(paramMap);
            if (StringUtil.isNotBlank(siteId)) {
                // Crawl pages in specific site
                if (logger.isDebugEnabled()) {
                    logger.debug("Crawling pages in specific site with ID: {}", siteId);
                }
                final Site site = client.getSite(siteId);
                if (logger.isDebugEnabled()) {
                    logger.debug("Retrieved site: {} (ID: {}, WebUrl: {})", site.getDisplayName(), site.getId(), site.getWebUrl());
                }
                if (!isExcludedSite(paramMap, site)) {
                    storePagesInSite(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client, site);
                } else if (logger.isDebugEnabled()) {
                    logger.debug("Skipping excluded site: {} (ID: {})", site.getDisplayName(), site.getId());
                }
            } else {
                // Crawl pages in all sites
                if (logger.isDebugEnabled()) {
                    logger.debug("Crawling pages in all sites");
                }
                client.getSites(site -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Evaluating site: {} (ID: {}, Excluded: {})", site.getDisplayName(), site.getId(),
                                isExcludedSite(paramMap, site));
                    }

                    if (!isExcludedSite(paramMap, site)) {
                        try {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Processing pages in site: {} (ID: {})", site.getDisplayName(), site.getId());
                            }
                            storePagesInSite(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client,
                                    site);
                            if (logger.isDebugEnabled()) {
                                logger.debug("Successfully processed pages in site: {} (ID: {})", site.getDisplayName(), site.getId());
                            }
                        } catch (final Exception e) {
                            logger.warn("Failed to process pages in site: {} (ID: {})", site.getDisplayName(), site.getId(), e);
                            if (!isIgnoreError(paramMap)) {
                                throw new DataStoreCrawlingException(site.getDisplayName(),
                                        "Failed to process pages in site: " + site.getDisplayName(), e);
                            }
                        }
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("Skipped site: {} (ID: {}) - Excluded", site.getDisplayName(), site.getId());
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
     * Stores all pages in the specified SharePoint site.
     *
     * @param dataConfig the data configuration
     * @param callback the index update callback
     * @param configMap configuration parameters map
     * @param paramMap data store parameters
     * @param scriptMap script parameters map
     * @param defaultDataMap default data values map
     * @param executorService executor service for concurrent processing
     * @param client Microsoft 365 client instance
     * @param site SharePoint site to crawl
     */
    protected void storePagesInSite(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Microsoft365Client client, final Site site) {

        if (logger.isDebugEnabled()) {
            logger.debug("Getting pages for site: {} (ID: {})", site.getDisplayName(), site.getId());
        }

        final Pattern includePattern = getPattern(paramMap, INCLUDE_PATTERN);
        final Pattern excludePattern = getPattern(paramMap, EXCLUDE_PATTERN);

        client.getSitePages(site.getId(), page -> {
            if (isTargetPage(paramMap, page, includePattern, excludePattern)) {
                executorService.execute(() -> {
                    try {
                        processPage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client, site, page);
                    } catch (final Exception e) {
                        if (!isIgnoreError(paramMap)) {
                            throw new DataStoreCrawlingException(page.getTitle(),
                                    "Failed to process page: " + page.getTitle() + " in site: " + site.getDisplayName(), e);
                        }
                        logger.warn("Failed to process page: {} in site: {}", page.getTitle(), site.getDisplayName(), e);
                    }
                });
            } else if (logger.isDebugEnabled()) {
                logger.debug("Skipping page: {} - Does not match filter criteria", page.getTitle());
            }
        });
    }

    /**
     * Processes an individual SharePoint page and stores it in the index.
     *
     * @param dataConfig the data configuration
     * @param callback the index update callback
     * @param configMap configuration parameters map
     * @param paramMap data store parameters
     * @param scriptMap script parameters map
     * @param defaultDataMap default data values map
     * @param client Microsoft 365 client instance
     * @param site SharePoint site containing the page
     * @param page SharePoint page to process
     */
    protected void processPage(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final Site site, final BaseSitePage page) {

        final String pageUrl = page.getWebUrl();

        if (logger.isDebugEnabled()) {
            logger.debug("Processing page: {} (ID: {}) in site: {} - URL: {}", page.getTitle(), page.getId(), site.getDisplayName(),
                    pageUrl);
        }

        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);

        final StatsKeyObject statsKey = new StatsKeyObject(pageUrl);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);

        try {
            crawlerStatsHelper.begin(statsKey);

            logger.info("Crawling page ID: {}, site ID: {}, URL: {}", page.getId(), site.getId(), pageUrl);

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> pageMap = new HashMap<>();
            final Map<String, Object> siteMap = new HashMap<>();

            // Get page with full content (including canvasLayout)
            final BaseSitePage fullPage = client.getPageWithContent(site.getId(), page.getId());

            // Add site-specific fields
            siteMap.put(PAGE_SITE_NAME, site.getDisplayName());
            siteMap.put(PAGE_SITE_URL, site.getWebUrl());
            siteMap.put("id", site.getId());

            pageMap.put("site", siteMap);

            // Add page fields
            pageMap.put(PAGE_ID, fullPage.getId());
            pageMap.put(PAGE_TITLE, fullPage.getTitle());
            pageMap.put(PAGE_URL, pageUrl);
            pageMap.put(PAGE_CANONICAL_URL, pageUrl);

            // Page type and promotion state
            final String pageType = determinePageType(fullPage);
            pageMap.put(PAGE_TYPE, pageType);

            if ((fullPage instanceof final SitePage sitePage) && (sitePage.getPromotionKind() != null)) {
                pageMap.put(PAGE_PROMOTION_STATE, sitePage.getPromotionKind().toString());
            }

            // Description
            pageMap.put(PAGE_DESCRIPTION, fullPage.getDescription() != null ? fullPage.getDescription() : StringUtil.EMPTY);

            // Timestamps
            if (fullPage.getCreatedDateTime() != null) {
                pageMap.put(PAGE_CREATED, fullPage.getCreatedDateTime());
            }
            if (fullPage.getLastModifiedDateTime() != null) {
                pageMap.put(PAGE_MODIFIED, fullPage.getLastModifiedDateTime());
            }

            // Author information
            if (fullPage.getCreatedByUser() != null && fullPage.getCreatedByUser().getDisplayName() != null) {
                pageMap.put(PAGE_AUTHOR, fullPage.getCreatedByUser().getDisplayName());
            }

            // Content extracted from canvasLayout
            final String pageContent = extractPageContent(fullPage);
            pageMap.put(PAGE_CONTENT, pageContent);

            if (logger.isDebugEnabled()) {
                logger.debug("Basic metadata prepared for page {} - Site: {}, Type: {}, Content length: {}", fullPage.getId(),
                        site.getDisplayName(), pageType, pageContent.length());
            }

            // Handle permissions
            final List<String> permissions = getPagePermissions(client, site.getId(), fullPage.getId(), paramMap);
            if (logger.isDebugEnabled()) {
                logger.debug("Initial permissions for page {} - Count: {}, Permissions: {}", fullPage.getId(), permissions.size(),
                        permissions);
            }

            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            if (defaultDataMap.get(fessConfig.getIndexFieldRole()) instanceof final List<?> roleTypeList) {
                roleTypeList.stream().map(s -> (String) s).forEach(permissions::add);
            }

            final List<String> finalPermissions = permissions.stream().distinct().collect(Collectors.toList());
            if (logger.isDebugEnabled()) {
                logger.debug("Final permissions for page {} - Count: {}, Permissions: {}", fullPage.getId(), finalPermissions.size(),
                        finalPermissions);
            }
            pageMap.put(PAGE_ROLES, finalPermissions);

            resultMap.put(PAGE, pageMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("Page map prepared for processing - Page: {}, Fields: {}, Permissions: {}, URL: {}", fullPage.getId(),
                        pageMap.size(), finalPermissions.size(), pageUrl);
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
                logger.debug("Successfully indexed page: {} (ID: {}, Site: {})", pageUrl, fullPage.getId(), site.getDisplayName());
            }

        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception for page: {} (ID: {}) in site: {} - Data: {}", pageUrl, page.getId(),
                    site.getDisplayName(), dataMap, e);

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
            failureUrlService.store(dataConfig, errorName, pageUrl, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Processing exception for page: {} (ID: {}) in site: {} - Data: {}", pageUrl, page.getId(), site.getDisplayName(),
                    dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), pageUrl, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Extracts text content from a SharePoint page's canvas layout.
     *
     * @param page the SharePoint page to extract content from
     * @return extracted text content as a string
     */
    protected String extractPageContent(final BaseSitePage page) {
        final StringBuilder content = new StringBuilder();

        if (logger.isDebugEnabled()) {
            logger.debug("Extracting content from page: {} (ID: {}, Type: {})", page.getTitle(), page.getId(),
                    page.getClass().getSimpleName());
        }

        // Add title and description
        if (page.getTitle() != null) {
            content.append(page.getTitle()).append("\n\n");
            if (logger.isDebugEnabled()) {
                logger.debug("Added title to content: {}", page.getTitle());
            }
        }
        if (page.getDescription() != null) {
            content.append(page.getDescription()).append("\n\n");
            if (logger.isDebugEnabled()) {
                logger.debug("Added description to content: {}", page.getDescription());
            }
        }

        // Extract content from canvasLayout (if available in specific implementations)
        CanvasLayout layout = null;
        if (page instanceof final SitePage sitePage) {
            layout = sitePage.getCanvasLayout();
            if (logger.isDebugEnabled()) {
                logger.debug("Retrieved canvas layout for SitePage: {}", layout != null);
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("Page is not a SitePage, cannot access canvas layout: {}", page.getClass().getSimpleName());
        }

        if (layout != null) {
            int webPartCount = 0;

            // Process horizontal sections
            if (layout.getHorizontalSections() != null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Processing {} horizontal sections", layout.getHorizontalSections().size());
                }
                for (final HorizontalSection section : layout.getHorizontalSections()) {
                    if (section.getColumns() != null) {
                        for (final HorizontalSectionColumn column : section.getColumns()) {
                            if (column.getWebparts() != null) {
                                for (final WebPart webpart : column.getWebparts()) {
                                    extractWebPartContent(webpart, content);
                                    webPartCount++;
                                }
                            }
                        }
                    }
                }
            }

            // Process vertical section
            if (layout.getVerticalSection() != null) {
                final VerticalSection vSection = layout.getVerticalSection();
                if (vSection.getWebparts() != null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing {} web parts in vertical section", vSection.getWebparts().size());
                    }
                    for (final WebPart webpart : vSection.getWebparts()) {
                        extractWebPartContent(webpart, content);
                        webPartCount++;
                    }
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Processed {} web parts from canvas layout", webPartCount);
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("No canvas layout available for page: {}", page.getId());
        }

        final String result = content.toString().trim();
        if (logger.isDebugEnabled()) {
            logger.debug("Extracted content length: {} characters for page: {}", result.length(), page.getId());
        }

        return result;
    }

    /**
     * Extracts content from a web part.
     *
     * @param webpart the web part to extract content from
     * @param content StringBuilder to append extracted content to
     */
    protected void extractWebPartContent(final WebPart webpart, final StringBuilder content) {
        if (webpart == null) {
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Processing web part type: {}", webpart.getClass().getSimpleName());
        }

        if (webpart instanceof final TextWebPart textPart) {
            if (textPart.getInnerHtml() != null) {
                // Improved HTML tag removal and text cleanup
                final String text = textPart.getInnerHtml()
                        .replaceAll("(?i)<br[^>]*>", "\n") // Convert <br> to newlines
                        .replaceAll("(?i)<p[^>]*>", "\n") // Convert <p> to newlines
                        .replaceAll("(?i)</p>", "\n") // Convert </p> to newlines
                        .replaceAll("<[^>]+>", " ") // Remove remaining HTML tags
                        .replace("&nbsp;", " ") // Replace &nbsp; with spaces
                        .replace("&lt;", "<") // HTML entity decoding
                        .replace("&gt;", ">")
                        .replace("&amp;", "&")
                        .replaceAll("\\s+", " ") // Normalize whitespace
                        .trim();

                if (!text.isEmpty() && text.length() > 2) {
                    content.append(text).append("\n\n");
                    if (logger.isDebugEnabled()) {
                        logger.debug("Extracted text from TextWebPart: {} characters", text.length());
                    }
                }
            }
        } else if (webpart instanceof final StandardWebPart stdPart) {
            if (stdPart.getData() != null) {
                final int beforeLength = content.length();
                extractDataFromObject(stdPart.getData(), content);
                if (logger.isDebugEnabled()) {
                    logger.debug("Extracted from StandardWebPart: {} characters", content.length() - beforeLength);
                }
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("Unsupported web part type: {}", webpart.getClass().getSimpleName());
        }
    }

    /**
     * Recursively extracts text content from web part data.
     *
     * @param data the data object to extract content from
     * @param content StringBuilder to append extracted content to
     */
    protected void extractDataFromObject(final Object data, final StringBuilder content) {
        if (data == null) {
            return;
        }

        if (data instanceof Map) {
            final Map<?, ?> map = (Map<?, ?>) data;
            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                final Object key = entry.getKey();
                final Object value = entry.getValue();
                if (logger.isDebugEnabled()) {
                    logger.debug("Extracting key: {} with value: {}", key, value);
                }

                if (value instanceof String) {
                    final String text = ((String) value).trim();
                    // Filter based on key names to avoid extracting IDs, GUIDs, or metadata
                    if (!text.isEmpty() && text.length() > 5 && !isGuidOrId(text)) {

                        // Clean up HTML entities and tags if present
                        final String cleanText = text.replace("&nbsp;", " ")
                                .replace("&lt;", "<")
                                .replace("&gt;", ">")
                                .replace("&amp;", "&")
                                .replaceAll("<[^>]+>", " ")
                                .replaceAll("\\s+", " ")
                                .trim();

                        if (cleanText.length() > 5) {
                            content.append(cleanText).append(" ");
                        }
                    }
                } else if (value instanceof Map || value instanceof List) {
                    extractDataFromObject(value, content);
                }
            }
        } else if (data instanceof List) {
            final List<?> list = (List<?>) data;
            for (final Object item : list) {
                extractDataFromObject(item, content);
            }
        } else if (data instanceof String) {
            final String text = ((String) data).trim();
            if (!text.isEmpty() && text.length() > 5 && !isGuidOrId(text)) {
                final String cleanText = text.replace("&nbsp;", " ")
                        .replace("&lt;", "<")
                        .replace("&gt;", ">")
                        .replace("&amp;", "&")
                        .replaceAll("<[^>]+>", " ")
                        .replaceAll("\\s+", " ")
                        .trim();

                if (cleanText.length() > 5) {
                    content.append(cleanText).append(" ");
                }
            }
        }
    }

    /**
     * Checks if a string appears to be a GUID or ID.
     *
     * @param text the text to check
     * @return true if the text appears to be a GUID or ID, false otherwise
     */
    protected boolean isGuidOrId(final String text) {
        if (StringUtil.isBlank(text)) {
            return false;
        }

        // Check for GUID pattern

        // Check for numeric IDs
        // Check for short alphanumeric IDs
        if (text.matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}") || text.matches("\\d+")
                || text.length() < 10 && text.matches("[a-zA-Z0-9]+")) {
            return true;
        }

        return false;
    }

    /**
     * Gets permissions for a specific page.
     *
     * @param client Microsoft 365 client instance
     * @param siteId SharePoint site ID
     * @param pageId SharePoint page ID
     * @param paramMap data store parameters
     * @return list of permission strings for the page
     */
    protected List<String> getPagePermissions(final Microsoft365Client client, final String siteId, final String pageId,
            final DataStoreParams paramMap) {
        final List<String> permissions = new ArrayList<>();

        try {
            // Get page-specific permissions
            final List<String> pagePerms = client.getPagePermissions(siteId, pageId);
            if (pagePerms != null && !pagePerms.isEmpty()) {
                permissions.addAll(pagePerms);
            }
        } catch (final Exception e) {
            logger.debug("Could not get page-specific permissions, using site permissions: {}", e.getMessage());
            // Fall back to site permissions
            try {
                permissions.addAll(getSitePermissions(client, siteId));
            } catch (final Exception ex) {
                logger.warn("Failed to get site permissions: {}", ex.getMessage());
            }
        }

        // Add default permissions if configured
        final String defaultPerms = paramMap.getAsString(DEFAULT_PERMISSIONS);
        if (StringUtil.isNotBlank(defaultPerms)) {
            permissions.addAll(StreamUtil.split(defaultPerms, ",")
                    .get(stream -> stream.map(String::trim).filter(StringUtil::isNotBlank).collect(Collectors.toList())));
        }

        return permissions.stream().distinct().collect(Collectors.toList());
    }

    /**
     * Determines the type of the page (news, wiki, article, etc.).
     *
     * @param page the SharePoint page to determine type for
     * @return the page type as a string
     */
    protected String determinePageType(final BaseSitePage page) {
        if (page instanceof final SitePage sitePage) {
            // Check if it's a news post
            if (sitePage.getPromotionKind() != null && "newsPost".equalsIgnoreCase(sitePage.getPromotionKind().toString())) {
                return "news";
            }
            // For now, assume it's an article since page layout type is not readily available
            return "article";
        }
        return "page"; // Default type
    }

    /**
     * Checks if a page should be processed based on filter criteria.
     *
     * @param paramMap data store parameters
     * @param page the SharePoint page to check
     * @param includePattern pattern for including pages
     * @param excludePattern pattern for excluding pages
     * @return true if the page should be processed, false otherwise
     */
    protected boolean isTargetPage(final DataStoreParams paramMap, final BaseSitePage page, final Pattern includePattern,
            final Pattern excludePattern) {

        // Check if it's a system page
        if (isIgnoreSystemPages(paramMap) && isSystemPage(page)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping system page: {}", page.getWebUrl());
            }
            return false;
        }

        // Check page type filter
        final String pageTypeFilter = paramMap.getAsString(PAGE_TYPE_FILTER);
        if (StringUtil.isNotBlank(pageTypeFilter)) {
            final String pageType = determinePageType(page);
            final List<String> allowedTypes = StreamUtil.split(pageTypeFilter, ",")
                    .get(stream -> stream.map(String::trim).map(String::toLowerCase).collect(Collectors.toList()));
            if (!allowedTypes.contains(pageType.toLowerCase())) {
                return false;
            }
        }

        // Check URL patterns
        final String pageUrl = page.getWebUrl();
        if (logger.isDebugEnabled()) {
            logger.debug("Evaluating page URL: {}", pageUrl);
        }

        if (pageUrl != null) {
            if (excludePattern != null && excludePattern.matcher(pageUrl).find()
                    || includePattern != null && !includePattern.matcher(pageUrl).find()) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks if a page is a system page.
     *
     * @param page the SharePoint page to check
     * @return true if the page is a system page, false otherwise
     */
    protected boolean isSystemPage(final BaseSitePage page) {
        if (page.getWebUrl() != null) {
            final String url = page.getWebUrl().toLowerCase();
            // Check for common system page patterns
            return url.contains("/_layouts/") || url.contains("/_catalogs/") || url.contains("/forms/") || url.contains("/_api/")
                    || url.contains("/sitepages/forms/") || url.contains("/sitepages/devhome.aspx");
        }
        return false;
    }

    /**
     * Gets the site ID from parameters.
     *
     * @param paramMap data store parameters
     * @return the site ID string
     */
    protected String getSiteId(final DataStoreParams paramMap) {
        return paramMap.getAsString(SITE_ID);
    }

    /**
     * Checks if a site is excluded from crawling.
     *
     * @param paramMap data store parameters
     * @param site the SharePoint site to check
     * @return true if the site is excluded, false otherwise
     */
    protected boolean isExcludedSite(final DataStoreParams paramMap, final Site site) {
        final String excludeSiteIds = paramMap.getAsString(EXCLUDE_SITE_ID);
        if (StringUtil.isBlank(excludeSiteIds)) {
            return false;
        }

        final List<String> excludeList = StreamUtil.split(excludeSiteIds, ",")
                .get(stream -> stream.map(String::trim).filter(StringUtil::isNotBlank).collect(Collectors.toList()));

        // Check by site ID
        // Check by site name
        if (excludeList.contains(site.getId())
                || site.getDisplayName() != null && excludeList.stream().anyMatch(pattern -> site.getDisplayName().matches(pattern))) {
            return true;
        }

        // Check by site URL
        if (site.getWebUrl() != null && excludeList.stream().anyMatch(pattern -> site.getWebUrl().contains(pattern))) {
            return true;
        }

        return false;
    }

    /**
     * Checks if system pages should be ignored.
     *
     * @param paramMap data store parameters
     * @return true if system pages should be ignored, false otherwise
     */
    protected boolean isIgnoreSystemPages(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_SYSTEM_PAGES, Constants.TRUE));
    }

    /**
     * Checks if errors should be ignored during crawling.
     *
     * @param paramMap data store parameters
     * @return true if errors should be ignored, false otherwise
     */
    @Override
    protected boolean isIgnoreError(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.FALSE));
    }

    /**
     * Gets a compiled regex pattern from parameters.
     *
     * @param paramMap data store parameters
     * @param key the parameter key for the pattern
     * @return compiled Pattern or null if pattern is blank or invalid
     */
    protected Pattern getPattern(final DataStoreParams paramMap, final String key) {
        final String pattern = paramMap.getAsString(key);
        if (StringUtil.isNotBlank(pattern)) {
            try {
                return Pattern.compile(pattern);
            } catch (final Exception e) {
                logger.warn("Invalid regex pattern for {}: {}", key, pattern, e);
            }
        }
        return null;
    }

    /**
     * Gets the field name with proper mapping.
     *
     * @param paramMap data store parameters
     * @param prefix the prefix for the field
     * @param field the field name
     * @return the mapped field name
     */
    protected String getFieldName(final DataStoreParams paramMap, final String prefix, final String field) {
        final String key = prefix + "." + field;
        return paramMap.getAsString(key, prefix + "_" + field);
    }

    /**
     * Gets crawler stats key for the site.
     *
     * @param siteName the name of the SharePoint site
     * @return statistics key object for tracking crawl metrics
     */
    protected StatsKeyObject getCrawlerStats(final String siteName) {
        return new StatsKeyObject("SharePointPage#" + siteName);
    }

    /**
     * Sets the extractor name for content extraction.
     *
     * @param extractorName the name of the extractor to use
     */
    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }
}
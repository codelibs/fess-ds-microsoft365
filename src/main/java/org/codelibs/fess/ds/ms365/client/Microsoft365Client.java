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
package org.codelibs.fess.ds.ms365.client;

import static org.codelibs.fess.ds.ms365.Microsoft365Constants.*;

import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.microsoft.graph.models.BaseSitePage;
import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.ChannelCollectionResponse;
import com.microsoft.graph.models.Chat;
import com.microsoft.graph.models.ChatCollectionResponse;
import com.microsoft.graph.models.ChatMessage;
import com.microsoft.graph.models.ChatMessageAttachment;
import com.microsoft.graph.models.ChatMessageCollectionResponse;
import com.microsoft.graph.models.ConversationMember;
import com.microsoft.graph.models.ConversationMemberCollectionResponse;
import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.DriveCollectionResponse;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.DriveItemCollectionResponse;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.GroupCollectionResponse;
import com.microsoft.graph.models.ListCollectionResponse;
import com.microsoft.graph.models.ListItem;
import com.microsoft.graph.models.ListItemCollectionResponse;
import com.microsoft.graph.models.NotebookCollectionResponse;
import com.microsoft.graph.models.OnenotePage;
import com.microsoft.graph.models.OnenotePageCollectionResponse;
import com.microsoft.graph.models.OnenoteSection;
import com.microsoft.graph.models.OnenoteSectionCollectionResponse;
import com.microsoft.graph.models.PermissionCollectionResponse;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.models.SiteCollectionResponse;
import com.microsoft.graph.models.SitePageCollectionResponse;
import com.microsoft.graph.models.User;
import com.microsoft.graph.models.UserCollectionResponse;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.kiota.ApiException;
import com.microsoft.kiota.ResponseHeaders;

/**
 * This class provides a client for accessing Microsoft Microsoft 365 services using the Microsoft Graph API.
 * It handles authentication, and provides methods for interacting with services like OneDrive, OneNote, and Teams.
 * This client is designed to be used within the Fess data store framework.
 */
public class Microsoft365Client implements Closeable {

    private static final Logger logger = LogManager.getLogger(Microsoft365Client.class);

    /** The parameter name for the Azure AD tenant ID. */
    protected static final String TENANT_PARAM = "tenant";
    /** The parameter name for the Azure AD client ID. */
    protected static final String CLIENT_ID_PARAM = "client_id";
    /** The parameter name for the Azure AD client secret. */
    protected static final String CLIENT_SECRET_PARAM = "client_secret";
    /** The parameter name for the access timeout. */
    protected static final String ACCESS_TIMEOUT = "access_timeout";
    /** The parameter name for the refresh token interval. */
    protected static final String REFRESH_TOKEN_INTERVAL = "refresh_token_interval";
    /** The parameter name for the cache size. */
    protected static final String CACHE_SIZE = "cache_size";
    /** The parameter name for the maximum content length. */
    protected static final String MAX_CONTENT_LENGTH = "max_content_length";

    /** Error code for an invalid authentication token. */
    protected static final String INVALID_AUTHENTICATION_TOKEN = "InvalidAuthenticationToken";

    /** The Microsoft Graph service client. */
    protected GraphServiceClient client;
    /** The data store parameters. */
    protected DataStoreParams params;
    /** A cache for user types. */
    protected LoadingCache<String, UserType> userTypeCache;
    /** A cache for group IDs. */
    protected LoadingCache<String, String[]> groupIdCache;
    private LoadingCache<String, String> groupNameCache;
    /** A cache for user principal names (UPNs). */
    protected LoadingCache<String, String> upnCache;

    /** The maximum content length for extracted text. */
    protected int maxContentLength = -1;

    /**
     * Constructs a new Microsoft365Client with the specified data store parameters.
     *
     * @param params The data store parameters for configuration.
     */
    public Microsoft365Client(final DataStoreParams params) {
        this.params = params;

        final String tenant = params.getAsString(TENANT_PARAM, StringUtil.EMPTY);
        final String clientId = params.getAsString(CLIENT_ID_PARAM, StringUtil.EMPTY);
        final String clientSecret = params.getAsString(CLIENT_SECRET_PARAM, StringUtil.EMPTY);
        if (tenant.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
            throw new DataStoreException("parameter '" + //
                    TENANT_PARAM + "', '" + //
                    CLIENT_ID_PARAM + "', '" + //
                    CLIENT_SECRET_PARAM + "' is required");
        }
        try {
            maxContentLength = Integer.parseInt(params.getAsString(MAX_CONTENT_LENGTH, Integer.toString(maxContentLength)));
        } catch (final NumberFormatException e) {
            logger.warn("Failed to parse {}.", params.getAsString(MAX_CONTENT_LENGTH), e);
        }

        try {
            // Add multi-tenant authentication support for Azure Identity v1.16.3
            final ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder().clientId(clientId)
                    .clientSecret(clientSecret)
                    .tenantId(tenant)
                    .additionallyAllowedTenants("*") // Allow all tenants for backward compatibility
                    .build();

            // Initialize GraphServiceClient with new v6 API
            client = new GraphServiceClient(clientSecretCredential);
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a client.", e);
        }

        userTypeCache = CacheBuilder.newBuilder()
                .maximumSize(Integer.parseInt(params.getAsString(CACHE_SIZE, "10000")))
                .build(new CacheLoader<String, UserType>() {
                    @Override
                    public UserType load(final String key) {
                        try {
                            getUser(key, Collections.emptyList());
                            return UserType.USER;
                        } catch (final ApiException e) {
                            if (e.getResponseStatusCode() == 404) {
                                return UserType.GROUP;
                            }
                            logger.warn("Failed to detect an user type.", e);
                        } catch (final Exception e) {
                            logger.warn("Failed to get an user.", e);
                        }
                        return UserType.UNKNOWN;
                    }
                });

        groupIdCache = CacheBuilder.newBuilder()
                .maximumSize(Integer.parseInt(params.getAsString(CACHE_SIZE, "10000")))
                .build(new CacheLoader<String, String[]>() {
                    @Override
                    public String[] load(final String email) {
                        final List<String> idList = new ArrayList<>();
                        getGroups(Collections.emptyList(), g -> {
                            if (email.equals(g.getMail())) {
                                idList.add(g.getId());
                            }
                        });
                        return idList.toArray(new String[idList.size()]);
                    }
                });

        upnCache = CacheBuilder.newBuilder()
                .maximumSize(Integer.parseInt(params.getAsString(CACHE_SIZE, "10000")))
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(final String objectId) {
                        return doResolveUserPrincipalName(objectId);
                    }
                });

        groupNameCache = CacheBuilder.newBuilder()
                .maximumSize(Integer.parseInt(params.getAsString(CACHE_SIZE, "10000")))
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(final String objectId) {
                        return doResolveGroupName(objectId);
                    }
                });

    }

    @Override
    public void close() {
        userTypeCache.invalidateAll();
        groupIdCache.invalidateAll();
    }

    /**
     * An enumeration of user types in Microsoft 365.
     */
    public enum UserType {
        /** Represents a regular user. */
        USER,
        /** Represents a group. */
        GROUP,
        /** Represents an unknown user type. */
        UNKNOWN;
    }

    /**
     * Retrieves the type of a user (user, group, or unknown) by their ID.
     *
     * @param id The ID of the user or group.
     * @return The UserType of the specified ID.
     */
    public UserType getUserType(final String id) {
        if (StringUtil.isBlank(id)) {
            if (logger.isDebugEnabled()) {
                logger.debug("User ID is blank, returning UNKNOWN type");
            }
            return UserType.UNKNOWN;
        }
        try {
            final UserType userType = userTypeCache.get(id);
            if (logger.isDebugEnabled()) {
                logger.debug("Retrieved user type - ID: {}, Type: {}", id, userType);
            }
            return userType;
        } catch (final ExecutionException e) {
            logger.warn("Failed to get user type for ID: {}", id, e);
            return UserType.UNKNOWN;
        }
    }

    /**
     * Retrieves the content of a drive item as an InputStream.
     *
     * @param driveId The ID of the drive.
     * @param itemId The ID of the drive item.
     * @return An InputStream containing the content of the drive item.
     */
    public InputStream getDriveContent(final String driveId, final String itemId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting drive content - Drive ID: {}, Item ID: {}", driveId, itemId);
        }
        try {
            final InputStream content = client.drives().byDriveId(driveId).items().byDriveItemId(itemId).content().get();
            if (logger.isDebugEnabled()) {
                logger.debug("Successfully retrieved drive content for Drive ID: {}, Item ID: {}", driveId, itemId);
            }
            return content;
        } catch (final Exception e) {
            logger.error("Failed to get drive content - Drive ID: {}, Item ID: {}", driveId, itemId, e);
            throw e;
        }
    }

    /**
     * Retrieves the permissions for a drive item.
     *
     * @param driveId The ID of the drive.
     * @param itemId The ID of the drive item.
     * @return A PermissionCollectionResponse containing the permissions.
     */
    public PermissionCollectionResponse getDrivePermissions(final String driveId, final String itemId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting drive permissions - Drive ID: {}, Item ID: {}", driveId, itemId);
        }
        final PermissionCollectionResponse response = client.drives().byDriveId(driveId).items().byDriveItemId(itemId).permissions().get();
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieved {} drive permissions for Drive ID: {}, Item ID: {}",
                    response.getValue() != null ? response.getValue().size() : 0, driveId, itemId);
        }
        return response;
    }

    /**
     * Retrieves a page of drive items within a drive.
     *
     * @param driveId The ID of the drive.
     * @param itemId The ID of the parent drive item, or null for the root.
     * @return A DriveItemCollectionResponse containing the drive items.
     * @throws IllegalArgumentException if driveId is null or empty
     */
    public DriveItemCollectionResponse getDriveItemPage(final String driveId, final String itemId) {
        // Validate driveId to prevent malformed drive ID errors
        if (driveId == null || driveId.trim().isEmpty()) {
            throw new IllegalArgumentException("Drive ID cannot be null or empty");
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Getting drive item page - Drive ID: {}, Parent Item ID: {}", driveId, itemId != null ? itemId : "root");
        }
        DriveItemCollectionResponse response;
        if (itemId == null) {
            response = client.drives().byDriveId(driveId).items().byDriveItemId("root").children().get();
        } else {
            response = client.drives().byDriveId(driveId).items().byDriveItemId(itemId).children().get();
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieved {} drive items for Drive ID: {}, Parent Item ID: {}",
                    response.getValue() != null ? response.getValue().size() : 0, driveId, itemId != null ? itemId : "root");
        }
        return response;
    }

    /**
     * Retrieves the next page of drive permissions using the provided nextLink URL.
     *
     * @param driveId The ID of the drive.
     * @param itemId The ID of the drive item.
     * @param nextLink The nextLink URL from a previous response.
     * @return A PermissionCollectionResponse containing the next page of permissions.
     */
    public PermissionCollectionResponse getDrivePermissionsByNextLink(final String driveId, final String itemId, final String nextLink) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting drive permissions via next link - Drive ID: {}, Item ID: {}", driveId, itemId);
        }
        final PermissionCollectionResponse response =
                client.drives().byDriveId(driveId).items().byDriveItemId(itemId).permissions().withUrl(nextLink).get();
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieved {} drive permissions via next link for Drive ID: {}, Item ID: {}",
                    response.getValue() != null ? response.getValue().size() : 0, driveId, itemId);
        }
        return response;
    }

    /**
     * Retrieves the next page of drive items using the provided nextLink URL.
     *
     * @param driveId The ID of the drive.
     * @param itemId The ID of the drive item.
     * @param nextLink The nextLink URL from a previous response.
     * @return A DriveItemCollectionResponse containing the next page of items.
     */
    public DriveItemCollectionResponse getDriveItemsByNextLink(final String driveId, final String itemId, final String nextLink) {
        return client.drives().byDriveId(driveId).items().byDriveItemId(itemId).children().withUrl(nextLink).get();
    }

    /**
     * Retrieves a user by their ID.
     * In SDK v6, query options are applied using requestConfiguration lambda.
     *
     * @param userId The ID of the user.
     * @param options A list of options for the request (deprecated - kept for API compatibility).
     * @return The User object.
     */
    public User getUser(final String userId, final List<? extends Object> options) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        return client.users().byUserId(userId).get(requestConfiguration -> {
            // Select only essential fields to improve performance
            requestConfiguration.queryParameters.select =
                    new String[] { "id", "displayName", "mail", "userPrincipalName", "assignedLicenses", "jobTitle", "department" };
        });
    }

    /**
     * Retrieves a user by their ID with only assignedLicenses field for license checking.
     * This is a highly optimized method for license verification.
     *
     * @param userId The ID of the user.
     * @return The User object with only assignedLicenses field populated.
     */
    public User getUserForLicenseCheck(final String userId) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        return client.users().byUserId(userId).get(requestConfiguration -> {
            // Select only assignedLicenses field to minimize data transfer
            requestConfiguration.queryParameters.select = new String[] { "id", "assignedLicenses" };
        });
    }

    /**
     * Retrieves a list of users, processing each user with the provided consumer.
     * In SDK v6, query options are applied using requestConfiguration lambda.
     * Note: License filtering is done post-retrieval to avoid Microsoft Graph API limitations.
     *
     * @param options A list of query options for the request (deprecated - kept for API compatibility).
     * @param consumer A consumer to process each User object.
     */
    public void getUsers(final List<Object> options, final Consumer<User> consumer) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        UserCollectionResponse response = client.users().get(requestConfiguration -> {
            // Select only essential fields to improve performance and include assignedLicenses for license checking
            requestConfiguration.queryParameters.select =
                    new String[] { "id", "displayName", "mail", "userPrincipalName", "assignedLicenses" };
            // Remove complex filters - license checking will be done after retrieval
            // This avoids "Complex query on property assignedLicenses is not supported" error
        });

        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.users().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves the group IDs associated with an email address.
     *
     * @param email The email address to search for.
     * @return An array of group IDs.
     */
    public String[] getGroupIdsByEmail(final String email) {
        try {
            return groupIdCache.get(email);
        } catch (final ExecutionException e) {
            logger.warn("Failed to get group ids.", e);
            return StringUtil.EMPTY_STRINGS;
        }
    }

    /**
     * Retrieves a list of groups, processing each group with the provided consumer.
     * In SDK v6, query options are applied using requestConfiguration lambda.
     *
     * @param options A list of query options for the request (deprecated - kept for API compatibility).
     * @param consumer A consumer to process each Group object.
     */
    public void getGroups(final List<Object> options, final Consumer<Group> consumer) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        GroupCollectionResponse response = client.groups().get(requestConfiguration -> {
            // Select only essential fields to improve performance
            requestConfiguration.queryParameters.select =
                    new String[] { "id", "displayName", "mail", "groupTypes", "resourceProvisioningOptions" };
            requestConfiguration.queryParameters.orderby = new String[] { "displayName" };
        });

        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.groups().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves Microsoft 365 groups (Unified groups) only, processing each group with the provided consumer.
     * This method filters for groups with groupTypes containing 'Unified' at the server level for efficiency.
     *
     * @param consumer A consumer to process each Group object.
     */
    public void getMicrosoft365Groups(final Consumer<Group> consumer) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        GroupCollectionResponse response = client.groups().get(requestConfiguration -> {
            // Filter for Microsoft 365 groups (Unified groups) only at server level
            requestConfiguration.queryParameters.filter = "groupTypes/any(c:c eq 'Unified')";
            // Select only essential fields to improve performance
            requestConfiguration.queryParameters.select =
                    new String[] { "id", "displayName", "mail", "groupTypes", "resourceProvisioningOptions" };
            // Removed orderby as it's not supported with advanced filters and ConsistencyLevel:eventual
            // Required for advanced queries
            requestConfiguration.headers.add("ConsistencyLevel", "eventual");
        });

        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.groups().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves a group by its ID.
     *
     * @param id The ID of the group.
     * @return The Group object, or null if not found.
     */
    public Group getGroupById(final String id) {
        final List<Group> groupList = new ArrayList<>();
        getGroups(Collections.emptyList(), g -> {
            if (id.equals(g.getId())) {
                groupList.add(g);
            }
        });
        if (logger.isDebugEnabled()) {
            groupList.forEach(ToStringBuilder::reflectionToString);
        }
        if (groupList.size() == 1) {
            return groupList.get(0);
        }
        return null;
    }

    /**
     * Retrieves a page of notebooks.
     *
     * @param userId The ID of the user, or null for the current user.
     * @return a NotebookCollectionResponse containing the notebooks.
     */
    public NotebookCollectionResponse getNotebookPage(final String userId) {
        if (userId != null) {
            return client.users().byUserId(userId).onenote().notebooks().get();
        }
        return client.me().onenote().notebooks().get();
    }

    /**
     * Retrieves all sections within a notebook.
     *
     * @param userId The ID of the user, or null for the current user.
     * @param notebookId The ID of the notebook.
     * @return A list of OnenoteSection objects.
     */
    protected List<OnenoteSection> getSections(final String userId, final String notebookId) {
        OnenoteSectionCollectionResponse response;
        if (userId != null) {
            response = client.users().byUserId(userId).onenote().notebooks().byNotebookId(notebookId).sections().get();
        } else {
            response = client.me().onenote().notebooks().byNotebookId(notebookId).sections().get();
        }
        final List<OnenoteSection> sections = new ArrayList<>();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            sections.addAll(response.getValue());

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            if (userId != null) {
                response = client.users()
                        .byUserId(userId)
                        .onenote()
                        .notebooks()
                        .byNotebookId(notebookId)
                        .sections()
                        .withUrl(response.getOdataNextLink())
                        .get();
            } else {
                response = client.me().onenote().notebooks().byNotebookId(notebookId).sections().withUrl(response.getOdataNextLink()).get();
            }
        }
        return sections;
    }

    /**
     * Retrieves all pages within a section.
     *
     * @param userId The ID of the user, or null for the current user.
     * @param sectionId The ID of the section.
     * @return A list of OnenotePage objects.
     */
    protected List<OnenotePage> getPages(final String userId, final String sectionId) {
        OnenotePageCollectionResponse response;
        if (userId != null) {
            response = client.users().byUserId(userId).onenote().sections().byOnenoteSectionId(sectionId).pages().get();
        } else {
            response = client.me().onenote().sections().byOnenoteSectionId(sectionId).pages().get();
        }
        final List<OnenotePage> pages = new ArrayList<>();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            pages.addAll(response.getValue());

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            if (userId != null) {
                response = client.users()
                        .byUserId(userId)
                        .onenote()
                        .sections()
                        .byOnenoteSectionId(sectionId)
                        .pages()
                        .withUrl(response.getOdataNextLink())
                        .get();
            } else {
                response =
                        client.me().onenote().sections().byOnenoteSectionId(sectionId).pages().withUrl(response.getOdataNextLink()).get();
            }
        }
        return pages;
    }

    /**
     * Retrieves the contents of a OneNote section as a single string.
     *
     * @param userId The ID of the user, or null for the current user.
     * @param section The OnenoteSection to retrieve contents from.
     * @return A string containing the concatenated contents of the section.
     */
    protected String getSectionContents(final String userId, final OnenoteSection section) {
        final StringBuilder sb = new StringBuilder();
        sb.append(section.getDisplayName()).append('\n');
        final List<OnenotePage> pages = getPages(userId, section.getId());
        Collections.reverse(pages);
        sb.append(pages.stream().map(page -> getPageContents(userId, page)).collect(Collectors.joining("\n")));
        return sb.toString();
    }

    /**
     * Retrieves the contents of a OneNote page as a single string.
     *
     * @param userId The ID of the user, or null for the current user.
     * @param page The OnenotePage to retrieve contents from.
     * @return A string containing the contents of the page.
     */
    protected String getPageContents(final String userId, final OnenotePage page) {
        final StringBuilder sb = new StringBuilder();
        sb.append(page.getTitle()).append('\n');
        try (final InputStream in =
                userId != null ? client.users().byUserId(userId).onenote().pages().byOnenotePageId(page.getId()).content().get()
                        : client.me().onenote().pages().byOnenotePageId(page.getId()).content().get()) {
            sb.append(ComponentUtil.getExtractorFactory()
                    .builder(in, Collections.emptyMap())
                    .maxContentLength(maxContentLength)
                    .extract()
                    .getContent());
        } catch (final Exception e) {
            if (!ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                throw new DataStoreCrawlingException(page.getTitle(), "Failed to get contents: " + page.getId(), e);
            }
            if (logger.isDebugEnabled()) {
                logger.warn("Failed to get contents of Page: {}", page.getTitle(), e);
            } else {
                logger.warn("Failed to get contents of Page: {}. {}", page.getTitle(), e.getMessage());
            }
        }
        return sb.toString();
    }

    /**
     * Retrieves the content of a notebook as a single string.
     *
     * @param userId The ID of the user, or null for the current user.
     * @param notebookId The ID of the notebook.
     * @return A string containing the concatenated contents of the notebook.
     */
    public String getNotebookContent(final String userId, final String notebookId) {
        final List<OnenoteSection> sections = getSections(userId, notebookId);
        Collections.reverse(sections);
        return sections.stream().map(section -> getSectionContents(userId, section)).collect(Collectors.joining("\n"));
    }

    /**
     * Retrieves a site by its ID.
     *
     * @param id The ID of the site, or "root" for the root site.
     * @return The Site object.
     */
    public Site getSite(final String id) {
        final String siteId = StringUtil.isNotBlank(id) ? id : "root";
        if (logger.isDebugEnabled()) {
            logger.debug("Getting site with ID: {} (resolved to: {})", id, siteId);
        }
        try {
            final Site site = client.sites().bySiteId(siteId).get();
            if (logger.isDebugEnabled()) {
                logger.debug("Successfully retrieved site - ID: {}, DisplayName: {}, WebUrl: {}", site.getId(), site.getDisplayName(),
                        site.getWebUrl());
            }
            return site;
        } catch (final Exception e) {
            logger.warn("Failed to get site with ID: {}", siteId, e);
            throw e;
        }
    }

    /**
     * Retrieves all sites with pagination support.
     *
     * @param consumer A consumer to process each Site object.
     */
    public void getSites(final Consumer<Site> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting all sites with pagination support");
        }

        try {
            SiteCollectionResponse response = client.sites().get();
            int pageCount = 0;
            int totalSites = 0;

            // Handle pagination with odata.nextLink
            while (response != null && response.getValue() != null) {
                pageCount++;
                final List<Site> sites = response.getValue();
                totalSites += sites.size();

                if (logger.isDebugEnabled()) {
                    logger.debug("Processing sites page {} with {} sites (total so far: {})", pageCount, sites.size(), totalSites);
                }

                sites.forEach(consumer::accept);

                // Check if there's a next page
                if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                    // No more pages, exit loop
                    if (logger.isDebugEnabled()) {
                        logger.debug("Site pagination completed - processed {} pages with total {} sites", pageCount, totalSites);
                    }
                    break;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Found next link, continuing to page {}", pageCount + 1);
                }
                // Request the next page using the nextLink URL
                response = client.sites().withUrl(response.getOdataNextLink()).get();
            }
        } catch (final Exception e) {
            logger.warn("Failed to get sites", e);
            throw e;
        }
    }

    /**
     * Retrieves lists from a specific site.
     *
     * @param siteId The ID of the site.
     * @param consumer A consumer to process each List object.
     */
    public void getSiteLists(final String siteId, final Consumer<com.microsoft.graph.models.List> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting lists for site: {}", siteId);
        }

        if (StringUtil.isBlank(siteId)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Site ID is blank, skipping list retrieval");
            }
            return;
        }

        try {
            // Get lists with system facet information
            // Note: system facet is not included by default, so we use $select to explicitly request it
            // along with commonly used fields to ensure compatibility
            ListCollectionResponse response = client.sites().bySiteId(siteId).lists().get(requestConfiguration -> {
                requestConfiguration.queryParameters.select = new String[] { "id", "name", "displayName", "description", "webUrl", "list",
                        "system", "createdDateTime", "lastModifiedDateTime", "createdBy", "lastModifiedBy" };
                if (logger.isDebugEnabled()) {
                    logger.debug("Request configured to select extended list properties including system facet for site: {}", siteId);
                }
            });

            int pageCount = 0;
            int totalLists = 0;

            // Handle pagination with odata.nextLink
            while (response != null && response.getValue() != null) {
                pageCount++;
                final List<com.microsoft.graph.models.List> lists = response.getValue();
                totalLists += lists.size();

                if (logger.isDebugEnabled()) {
                    logger.debug("Processing lists page {} with {} lists for site {} (total so far: {})", pageCount, lists.size(), siteId,
                            totalLists);
                }

                lists.forEach(consumer::accept);

                // Check if there's a next page
                if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                    // No more pages, exit loop
                    if (logger.isDebugEnabled()) {
                        logger.debug("List pagination completed for site {} - processed {} pages with total {} lists", siteId, pageCount,
                                totalLists);
                    }
                    break;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Found next link for lists, continuing to page {} for site: {}", pageCount + 1, siteId);
                }
                // Request the next page using the nextLink URL
                response = client.sites().bySiteId(siteId).lists().withUrl(response.getOdataNextLink()).get();
            }
        } catch (final Exception e) {
            logger.warn("Failed to get lists for site: {}", siteId, e);
            throw e;
        }
    }

    /**
     * Retrieves a specific list from a site.
     *
     * @param siteId The ID of the site.
     * @param listId The ID of the list.
     * @return The List object.
     */
    public com.microsoft.graph.models.List getList(final String siteId, final String listId) {
        return client.sites().bySiteId(siteId).lists().byListId(listId).get(requestConfiguration -> {
            requestConfiguration.queryParameters.select = new String[] { "id", "name", "displayName", "description", "webUrl", "list",
                    "system", "createdDateTime", "lastModifiedDateTime", "createdBy", "lastModifiedBy" };
        });
    }

    /**
     * Retrieves permissions for a SharePoint site.
     *
     * @param siteId The ID of the SharePoint site
     * @return PermissionCollectionResponse containing the site permissions
     * @throws IllegalArgumentException if siteId is null or empty
     */
    public PermissionCollectionResponse getSitePermissions(final String siteId) {
        if (StringUtil.isBlank(siteId)) {
            throw new IllegalArgumentException("siteId cannot be null or empty");
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Getting site permissions - Site ID: {}", siteId);
        }
        final PermissionCollectionResponse response = client.sites().bySiteId(siteId).permissions().get();
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieved {} site permissions for Site ID: {}", response.getValue() != null ? response.getValue().size() : 0,
                    siteId);
        }
        return response;
    }

    /**
     * Retrieves the next page of permissions for a SharePoint site using pagination.
     *
     * @param siteId The ID of the SharePoint site
     * @param nextLink The next link URL for pagination
     * @return PermissionCollectionResponse containing the next page of site permissions, or null if nextLink is blank
     */
    public PermissionCollectionResponse getSitePermissionsByNextLink(final String siteId, final String nextLink) {
        if (StringUtil.isBlank(nextLink)) {
            return null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Getting site permissions via next link - Site ID: {}", siteId);
        }
        return client.sites().bySiteId(siteId).permissions().withUrl(nextLink).get();
    }

    /**
     * Retrieves all items from a specific list with pagination support.
     *
     * @param siteId The ID of the site.
     * @param listId The ID of the list.
     * @param consumer A consumer to process each ListItem object.
     */
    public void getListItems(final String siteId, final String listId, final Consumer<ListItem> consumer) {
        // Get list items with expanded fields to ensure content is available
        ListItemCollectionResponse response = client.sites().bySiteId(siteId).lists().byListId(listId).items().get(config -> {
            config.queryParameters.expand = new String[] { "fields" };
            config.queryParameters.select = new String[] { "id", "createdDateTime", "lastModifiedDateTime", "webUrl", "fields" };
        });

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.sites().bySiteId(siteId).lists().byListId(listId).items().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves a specific list item.
     *
     * @param siteId The ID of the site.
     * @param listId The ID of the list.
     * @param itemId The ID of the list item.
     * @return The ListItem object.
     */
    public ListItem getListItem(final String siteId, final String listId, final String itemId) {
        return client.sites().bySiteId(siteId).lists().byListId(listId).items().byListItemId(itemId).get();
    }

    /**
     * Retrieves a specific list item with expanded fields.
     *
     * @param siteId The ID of the site.
     * @param listId The ID of the list.
     * @param itemId The ID of the list item.
     * @param expandFields Whether to expand the fields property.
     * @return The ListItem object with expanded fields.
     */
    public ListItem getListItem(final String siteId, final String listId, final String itemId, final boolean expandFields) {
        if (!expandFields) {
            return getListItem(siteId, listId, itemId);
        }
        return client.sites().bySiteId(siteId).lists().byListId(listId).items().byListItemId(itemId).get(config -> {
            config.queryParameters.expand = new String[] { "fields" };
            config.queryParameters.select = new String[] { "id", "createdDateTime", "lastModifiedDateTime", "webUrl", "fields" };
        });
    }

    /**
     * Retrieves all items in a drive with recursive traversal and pagination support.
     *
     * @param driveId The ID of the drive.
     * @param consumer A consumer to process each DriveItem object.
     */
    public void getDriveItemsInDrive(final String driveId, final Consumer<com.microsoft.graph.models.DriveItem> consumer) {
        getDriveItemChildren(driveId, consumer, null);
    }

    /**
     * Recursively retrieves children of a drive item with pagination support.
     *
     * @param driveId The ID of the drive.
     * @param consumer A consumer to process each DriveItem object.
     * @param item The parent drive item (null for root).
     */
    protected void getDriveItemChildren(final String driveId, final Consumer<com.microsoft.graph.models.DriveItem> consumer,
            final com.microsoft.graph.models.DriveItem item) {
        if (logger.isDebugEnabled()) {
            logger.debug("Current item: {}", item != null ? item.getName() + " -> " + item.getWebUrl() : "root");
        }

        com.microsoft.graph.models.DriveItemCollectionResponse response;
        try {
            if (item != null) {
                consumer.accept(item);
                if (item.getFolder() == null) {
                    return;
                }
            }

            response = getDriveItemPage(driveId, item != null ? item.getId() : null);

            // Handle pagination with odata.nextLink
            while (response != null && response.getValue() != null) {
                response.getValue().forEach(child -> getDriveItemChildren(driveId, consumer, child));

                // Check if there's a next page
                if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                    // No more pages, exit loop
                    break;
                }
                // Request the next page using the nextLink URL
                try {
                    final String itemIdToUse = item != null ? item.getId() : "root";
                    response = getDriveItemsByNextLink(driveId, itemIdToUse, response.getOdataNextLink());
                } catch (final Exception e) {
                    logger.warn("Failed to get next page of drive items: {}", e.getMessage());
                    break;
                }
            }
        } catch (final com.microsoft.kiota.ApiException e) {
            if (e.getResponseStatusCode() == 404) {
                logger.debug("Drive item is not found.", e);
            } else {
                logger.warn("Failed to access a drive item.", e);
            }
        }
    }

    //    public SiteCollectionPage getSites() {
    //        return client.sites().buildRequest().get();
    //    }
    //
    //    public SiteCollectionPage getNextSitePage(final SiteCollectionPage page) {
    //        if (page.getNextPage() == null) {
    //            return null;
    //        }
    //        return page.getNextPage().buildRequest().get();
    //    }

    /**
     * Retrieves a drive by its ID.
     *
     * @param driveId The ID of the drive.
     * @return The Drive object.
     */
    public Drive getDrive(final String driveId) {
        if (driveId == null) {
            return client.me().drive().get();
        }
        return client.drives().byDriveId(driveId).get();
    }

    /**
     * Retrieves all drives, processing each drive with the provided consumer.
     * Implements pagination to handle large tenant environments with many SharePoint drives.
     *
     * @param consumer A consumer to process each Drive object.
     */
    // for testing
    public void getDrives(final Consumer<Drive> consumer) {
        DriveCollectionResponse response = client.drives().get();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.drives().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves all drives for a specific SharePoint site, processing each drive with the provided consumer.
     * Implements pagination to handle sites with many drives.
     *
     * @param siteId The ID of the SharePoint site.
     * @param consumer A consumer to process each Drive object.
     */
    public void getSiteDrives(final String siteId, final Consumer<Drive> consumer) {
        DriveCollectionResponse response = client.sites().bySiteId(siteId).drives().get();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.sites().bySiteId(siteId).drives().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves the drive for a specific user.
     *
     * @param userId the ID of the user
     * @return the user's drive
     */
    public Drive getUserDrive(final String userId) {
        return client.users().byUserId(userId).drive().get();
    }

    /**
     * Retrieves the drive for a specific group.
     *
     * @param groupId the ID of the group
     * @return the group's drive
     */
    public Drive getGroupDrive(final String groupId) {
        return client.groups().byGroupId(groupId).drive().get();
    }

    /**
     * Retrieves a list of Teams, processing each Team with the provided consumer.
     * In SDK v6, query options are applied using requestConfiguration lambda.
     *
     * @param options A list of query options for the request (deprecated - kept for API compatibility).
     * @param consumer A consumer to process each Group object representing a Team.
     */
    public void geTeams(final List<Object> options, final Consumer<Group> consumer) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        GroupCollectionResponse response = client.groups().get(requestConfiguration -> {
            // Filter for Teams-enabled groups only
            requestConfiguration.queryParameters.filter = "resourceProvisioningOptions/any(x:x eq 'Team')";
            // Select only essential fields to improve performance
            requestConfiguration.queryParameters.select =
                    new String[] { "id", "displayName", "mail", "description", "resourceProvisioningOptions" };
            // Removed orderby as it's not supported with advanced filters and ConsistencyLevel:eventual
            // Required for advanced queries
            requestConfiguration.headers.add("ConsistencyLevel", "eventual");
        });
        final Consumer<Group> filter = g -> {
            final Map<String, Object> additionalDataManager = g.getAdditionalData();
            if (additionalDataManager != null) {
                final Object jsonObj = additionalDataManager.get("resourceProvisioningOptions");
                // Handle both JsonElement (SDK v5 style) and native List/Collection (SDK v6 style)
                if (jsonObj instanceof final JsonElement jsonElement && jsonElement.isJsonArray()) {
                    final JsonArray array = jsonElement.getAsJsonArray();
                    for (int i = 0; i < array.size(); i++) {
                        if ("Team".equals(array.get(i).getAsString())) {
                            consumer.accept(g);
                            return;
                        }
                    }
                } else if (jsonObj instanceof final java.util.Collection<?> collection) {
                    // Handle native collection objects (SDK v6 may provide List<String> directly)
                    for (final Object item : collection) {
                        if ("Team".equals(String.valueOf(item))) {
                            consumer.accept(g);
                            return;
                        }
                    }
                } else if (jsonObj instanceof final Object[] array) {
                    // Handle object arrays (another possible format)
                    for (final Object item : array) {
                        if ("Team".equals(String.valueOf(item))) {
                            consumer.accept(g);
                            return;
                        }
                    }
                }
            }
        };

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(filter);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.groups().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves a list of channels in a Team, processing each channel with the provided consumer.
     * In SDK v6, query options are applied using requestConfiguration lambda.
     *
     * @param options A list of query options for the request (deprecated - kept for API compatibility).
     * @param consumer A consumer to process each Channel object.
     * @param teamId The ID of the Team.
     */
    public void getChannels(final List<Object> options, final Consumer<Channel> consumer, final String teamId) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        ChannelCollectionResponse response = client.teams().byTeamId(teamId).channels().get(requestConfiguration -> {
            // Select only essential fields to improve performance
            requestConfiguration.queryParameters.select = new String[] { "id", "displayName", "description", "membershipType" };
            requestConfiguration.queryParameters.orderby = new String[] { "displayName" };
        });

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.teams().byTeamId(teamId).channels().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves a channel by its ID.
     *
     * @param teamId The ID of the Team.
     * @param id The ID of the channel.
     * @return The Channel object, or null if not found.
     */
    public Channel getChannelById(final String teamId, final String id) {
        final List<Channel> channelList = new ArrayList<>();
        getChannels(Collections.emptyList(), g -> {
            if (id.equals(g.getId())) {
                channelList.add(g);
            }
        }, teamId);
        if (logger.isDebugEnabled()) {
            channelList.forEach(ToStringBuilder::reflectionToString);
        }
        if (channelList.size() == 1) {
            return channelList.get(0);
        }
        return null;
    }

    /**
     * Retrieves a list of messages from a Team channel, processing each message with the provided consumer.
     * In SDK v6, query options are applied using requestConfiguration lambda.
     *
     * @param options A list of query options for the request (deprecated - kept for API compatibility).
     * @param consumer A consumer to process each ChatMessage object.
     * @param teamId The ID of the Team.
     * @param channelId The ID of the channel.
     */
    public void getTeamMessages(final List<Object> options, final Consumer<ChatMessage> consumer, final String teamId,
            final String channelId) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        ChatMessageCollectionResponse response =
                client.teams().byTeamId(teamId).channels().byChannelId(channelId).messages().get(requestConfiguration -> {
                    // Select only essential fields to improve performance
                    requestConfiguration.queryParameters.select =
                            new String[] { "id", "body", "from", "createdDateTime", "attachments", "messageType" };
                    requestConfiguration.queryParameters.orderby = new String[] { "createdDateTime desc" };
                });

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response =
                    client.teams().byTeamId(teamId).channels().byChannelId(channelId).messages().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves a list of reply messages to a specific message in a Team channel,
     * processing each message with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ChatMessage object.
     * @param teamId The ID of the Team.
     * @param channelId The ID of the channel.
     * @param messageId The ID of the message to retrieve replies for.
     */
    public void getTeamReplyMessages(final List<Object> options, final Consumer<ChatMessage> consumer, final String teamId,
            final String channelId, final String messageId) {
        ChatMessageCollectionResponse response =
                client.teams().byTeamId(teamId).channels().byChannelId(channelId).messages().byChatMessageId(messageId).replies().get();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.teams()
                    .byTeamId(teamId)
                    .channels()
                    .byChannelId(channelId)
                    .messages()
                    .byChatMessageId(messageId)
                    .replies()
                    .withUrl(response.getOdataNextLink())
                    .get();
        }
    }

    /**
     * Retrieves a list of members in a channel, processing each member with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ConversationMember object.
     * @param teamId The ID of the Team.
     * @param channelId The ID of the channel.
     */
    public void getChannelMembers(final List<Object> options, final Consumer<ConversationMember> consumer, final String teamId,
            final String channelId) {
        ConversationMemberCollectionResponse response = client.teams().byTeamId(teamId).channels().byChannelId(channelId).members().get();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response =
                    client.teams().byTeamId(teamId).channels().byChannelId(channelId).members().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves a list of chats, processing each chat with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each Chat object.
     */
    public void getChats(final List<Object> options, final Consumer<Chat> consumer) {
        ChatCollectionResponse response = client.chats().get();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.chats().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves a list of messages from a chat, processing each message with the provided consumer.
     * In SDK v6, query options are applied using requestConfiguration lambda.
     *
     * @param options A list of query options for the request (deprecated - kept for API compatibility).
     * @param consumer A consumer to process each ChatMessage object.
     * @param chatId The ID of the chat.
     */
    public void getChatMessages(final List<Object> options, final Consumer<ChatMessage> consumer, final String chatId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting chat messages for chat: {} with options count: {}", chatId, options != null ? options.size() : 0);
        }

        if (StringUtil.isBlank(chatId)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Chat ID is blank, skipping chat message retrieval");
            }
            return;
        }

        try {
            // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
            ChatMessageCollectionResponse response = client.chats().byChatId(chatId).messages().get(requestConfiguration -> {
                // Select only essential fields to improve performance
                requestConfiguration.queryParameters.select =
                        new String[] { "id", "body", "from", "createdDateTime", "attachments", "messageType" };
                requestConfiguration.queryParameters.orderby = new String[] { "createdDateTime desc" };
                if (logger.isDebugEnabled()) {
                    logger.debug("Request configured with select fields and orderby=createdDateTime desc for chat: {}", chatId);
                }
            });

            int pageCount = 0;
            int totalMessages = 0;

            // Handle pagination with odata.nextLink
            while (response != null && response.getValue() != null) {
                pageCount++;
                final List<ChatMessage> messages = response.getValue();
                totalMessages += messages.size();

                if (logger.isDebugEnabled()) {
                    logger.debug("Processing chat messages page {} with {} messages for chat {} (total so far: {})", pageCount,
                            messages.size(), chatId, totalMessages);
                }

                messages.forEach(consumer::accept);

                // Check if there's a next page
                if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                    // No more pages, exit loop
                    if (logger.isDebugEnabled()) {
                        logger.debug("Chat message pagination completed for chat {} - processed {} pages with total {} messages", chatId,
                                pageCount, totalMessages);
                    }
                    break;
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Found next link for chat messages, continuing to page {} for chat: {}", pageCount + 1, chatId);
                }
                // Request the next page using the nextLink URL
                response = client.chats().byChatId(chatId).messages().withUrl(response.getOdataNextLink()).get();
            }
        } catch (final Exception e) {
            logger.warn("Failed to get chat messages for chat: {}", chatId, e);
            throw e;
        }
    }

    /**
     * Retrieves a list of reply messages to a specific message in a chat,
     * processing each message with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ChatMessage object.
     * @param chatId The ID of the chat.
     * @param messageId The ID of the message to retrieve replies for.
     */
    public void getChatReplyMessages(final List<Object> options, final Consumer<ChatMessage> consumer, final String chatId,
            final String messageId) {
        ChatMessageCollectionResponse response = client.chats().byChatId(chatId).messages().byChatMessageId(messageId).replies().get();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.chats()
                    .byChatId(chatId)
                    .messages()
                    .byChatMessageId(messageId)
                    .replies()
                    .withUrl(response.getOdataNextLink())
                    .get();
        }
    }

    /**
     * Retrieves a chat by its ID.
     *
     * @param id The ID of the chat.
     * @return The Chat object, or null if not found.
     */
    public Chat getChatById(final String id) {
        final List<Chat> chatList = new ArrayList<>();
        getChats(Collections.emptyList(), g -> {
            if (id.equals(g.getId())) {
                chatList.add(g);
            }
        });
        if (logger.isDebugEnabled()) {
            chatList.forEach(ToStringBuilder::reflectionToString);
        }
        if (chatList.size() == 1) {
            return chatList.get(0);
        }
        return null;
    }

    /**
     * Retrieves a list of members in a chat, processing each member with the provided consumer.
     *
     * @param options A list of query options for the request.
     * @param consumer A consumer to process each ConversationMember object.
     * @param chatId The ID of the chat.
     */
    public void getChatMembers(final List<Object> options, final Consumer<ConversationMember> consumer, final String chatId) {
        ConversationMemberCollectionResponse response = client.chats().byChatId(chatId).members().get();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.chats().byChatId(chatId).members().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Retrieves the content of a chat message attachment as a string.
     *
     * @param attachment The ChatMessageAttachment to retrieve content from.
     * @return A string containing the content of the attachment.
     */
    public String getAttachmentContent(final ChatMessageAttachment attachment) {
        if (attachment.getContent() != null || StringUtil.isBlank(attachment.getContentUrl())) {
            return StringUtil.EMPTY;
        }
        // https://learn.microsoft.com/en-us/answers/questions/1072289/download-directly-chat-attachment-using-contenturl
        final String id = "u!" + Base64.getUrlEncoder()
                .encodeToString(attachment.getContentUrl().getBytes(Constants.CHARSET_UTF_8))
                .replaceFirst("=+$", StringUtil.EMPTY)
                .replace('/', '_')
                .replace('+', '-');
        try (InputStream in = client.shares().bySharedDriveItemId(id).driveItem().content().get()) {
            return ComponentUtil.getExtractorFactory()
                    .builder(in, null)
                    .filename(attachment.getName())
                    .maxContentLength(maxContentLength)
                    .extract()
                    .getContent();
        } catch (final Exception e) {
            if (!ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                throw new CrawlingAccessException(e);
            }
            if (logger.isDebugEnabled()) {
                logger.warn("Could not get a text.", e);
            } else {
                logger.warn("Could not get a text. {}", e.getMessage());
            }
            return StringUtil.EMPTY;
        }
    }

    /**
     * Attempts to resolve a user principal name (UPN) from a given identifier.
     * If the identifier is already in UPN format (contains '@'), it is returned as-is.
     * Otherwise, it tries to resolve the UPN using a cache to minimize API calls.
     *
     * @param id The identifier to resolve (could be UPN or object ID).
     * @return The resolved UPN, or null if it cannot be resolved.
     */
    public String tryResolveUserPrincipalName(final String id) {
        if (StringUtil.isBlank(id)) {
            return null;
        }
        if (id.indexOf('@') >= 0) {
            return id;
        }
        try {
            return upnCache.getIfPresent(id);
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to resolve UPN for id={}", id, e);
            }
            return null;
        }
    }

    /**
     * Resolves a user principal name (UPN) from a given object ID by querying the Microsoft Graph API.
     * Implements a simple retry mechanism for transient errors like rate limiting (HTTP 429) or service unavailability (HTTP 503).
     *
     * @param objectId The object ID of the user to resolve.
     * @return The resolved UPN, or null if it cannot be resolved.
     */
    private String doResolveUserPrincipalName(final String objectId) {
        int attempts = 0;
        while (true) {
            attempts++;
            try {
                final User u = client.users().byUserId(objectId).get(rc -> {
                    rc.queryParameters.select = new String[] { "userPrincipalName", "mail", "id" };
                });
                if (u == null) {
                    return null;
                }
                if (StringUtil.isNotBlank(u.getUserPrincipalName())) {
                    return u.getUserPrincipalName();
                }
                if (StringUtil.isNotBlank(u.getMail())) {
                    return u.getMail();
                }
                return null;
            } catch (final ApiException e) {
                final int status = e.getResponseStatusCode();
                if ((status == 429 || status == 503) && attempts == 1) {
                    final long waitMs = parseRetryAfterMillis(e, 2000L, 15000L); // default 2s ~ max 15s
                    if (logger.isDebugEnabled()) {
                        logger.debug("Retrying /users/{} after {} ms due to {}", objectId, waitMs, status, e);
                    }
                    sleepSilently(waitMs);
                    continue;
                }
                if (status == 404) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("User not found for id={}", objectId, e);
                    }
                    return null;
                }
                logger.warn("Failed to resolve UPN for id={} (status={})", objectId, status, e);
                return null;
            } catch (final Exception ex) {
                logger.warn("Failed to resolve UPN for id={}", objectId, ex);
                return null;
            }
        }
    }

    /**
       * Attempts to resolve a group name from a given identifier.
       * If the identifier is already in email format (contains '@'), it is returned as-is.
       * Otherwise, it tries to resolve the group name using a cache to minimize API calls.
       *
       * @param id The identifier to resolve (could be email or object ID).
       * @return The resolved group name, or null if it cannot be resolved.
       */
    public String tryResolveGroupName(final String id) {
        if (StringUtil.isBlank(id)) {
            return null;
        }
        if (id.indexOf('@') >= 0) {
            return id;
        }
        try {
            return groupNameCache.getIfPresent(id);
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to resolve group name for id={}", id, e);
            }
            return null;
        }
    }

    private String doResolveGroupName(final String objectId) {
        try {
            final Group g = client.groups().byGroupId(objectId).get(rc -> {
                rc.queryParameters.select = new String[] { "id", "displayName", "mail", "mailNickname" };
            });
            if (g == null) {
                return null;
            }
            if (StringUtil.isNotBlank(g.getMail())) {
                return g.getMail();
            }
            if (StringUtil.isNotBlank(g.getMailNickname())) {
                return g.getMailNickname();
            }
            if (StringUtil.isNotBlank(g.getDisplayName())) {
                return g.getDisplayName();
            }
            return null;
        } catch (final ApiException e) {
            if (e.getResponseStatusCode() == 404) {
                return null;
            }
            logger.warn("Failed to resolve group name for id={}", objectId, e);
            return null;
        } catch (final Exception e) {
            logger.warn("Failed to resolve group name for id={}", objectId, e);
            return null;
        }
    }

    private long parseRetryAfterMillis(final ApiException e, final long minMs, final long maxMs) {
        try {
            final ResponseHeaders headers = e.getResponseHeaders();
            if (headers != null) {
                final Set<String> values = headers.get("Retry-After");
                if (values != null && !values.isEmpty()) {
                    final String v = values.stream().findFirst().orElse(StringUtil.EMPTY).trim();
                    if (v.matches("\\d+")) {
                        final long ms = Long.parseLong(v) * 1000L;
                        return Math.min(Math.max(ms, minMs), maxMs);
                    }
                    final long epochMs = java.time.ZonedDateTime.parse(v, java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME)
                            .toInstant()
                            .toEpochMilli();
                    final long delta = epochMs - System.currentTimeMillis();
                    return Math.min(Math.max(delta, minMs), maxMs);
                }
            }
        } catch (final Exception ignore) {
            // ignore
        }
        return minMs;
    }

    private void sleepSilently(final long ms) {
        try {
            Thread.sleep(ms);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Retrieves all attachments for a specific list item, processing each attachment with the provided consumer.
     * Implements pagination to handle list items with many attachments.
     *
     * @param siteId The ID of the SharePoint site.
     * @param listId The ID of the SharePoint list.
     * @param itemId The ID of the list item.
     * @param consumer A consumer to process each Attachment object.
     */
    public void getListItemAttachments(final String siteId, final String listId, final String itemId, final Consumer<DriveItem> consumer) {
        if (StringUtil.isBlank(siteId) || StringUtil.isBlank(listId) || StringUtil.isBlank(itemId)) {
            logger.warn("siteId, listId, and itemId cannot be null or empty - Site: {}, List: {}, Item: {}", siteId, listId, itemId);
            return;
        }

        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Checking for attachments in list item - Site: {}, List: {}, Item: {}", siteId, listId, itemId);
            }

            // Get the list item to check for attachments field
            final ListItem listItem = getListItem(siteId, listId, itemId, true);
            if (listItem == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("List item {} not found in list {}", itemId, listId);
                }
                return;
            }

            // Check if the list item has attachments
            boolean hasAttachments = false;
            if (listItem.getFields() != null && listItem.getFields().getAdditionalData() != null) {
                final Object attachmentsField = listItem.getFields().getAdditionalData().get("Attachments");
                if (attachmentsField instanceof Boolean) {
                    hasAttachments = (Boolean) attachmentsField;
                } else if (attachmentsField instanceof String) {
                    hasAttachments = "true".equalsIgnoreCase((String) attachmentsField);
                }
            }

            if (hasAttachments) {
                if (logger.isDebugEnabled()) {
                    logger.debug("List item {} has attachments - creating virtual DriveItem", itemId);
                }

                // Create a virtual DriveItem for list attachment
                // Since we can't get actual attachment details via Graph API, we create a placeholder
                final DriveItem virtualAttachment = new DriveItem();

                // Set basic properties with fallback values
                virtualAttachment.setName("ListAttachment_" + itemId); // Generic name since actual filename unknown
                virtualAttachment.setId("attachment_" + itemId); // Virtual ID

                // Set timestamps from list item
                virtualAttachment.setCreatedDateTime(listItem.getCreatedDateTime());
                virtualAttachment.setLastModifiedDateTime(listItem.getLastModifiedDateTime());

                // Create virtual web URL pointing to list item
                final Site site = getSite(siteId);
                if (site != null && site.getWebUrl() != null) {
                    final String attachmentUrl = String.format("%s/Lists/%s/DispForm.aspx?ID=%s", site.getWebUrl(), listId, itemId);
                    virtualAttachment.setWebUrl(attachmentUrl);
                }

                // Mark as attachment with additional metadata
                final Map<String, Object> additionalData = new HashMap<>();
                additionalData.put(SOURCE_TYPE_KEY, LIST_ATTACHMENT_SOURCE_TYPE);
                additionalData.put(SITE_ID_KEY, siteId);
                additionalData.put(LIST_ID_KEY, listId);
                additionalData.put(LIST_ITEM_ID_KEY, itemId);
                // For now, using a placeholder name since we can't get actual attachment names via Graph API
                additionalData.put(ATTACHMENT_NAME_KEY, "attachment_" + itemId);

                // Add list item title if available
                if (listItem.getFields() != null && listItem.getFields().getAdditionalData() != null) {
                    final Object titleObj = listItem.getFields().getAdditionalData().get("Title");
                    if (titleObj != null) {
                        additionalData.put(LIST_ITEM_TITLE_KEY, titleObj.toString());
                    }
                }

                virtualAttachment.setAdditionalData(additionalData);

                // Pass the virtual attachment to consumer
                consumer.accept(virtualAttachment);

                if (logger.isDebugEnabled()) {
                    logger.debug("Created virtual DriveItem for list item {} attachments", itemId);
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("No attachments found for list item {} in list {}", itemId, listId);
                }
            }

        } catch (final Exception e) {
            logger.warn("Failed to check attachments for list item {} in list {} on site {}: {}", itemId, listId, siteId, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("Full exception for list item attachments check", e);
            }
            // Don't throw exception to avoid breaking the crawling process
        }
    }

    /**
     * Retrieves the content of a specific attachment from a list item as an InputStream.
     *
     * @param siteId The ID of the SharePoint site.
     * @param listId The ID of the SharePoint list.
     * @param itemId The ID of the list item.
     * @param attachmentName The name of the attachment.
     * @return An InputStream containing the attachment content.
     */
    public InputStream getListItemAttachmentContent(final String siteId, final String listId, final String itemId,
            final String attachmentName) {
        if (logger.isDebugEnabled()) {
            logger.debug("Attempting to get content for list attachment {} in item {} of list {} on site {}", attachmentName, itemId,
                    listId, siteId);
        }

        // For now, return placeholder content as Microsoft Graph API doesn't provide direct access to list attachments
        // In a full implementation, this would use SharePoint REST API or other approaches
        final String placeholderContent = String.format(
                "List Attachment Placeholder\n" + "Site ID: %s\n" + "List ID: %s\n" + "Item ID: %s\n" + "Attachment: %s\n"
                        + "Note: Actual content retrieval requires SharePoint REST API integration.",
                siteId, listId, itemId, attachmentName);

        return new java.io.ByteArrayInputStream(placeholderContent.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Retrieves all pages in a SharePoint site.
     *
     * @param siteId The ID of the SharePoint site
     * @param consumer Consumer to process each page
     */
    public void getSitePages(final String siteId, final Consumer<BaseSitePage> consumer) {
        try {
            SitePageCollectionResponse response = client.sites().bySiteId(siteId).pages().graphSitePage().get();

            while (response != null && response.getValue() != null) {
                response.getValue().forEach(consumer::accept);

                // Check if there's a next page
                if ((response.getOdataNextLink() == null) || response.getOdataNextLink().isEmpty()) {
                    // No more pages, exit loop
                    break;
                }
                // Request the next page using the nextLink URL
                response = client.sites().bySiteId(siteId).pages().graphSitePage().withUrl(response.getOdataNextLink()).get();
            }
        } catch (final Exception e) {
            logger.warn("Failed to get pages for site: {} - {}", siteId, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("Exception details for getSitePages:", e);
            }
        }
    }

    /**
     * Retrieves a specific page with full content including canvasLayout.
     *
     * @param siteId The ID of the SharePoint site
     * @param pageId The ID of the page
     * @return BaseSitePage with full content
     */
    public BaseSitePage getPageWithContent(final String siteId, final String pageId) {
        try {
            // Retrieve page with expanded canvasLayout
            return client.sites().bySiteId(siteId).pages().byBaseSitePageId(pageId).graphSitePage().get(requestConfiguration -> {
                requestConfiguration.queryParameters.expand = new String[] { "canvasLayout" };
            });
        } catch (final Exception e) {
            logger.warn("Failed to get page content for page: {} in site: {} - {}", pageId, siteId, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("Exception details for getPageWithContent:", e);
            }
            // Fall back to basic page info without content
            try {
                return client.sites().bySiteId(siteId).pages().byBaseSitePageId(pageId).graphSitePage().get();
            } catch (final Exception ex) {
                logger.error("Failed to get basic page info for page: {} in site: {} - {}", pageId, siteId, ex.getMessage());
                throw new RuntimeException("Unable to retrieve page: " + pageId, ex);
            }
        }
    }

    /**
     * Retrieves permissions for a specific page.
     *
     * @param siteId The ID of the SharePoint site
     * @param pageId The ID of the page
     * @return List of permission principals
     */
    public List<String> getPagePermissions(final String siteId, final String pageId) {
        final List<String> permissions = new ArrayList<>();
        // Page-specific permissions are not directly available through Graph API
        // Fall back to site permissions
        try {
            final List<String> sitePerms = getSitePermissionsAsList(siteId);
            if (sitePerms != null) {
                permissions.addAll(sitePerms);
            }
        } catch (final Exception ex) {
            logger.warn("Failed to get site permissions for site: {} - {}", siteId, ex.getMessage());
        }
        return permissions;
    }

    /**
     * Helper method to get site permissions as a list of strings.
     *
     * @param siteId The ID of the SharePoint site
     * @return List of permission principals
     */
    public List<String> getSitePermissionsAsList(final String siteId) {
        final List<String> permissions = new ArrayList<>();
        try {
            final PermissionCollectionResponse response = getSitePermissions(siteId);
            if (response != null && response.getValue() != null) {
                response.getValue().forEach(permission -> {
                    // Extract permission information manually since assignPermission is not available here
                    if (permission.getGrantedToV2() != null && permission.getGrantedToV2().getUser() != null
                            && permission.getGrantedToV2().getUser().getDisplayName() != null) {
                        permissions.add(permission.getGrantedToV2().getUser().getDisplayName());
                    } else if (permission.getGrantedToV2() != null && permission.getGrantedToV2().getGroup() != null
                            && permission.getGrantedToV2().getGroup().getDisplayName() != null) {
                        permissions.add(permission.getGrantedToV2().getGroup().getDisplayName());
                    }
                });
            }
        } catch (final Exception e) {
            logger.warn("Failed to get site permissions for site: {} - {}", siteId, e.getMessage());
        }
        return permissions;
    }

    /**
     * Retrieves the next page of site pages using pagination.
     *
     * @param siteId The ID of the SharePoint site
     * @param nextLink The next link URL for pagination
     * @return SitePageCollectionResponse containing the next page of site pages
     */
    public SitePageCollectionResponse getSitePagesByNextLink(final String siteId, final String nextLink) {
        if (StringUtil.isBlank(nextLink)) {
            return null;
        }

        try {
            return client.sites().bySiteId(siteId).pages().graphSitePage().withUrl(nextLink).get();
        } catch (final Exception e) {
            logger.warn("Failed to get next page of site pages using nextLink for site: {} - {}", siteId, e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.debug("Exception details for getSitePagesByNextLink:", e);
            }
            return null;
        }
    }
}

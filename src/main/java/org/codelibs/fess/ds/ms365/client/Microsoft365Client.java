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

import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
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
import org.codelibs.core.CoreLibConstants;
import org.codelibs.core.lang.StringUtil;
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
import com.microsoft.graph.models.TeamCollectionResponse;
import com.microsoft.graph.models.User;
import com.microsoft.graph.models.UserCollectionResponse;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.kiota.ApiException;
import com.microsoft.kiota.ResponseHeaders;
import com.microsoft.kiota.serialization.UntypedArray;
import com.microsoft.kiota.serialization.UntypedNode;
import com.microsoft.kiota.serialization.UntypedString;

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

    /** Default cache size for user type, group ID, UPN, and group name caches. */
    protected static final int DEFAULT_CACHE_SIZE = 10000;

    /** The Microsoft Graph service client. */
    protected GraphServiceClient client;
    /** The data store parameters. */
    protected DataStoreParams params;
    /** A cache for user types. */
    protected LoadingCache<String, UserType> userTypeCache;
    /** A cache for group IDs. */
    protected LoadingCache<String, String[]> groupIdCache;
    /** A cache for group names. */
    protected LoadingCache<String, String> groupNameCache;
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
            final ClientSecretCredential credential = new ClientSecretCredentialBuilder().clientId(clientId)
                    .clientSecret(clientSecret)
                    .tenantId(tenant)
                    .additionallyAllowedTenants("*") // Allow all tenants for backward compatibility
                    .build();

            // Initialize GraphServiceClient with new v6 API
            client = new GraphServiceClient(credential);
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a client.", e);
        }

        userTypeCache = CacheBuilder.newBuilder()
                .maximumSize(Integer.parseInt(params.getAsString(CACHE_SIZE, String.valueOf(DEFAULT_CACHE_SIZE))))
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
                .maximumSize(Integer.parseInt(params.getAsString(CACHE_SIZE, String.valueOf(DEFAULT_CACHE_SIZE))))
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
                .maximumSize(Integer.parseInt(params.getAsString(CACHE_SIZE, String.valueOf(DEFAULT_CACHE_SIZE))))
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(final String objectId) {
                        return doResolveUserPrincipalName(objectId);
                    }
                });

        groupNameCache = CacheBuilder.newBuilder()
                .maximumSize(Integer.parseInt(params.getAsString(CACHE_SIZE, String.valueOf(DEFAULT_CACHE_SIZE))))
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
        upnCache.invalidateAll();
        groupNameCache.invalidateAll();
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
                    new String[] { "id", "displayName", "mail", "groupTypes", "resourceProvisioningOptions", "visibility" };
            requestConfiguration.queryParameters.orderby = new String[] { "displayName" };
        });

        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            requestConfiguration.queryParameters.count = true;
            requestConfiguration.headers.add("ConsistencyLevel", "eventual");
        });

        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            groupList.forEach(group -> logger.debug("Group: {}", ToStringBuilder.reflectionToString(group)));
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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

                sites.forEach(site -> {
                    consumer.accept(site);
                    getSiteChildren(site.getId(), consumer);
                });

                // Check if there's a next page
                if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
     * Retrieves child sites (sub-sites) for a specific SharePoint site with pagination support.
     *
     * @param siteId The ID of the parent site.
     * @param consumer A consumer to process each child Site object.
     */
    public void getSiteChildren(final String siteId, final Consumer<Site> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Getting child sites for site: {}", siteId);
        }

        try {
            SiteCollectionResponse response = client.sites().bySiteId(siteId).sites().get();
            int pageCount = 0;
            int totalChildSites = 0;

            // Handle pagination with odata.nextLink
            while (response != null && response.getValue() != null) {
                pageCount++;
                final List<Site> childSites = response.getValue();
                totalChildSites += childSites.size();

                if (logger.isDebugEnabled()) {
                    logger.debug("Processing child sites page {} with {} sites for parent {} (total so far: {})", pageCount,
                            childSites.size(), siteId, totalChildSites);
                }

                childSites.forEach(site -> {
                    consumer.accept(site);
                    getSiteChildren(site.getId(), consumer);
                });

                // Check if there's a next page
                if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Child site pagination completed for {} - processed {} pages with total {} child sites", siteId,
                                pageCount, totalChildSites);
                    }
                    break;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Found next link for child sites, continuing to page {}", pageCount + 1);
                }

                // Request the next page using the nextLink URL
                response = client.sites().bySiteId(siteId).sites().withUrl(response.getOdataNextLink()).get();
            }
        } catch (final com.microsoft.kiota.ApiException e) {
            if (e.getResponseStatusCode() == 404) {
                logger.debug("No child sites found for site: {}", siteId, e);
            } else if (e.getResponseStatusCode() == 403) {
                logger.debug("Access denied to child sites for site: {}", siteId, e);
            } else {
                logger.warn("Failed to get child sites for site: {}", siteId, e);
            }
        } catch (final Exception e) {
            logger.warn("Failed to get child sites for site: {}", siteId, e);
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
                if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
                if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
     * Uses the /teams endpoint directly to ensure all teams are retrieved, including old teams
     * that may not have resourceProvisioningOptions set.
     * For each team, retrieves the corresponding Group object to provide full team details.
     *
     * @param options A list of query options for the request (deprecated - kept for API compatibility).
     * @param consumer A consumer to process each Group object representing a Team.
     */
    public void getTeams(final List<Object> options, final Consumer<Group> consumer) {
        // Use /teams endpoint to get all teams in the organization
        // This ensures we get ALL teams, including old teams that may not have resourceProvisioningOptions set
        TeamCollectionResponse response = client.teams().get();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            // For each team, get the corresponding Group object to provide full details
            response.getValue().forEach(team -> {
                if (team.getId() != null) {
                    try {
                        // Retrieve the Group object using the team's ID
                        // Teams are backed by Microsoft 365 groups, so the team ID is also the group ID
                        Group group = client.groups().byGroupId(team.getId()).get(requestConfiguration -> {
                            // Select essential fields to improve performance
                            requestConfiguration.queryParameters.select = new String[] { "id", "displayName", "mail", "description",
                                    "resourceProvisioningOptions", "visibility" };
                        });
                        if (group != null) {
                            // Validate that this is an active Team by checking resourceProvisioningOptions
                            // This prevents errors when trying to access channels for inactive/archived teams
                            if (isActiveTeam(group)) {
                                consumer.accept(group);
                            } else {
                                if (logger.isDebugEnabled()) {
                                    logger.debug(
                                            "Skipping team {} ({}): not an active Team (missing 'Team' in resourceProvisioningOptions)",
                                            team.getId(), group.getDisplayName());
                                }
                            }
                        }
                    } catch (final Exception e) {
                        logger.warn("Failed to retrieve group details for team ID: {}", team.getId(), e);
                    }
                }
            });

            // Check if there's a next page
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
                // No more pages, exit loop
                break;
            }
            // Request the next page using the nextLink URL
            response = client.teams().withUrl(response.getOdataNextLink()).get();
        }
    }

    /**
     * Checks if a Group is an active Team by verifying the resourceProvisioningOptions property.
     *
     * @param group The Group object to check
     * @return true if the group has "Team" in its resourceProvisioningOptions, false otherwise
     */
    private boolean isActiveTeam(final Group group) {
        final Map<String, Object> additionalDataManager = group.getAdditionalData();
        if (additionalDataManager != null) {
            final Object jsonObj = additionalDataManager.get("resourceProvisioningOptions");
            // Handle UntypedArray (Kiota SDK v6 style)
            if (jsonObj instanceof final UntypedArray untypedArray) {
                for (final UntypedNode node : untypedArray.getValue()) {
                    if (node instanceof final UntypedString untypedString) {
                        if ("Team".equals(untypedString.getValue())) {
                            return true;
                        }
                    }
                }
            } else if (jsonObj instanceof final JsonElement jsonElement && jsonElement.isJsonArray()) {
                // Handle JsonElement (SDK v5 style)
                final JsonArray array = jsonElement.getAsJsonArray();
                for (int i = 0; i < array.size(); i++) {
                    if ("Team".equals(array.get(i).getAsString())) {
                        return true;
                    }
                }
            } else if (jsonObj instanceof final java.util.Collection<?> collection) {
                // Handle native collection objects (may be used in some SDK versions)
                for (final Object item : collection) {
                    if ("Team".equals(String.valueOf(item))) {
                        return true;
                    }
                }
            } else if (jsonObj instanceof final Object[] array) {
                // Handle object arrays (another possible format)
                for (final Object item : array) {
                    if ("Team".equals(String.valueOf(item))) {
                        return true;
                    }
                }
            }
        }
        return false;
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
            // Note: $select is not supported by the channels API.
            //       All channel properties are returned by default.
        });

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue()
                    .stream()
                    .filter(java.util.Objects::nonNull)
                    .sorted(Comparator.comparing(Channel::getDisplayName, Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER)))
                    .forEach(consumer::accept);

            // Check if there's a next page
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            channelList.forEach(channel -> logger.debug("Channel: {}", ToStringBuilder.reflectionToString(channel)));
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
                    // Note: $select is not supported by the channel messages API.
                    //       All message properties are returned by default.
                    // Note: $orderby is not supported by the channel messages API
                    // Client-side sorting is applied below instead
                });

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            // Sort messages by createdDateTime in descending order (newest first)
            // API doesn't support $orderby, so we sort client-side
            response.getValue()
                    .stream()
                    .filter(java.util.Objects::nonNull)
                    .sorted(Comparator.comparing(ChatMessage::getCreatedDateTime, Comparator.nullsLast(Comparator.reverseOrder())))
                    .forEach(consumer::accept);

            // Check if there's a next page
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            // Chat message endpoint rejects $select/$orderby; rely on default projection.
            ChatMessageCollectionResponse response = client.chats().byChatId(chatId).messages().get();

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
                if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
            chatList.forEach(chat -> logger.debug("Chat: {}", ToStringBuilder.reflectionToString(chat)));
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
            if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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
                .encodeToString(attachment.getContentUrl().getBytes(CoreLibConstants.CHARSET_UTF_8))
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
     * Retrieves attachments for a SharePoint list item using Microsoft Graph API.
     * This method uses the driveItem relationship to access attachments as DriveItem objects.
     *
     * @param siteId The SharePoint site ID
     * @param listId The SharePoint list ID
     * @param itemId The SharePoint list item ID
     * @param consumer Consumer to process each attachment DriveItem
     */
    public void getListItemAttachments(final String siteId, final String listId, final String itemId, final Consumer<DriveItem> consumer) {
        if (siteId == null || listId == null || itemId == null || consumer == null) {
            return;
        }

        // Stage 1: Try DriveItem approach (for documentLibrary and similar templates)
        // Stage 2: Try Attachments field approach (for genericList and other standard templates)
        if (tryDriveItemAttachments(siteId, listId, itemId, consumer) || tryFieldsAttachments(siteId, listId, itemId, consumer)) {
            return;
        }

        // Stage 3: Fallback - no attachments found
        logger.debug("No attachments found for list item: siteId={}, listId={}, itemId={}", siteId, listId, itemId);
    }

    /**
     * Attempts to retrieve attachments using driveItem relationship (for document libraries).
     *
     * @param siteId The SharePoint site ID
     * @param listId The SharePoint list ID
     * @param itemId The SharePoint list item ID
     * @param consumer Consumer to process each attachment DriveItem
     * @return true if successful, false if this list item doesn't have driveItem
     */
    private boolean tryDriveItemAttachments(final String siteId, final String listId, final String itemId,
            final Consumer<DriveItem> consumer) {
        try {
            // First get the list item to check if it has a driveItem
            final DriveItem driveItem =
                    client.sites().bySiteId(siteId).lists().byListId(listId).items().byListItemId(itemId).driveItem().get();
            if (driveItem == null) {
                logger.debug("No driveItem found for list item (likely generic list): siteId={}, listId={}, itemId={}", siteId, listId,
                        itemId);
                return false;
            }

            // Get the drive ID from the driveItem
            final String driveId = driveItem.getParentReference() != null ? driveItem.getParentReference().getDriveId() : null;

            if (driveId == null) {
                logger.debug("No drive ID found in driveItem for list item: siteId={}, listId={}, itemId={}", siteId, listId, itemId);
                return false;
            }

            final String driveItemId = driveItem.getId();
            if (driveItemId == null) {
                logger.debug("No driveItem ID found for list item: siteId={}, listId={}, itemId={}", siteId, listId, itemId);
                return false;
            }

            // Get children (attachments) of the driveItem
            try {
                final DriveItemCollectionResponse childrenResponse =
                        client.drives().byDriveId(driveId).items().byDriveItemId(driveItemId).children().get();

                if (childrenResponse != null && childrenResponse.getValue() != null) {
                    for (final DriveItem attachment : childrenResponse.getValue()) {
                        if (attachment != null) {
                            logger.debug("Processing driveItem attachment: {}", attachment.getName());
                            consumer.accept(attachment);
                        }
                    }

                    // Handle pagination if there are more attachments
                    String nextLink = childrenResponse.getOdataNextLink();
                    while (nextLink != null) {
                        final DriveItemCollectionResponse nextResponse =
                                client.drives().byDriveId(driveId).items().byDriveItemId(driveItemId).children().withUrl(nextLink).get();

                        if (nextResponse != null && nextResponse.getValue() != null) {
                            for (final DriveItem attachment : nextResponse.getValue()) {
                                if (attachment != null) {
                                    logger.debug("Processing paginated driveItem attachment: {}", attachment.getName());
                                    consumer.accept(attachment);
                                }
                            }
                        }
                        nextLink = nextResponse != null ? nextResponse.getOdataNextLink() : null;
                    }
                }
                return true; // Successfully processed driveItem attachments
            } catch (final ApiException e) {
                if (e.getResponseStatusCode() == 404) {
                    logger.debug("No driveItem attachments found for list item: siteId={}, listId={}, itemId={}", siteId, listId, itemId);
                } else {
                    logger.warn("Failed to retrieve driveItem attachments for list item: siteId={}, listId={}, itemId={}", siteId, listId,
                            itemId, e);
                }
                return true; // We successfully tried driveItem approach, even if no attachments found
            }

        } catch (final ApiException e) {
            logger.warn("Failed to access list item for driveItem attachments: siteId={}, listId={}, itemId={}", siteId, listId, itemId, e);
            return false;
        } catch (final Exception e) {
            logger.warn("Unexpected error while retrieving driveItem attachments: siteId={}, listId={}, itemId={}", siteId, listId, itemId,
                    e);
            return false;
        }
    }

    /**
     * Attempts to retrieve attachments using fields approach (for generic lists and other standard templates).
     *
     * @param siteId The SharePoint site ID
     * @param listId The SharePoint list ID
     * @param itemId The SharePoint list item ID
     * @param consumer Consumer to process each attachment DriveItem (created from field data)
     * @return true if successfully processed, false if no attachments or failed
     */
    private boolean tryFieldsAttachments(final String siteId, final String listId, final String itemId,
            final Consumer<DriveItem> consumer) {
        try {
            // Get the list item with expanded fields
            final ListItem listItem =
                    client.sites().bySiteId(siteId).lists().byListId(listId).items().byListItemId(itemId).get(requestConfiguration -> {
                        requestConfiguration.queryParameters.expand = new String[] { "fields" };
                    });

            if (listItem == null) {
                logger.debug("List item not found for fields attachment processing: siteId={}, listId={}, itemId={}", siteId, listId,
                        itemId);
                return false;
            }

            final com.microsoft.graph.models.FieldValueSet fieldValueSet = listItem.getFields();
            if (fieldValueSet == null || fieldValueSet.getAdditionalData() == null) {
                logger.debug("No fields found for list item: siteId={}, listId={}, itemId={}", siteId, listId, itemId);
                return false;
            }

            final Map<String, Object> fields = fieldValueSet.getAdditionalData();

            // Check for Attachments field (standard SharePoint field for generic lists)
            final Object attachmentsField = fields.get("Attachments");
            if (attachmentsField == null) {
                logger.debug("No Attachments field found for list item: siteId={}, listId={}, itemId={}", siteId, listId, itemId);
                return false;
            }

            // Parse attachments field value
            if (!hasAttachments(attachmentsField)) {
                logger.debug("Attachments field indicates no attachments for list item: siteId={}, listId={}, itemId={}", siteId, listId,
                        itemId);
                return true; // Successfully processed (no attachments)
            }

            // Try to get attachment details from other fields or item metadata
            final List<String> attachmentNames = extractAttachmentNames(fields);
            if (attachmentNames.isEmpty()) {
                logger.debug("No attachment names found for list item with attachments: siteId={}, listId={}, itemId={}", siteId, listId,
                        itemId);
                return true; // Successfully processed (no attachment names available)
            }

            // Create virtual DriveItems for each attachment
            int attachmentCount = 0;
            for (final String attachmentName : attachmentNames) {
                if (attachmentName != null && !attachmentName.trim().isEmpty()) {
                    final DriveItem virtualDriveItem =
                            createVirtualDriveItemFromFieldAttachment(attachmentName, siteId, listId, itemId, attachmentCount);
                    attachmentCount++;
                    if (virtualDriveItem != null) {
                        logger.debug("Processing fields-based attachment: {}", attachmentName);
                        consumer.accept(virtualDriveItem);
                    }
                }
            }

            logger.debug("Successfully processed {} field-based attachments for list item: siteId={}, listId={}, itemId={}",
                    attachmentCount, siteId, listId, itemId);
            return true;

        } catch (final ApiException e) {
            if (e.getResponseStatusCode() == 404) {
                logger.debug("List item not found for fields attachment processing: siteId={}, listId={}, itemId={}", siteId, listId,
                        itemId);
            } else {
                logger.info("Failed to retrieve fields-based attachments for list item: siteId={}, listId={}, itemId={}", siteId, listId,
                        itemId, e);
            }
            return false;
        } catch (final Exception e) {
            logger.info("Unexpected error while retrieving fields-based attachments: siteId={}, listId={}, itemId={}", siteId, listId,
                    itemId, e);
            return false;
        }
    }

    /**
     * Checks if the Attachments field indicates that attachments exist.
     *
     * @param attachmentsField The value of the Attachments field
     * @return true if attachments exist, false otherwise
     */
    private boolean hasAttachments(final Object attachmentsField) {
        if (attachmentsField == null) {
            return false;
        }

        final String attachmentsValue = attachmentsField.toString().trim();

        // SharePoint Attachments field typically contains:
        // "1" or "true" = has attachments
        // "0" or "false" = no attachments
        // Or actual attachment filenames
        return "1".equals(attachmentsValue) || "true".equalsIgnoreCase(attachmentsValue)
                || !attachmentsValue.isEmpty() && !"0".equals(attachmentsValue) && !"false".equalsIgnoreCase(attachmentsValue);
    }

    /**
     * Extracts attachment names from SharePoint list item fields.
     * This is a best-effort approach as different list templates may store attachment info differently.
     *
     * @param fields The field values map
     * @return List of attachment names (may be empty if names cannot be determined)
     */
    private List<String> extractAttachmentNames(final Map<String, Object> fields) {
        final List<String> attachmentNames = new ArrayList<>();

        // Try various field patterns that might contain attachment names
        final String[] attachmentFields = { "AttachmentFiles", "Attachments", "FileRef", "FileLeafRef", "File_x0020_Name" };

        for (final String fieldName : attachmentFields) {
            final Object fieldValue = fields.get(fieldName);
            if (fieldValue != null) {
                final String value = fieldValue.toString().trim();
                if (!value.isEmpty() && !"0".equals(value) && !"false".equalsIgnoreCase(value)) {
                    // If it looks like a filename or list of filenames, add them
                    if (value.contains(".") || value.contains(";") || value.contains(",")) {
                        // Split potential multiple filenames
                        final String[] names = value.split("[;,]");
                        for (final String name : names) {
                            final String cleanName = name.trim();
                            if (!cleanName.isEmpty() && cleanName.contains(".")) {
                                attachmentNames.add(cleanName);
                            }
                        }
                    } else if (value.contains(".")) {
                        // Single filename
                        attachmentNames.add(value);
                    }
                }
            }
        }

        // If no specific attachment names found but attachments exist, create generic names
        if (attachmentNames.isEmpty()) {
            final Object attachmentsField = fields.get("Attachments");
            if (hasAttachments(attachmentsField)) {
                // Create a generic attachment entry
                attachmentNames.add("attachment.bin");
            }
        }

        return attachmentNames;
    }

    /**
     * Creates a virtual DriveItem from SharePoint list field-based attachment information.
     *
     * @param attachmentName The name of the attachment
     * @param siteId The SharePoint site ID
     * @param listId The SharePoint list ID
     * @param listItemId The SharePoint list item ID
     * @param attachmentIndex The index of this attachment (for unique IDs)
     * @return Virtual DriveItem representing the attachment
     */
    private DriveItem createVirtualDriveItemFromFieldAttachment(final String attachmentName, final String siteId, final String listId,
            final String listItemId, final int attachmentIndex) {
        if (attachmentName == null || attachmentName.trim().isEmpty()) {
            return null;
        }

        try {
            final DriveItem virtualDriveItem = new DriveItem();

            // Generate a unique ID for the virtual attachment
            final String virtualId = String.format("field-attachment-%s-%s-%s-%d", siteId, listId, listItemId, attachmentIndex);
            virtualDriveItem.setId(virtualId);
            virtualDriveItem.setName(attachmentName);

            // Create a File object to indicate this is a file
            final com.microsoft.graph.models.File file = new com.microsoft.graph.models.File();
            file.setMimeType("application/octet-stream"); // Default mime type, will be detected during processing
            virtualDriveItem.setFile(file);

            // Add metadata to identify this as a fields-based attachment
            final Map<String, Object> additionalData = new HashMap<>();
            additionalData.put("sourceType", "Fields");
            additionalData.put("siteId", siteId);
            additionalData.put("listId", listId);
            additionalData.put("listItemId", listItemId);
            additionalData.put("attachmentName", attachmentName);
            additionalData.put("attachmentIndex", attachmentIndex);
            virtualDriveItem.setAdditionalData(additionalData);

            // Create a web URL for the attachment (approximation)
            final String webUrl = String.format("https://graph.microsoft.com/v1.0/sites/%s/lists/%s/items/%s/attachments/%s", siteId,
                    listId, listItemId, attachmentName);
            virtualDriveItem.setWebUrl(webUrl);

            if (logger.isDebugEnabled()) {
                logger.debug("Created virtual DriveItem for field-based attachment: {} (ID: {})", attachmentName, virtualId);
            }

            return virtualDriveItem;

        } catch (final Exception e) {
            logger.warn("Failed to create virtual DriveItem from field attachment: {}", attachmentName, e);
            return null;
        }
    }

    /**
     * Retrieves the content of a specific SharePoint list item attachment using Microsoft Graph API.
     * This method accesses the attachment through the driveItem relationship.
     *
     * @param siteId The SharePoint site ID
     * @param listId The SharePoint list ID
     * @param itemId The SharePoint list item ID
     * @param attachmentName The name of the attachment to retrieve
     * @return InputStream containing the attachment content, or null if not found
     */
    public InputStream getListItemAttachmentContent(final String siteId, final String listId, final String itemId,
            final String attachmentName) {
        if (siteId == null || listId == null || itemId == null || attachmentName == null) {
            return null;
        }

        try {
            // First get the list item to access its driveItem
            final ListItem listItem =
                    client.sites().bySiteId(siteId).lists().byListId(listId).items().byListItemId(itemId).get(requestConfiguration -> {
                        requestConfiguration.queryParameters.expand = new String[] { "driveItem" };
                    });

            if (listItem == null || listItem.getDriveItem() == null) {
                logger.debug("List item or driveItem not found: siteId={}, listId={}, itemId={}", siteId, listId, itemId);
                return null;
            }

            final DriveItem driveItem = listItem.getDriveItem();
            final String driveId = driveItem.getParentReference() != null ? driveItem.getParentReference().getDriveId() : null;

            if (driveId == null || driveItem.getId() == null) {
                logger.debug("Drive ID or driveItem ID not found: siteId={}, listId={}, itemId={}", siteId, listId, itemId);
                return null;
            }

            // Search for the attachment by name in the driveItem children
            final DriveItemCollectionResponse childrenResponse =
                    client.drives().byDriveId(driveId).items().byDriveItemId(driveItem.getId()).children().get();

            if (childrenResponse != null && childrenResponse.getValue() != null) {
                for (final DriveItem attachment : childrenResponse.getValue()) {
                    if (attachment != null && attachmentName.equals(attachment.getName())) {
                        // Found the attachment, get its content
                        return client.drives().byDriveId(driveId).items().byDriveItemId(attachment.getId()).content().get();
                    }
                }
            }

            logger.debug("Attachment not found: siteId={}, listId={}, itemId={}, attachmentName={}", siteId, listId, itemId,
                    attachmentName);
            return null;

        } catch (final ApiException e) {
            logger.warn("Failed to retrieve attachment content: siteId={}, listId={}, itemId={}, attachmentName={}", siteId, listId, itemId,
                    attachmentName, e);
            return null;
        } catch (final Exception e) {
            logger.warn("Unexpected error retrieving attachment content: siteId={}, listId={}, itemId={}, attachmentName={}", siteId,
                    listId, itemId, attachmentName, e);
            return null;
        }
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
                if (response.getOdataNextLink() == null || response.getOdataNextLink().isEmpty()) {
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

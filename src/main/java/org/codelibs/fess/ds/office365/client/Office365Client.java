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
package org.codelibs.fess.ds.office365.client;

import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
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
import com.microsoft.kiota.ApiException;
import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.Chat;
import com.microsoft.graph.models.ChatMessage;
import com.microsoft.graph.models.ChatMessageAttachment;
import com.microsoft.graph.models.ConversationMember;
import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.OnenotePage;
import com.microsoft.graph.models.OnenoteSection;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.models.User;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.graph.models.ChannelCollectionResponse;
import com.microsoft.graph.models.ChatCollectionResponse;
import com.microsoft.graph.models.ChatMessageCollectionResponse;
import com.microsoft.graph.models.ConversationMemberCollectionResponse;
import com.microsoft.graph.models.DriveCollectionResponse;
import com.microsoft.graph.models.DriveItemCollectionResponse;
import com.microsoft.graph.models.GroupCollectionResponse;
import com.microsoft.graph.models.NotebookCollectionResponse;
import com.microsoft.graph.models.OnenotePageCollectionResponse;
import com.microsoft.graph.models.OnenoteSectionCollectionResponse;
import com.microsoft.graph.models.PermissionCollectionResponse;
import com.microsoft.graph.models.UserCollectionResponse;
import java.util.Map;

import okhttp3.Request;

/**
 * This class provides a client for accessing Microsoft Office 365 services using the Microsoft Graph API.
 * It handles authentication, and provides methods for interacting with services like OneDrive, OneNote, and Teams.
 * This client is designed to be used within the Fess data store framework.
 */
public class Office365Client implements Closeable {

    private static final Logger logger = LogManager.getLogger(Office365Client.class);

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
    /** The parameter name for the user type cache size. */
    protected static final String USER_TYPE_CACHE_SIZE = "user_type_cache_size";
    /** The parameter name for the group ID cache size. */
    protected static final String GROUP_ID_CACHE_SIZE = "group_id_cache_size";
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

    /** The maximum content length for extracted text. */
    protected int maxContentLength = -1;

    /**
     * Constructs a new Office365Client with the specified data store parameters.
     *
     * @param params The data store parameters for configuration.
     */
    public Office365Client(final DataStoreParams params) {
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
        } catch (NumberFormatException e) {
            logger.warn("Failed to parse {}.", params.getAsString(MAX_CONTENT_LENGTH), e);
        }

        try {
            // Add multi-tenant authentication support for Azure Identity v1.16.3
            final ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder().clientId(clientId)
                    .clientSecret(clientSecret).tenantId(tenant).additionallyAllowedTenants("*") // Allow all tenants for backward compatibility
                    .build();

            // Initialize GraphServiceClient with new v6 API
            client = new GraphServiceClient(clientSecretCredential);
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a client.", e);
        }

        userTypeCache = CacheBuilder.newBuilder().maximumSize(Integer.parseInt(params.getAsString(USER_TYPE_CACHE_SIZE, "10000")))
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

        groupIdCache = CacheBuilder.newBuilder().maximumSize(Integer.parseInt(params.getAsString(GROUP_ID_CACHE_SIZE, "10000")))
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
    }

    @Override
    public void close() {
        userTypeCache.invalidateAll();
        groupIdCache.invalidateAll();
    }

    /**
     * An enumeration of user types in Office 365.
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
            return UserType.UNKNOWN;
        }
        try {
            return userTypeCache.get(id);
        } catch (final ExecutionException e) {
            logger.warn("Failed to get an user type.", e);
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
        return client.drives().byDriveId(driveId).items().byDriveItemId(itemId).content().get();
    }

    /**
     * Retrieves the permissions for a drive item.
     *
     * @param driveId The ID of the drive.
     * @param itemId The ID of the drive item.
     * @return A PermissionCollectionResponse containing the permissions.
     */
    public PermissionCollectionResponse getDrivePermissions(final String driveId, final String itemId) {
        return client.drives().byDriveId(driveId).items().byDriveItemId(itemId).permissions().get();
    }

    /**
     * Retrieves a page of drive items within a drive.
     *
     * @param driveId The ID of the drive.
     * @param itemId The ID of the parent drive item, or null for the root.
     * @return A DriveItemCollectionResponse containing the drive items.
     */
    public DriveItemCollectionResponse getDriveItemPage(final String driveId, final String itemId) {
        if (itemId == null) {
            return client.drives().byDriveId(driveId).items().byDriveItemId("root").children().get();
        }
        return client.drives().byDriveId(driveId).items().byDriveItemId(itemId).children().get();
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
        return client.drives().byDriveId(driveId).items().byDriveItemId(itemId).permissions().withUrl(nextLink).get();
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
     *
     * @param options A list of query options for the request (deprecated - kept for API compatibility).
     * @param consumer A consumer to process each User object.
     */
    public void getUsers(final List<Object> options, final Consumer<User> consumer) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        UserCollectionResponse response = client.users().get(requestConfiguration -> {
            // Select only essential fields to improve performance
            requestConfiguration.queryParameters.select =
                    new String[] { "id", "displayName", "mail", "userPrincipalName", "assignedLicenses" };
            // Filter for licensed users only to reduce data transfer
            requestConfiguration.queryParameters.filter = "assignedLicenses/$count ne 0";
            requestConfiguration.queryParameters.orderby = new String[] { "displayName" };
            // Required for advanced queries
            requestConfiguration.headers.add("ConsistencyLevel", "eventual");
        });

        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.users().withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.groups().withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
        }
    }

    /**
     * Retrieves Office 365 groups (Unified groups) only, processing each group with the provided consumer.
     * This method filters for groups with groupTypes containing 'Unified' at the server level for efficiency.
     *
     * @param consumer A consumer to process each Group object.
     */
    public void getOffice365Groups(final Consumer<Group> consumer) {
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        GroupCollectionResponse response = client.groups().get(requestConfiguration -> {
            // Filter for Office 365 groups (Unified groups) only at server level
            requestConfiguration.queryParameters.filter = "groupTypes/any(c:c eq 'Unified')";
            // Select only essential fields to improve performance
            requestConfiguration.queryParameters.select =
                    new String[] { "id", "displayName", "mail", "groupTypes", "resourceProvisioningOptions" };
            requestConfiguration.queryParameters.orderby = new String[] { "displayName" };
            // Required for advanced queries
            requestConfiguration.headers.add("ConsistencyLevel", "eventual");
        });

        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.groups().withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
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
        } else {
            return client.me().onenote().notebooks().get();
        }
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                if (userId != null) {
                    response = client.users().byUserId(userId).onenote().notebooks().byNotebookId(notebookId).sections()
                            .withUrl(response.getOdataNextLink()).get();
                } else {
                    response = client.me().onenote().notebooks().byNotebookId(notebookId).sections().withUrl(response.getOdataNextLink())
                            .get();
                }
            } else {
                // No more pages, exit loop
                break;
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                if (userId != null) {
                    response = client.users().byUserId(userId).onenote().sections().byOnenoteSectionId(sectionId).pages()
                            .withUrl(response.getOdataNextLink()).get();
                } else {
                    response = client.me().onenote().sections().byOnenoteSectionId(sectionId).pages().withUrl(response.getOdataNextLink())
                            .get();
                }
            } else {
                // No more pages, exit loop
                break;
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
                (userId != null ? client.users().byUserId(userId).onenote().pages().byOnenotePageId(page.getId()).content().get()
                        : client.me().onenote().pages().byOnenotePageId(page.getId()).content().get())) {
            sb.append(ComponentUtil.getExtractorFactory().builder(in, Collections.emptyMap()).maxContentLength(maxContentLength).extract()
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
        return client.sites().bySiteId(StringUtil.isNotBlank(id) ? id : "root").get();
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
        return client.drives().byDriveId(driveId).get();
    }

    /**
     * Retrieves all drives, processing each drive with the provided consumer.
     * Implements pagination to handle large tenant environments with many SharePoint drives.
     *
     * @param consumer A consumer to process each Drive object.
     */
    // for testing
    protected void getDrives(final Consumer<Drive> consumer) {
        DriveCollectionResponse response = client.drives().get();

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.drives().withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
        }
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
            requestConfiguration.queryParameters.orderby = new String[] { "displayName" };
            // Required for advanced queries
            requestConfiguration.headers.add("ConsistencyLevel", "eventual");
        });
        final Consumer<Group> filter = g -> {
            final Map<String, Object> additionalDataManager = g.getAdditionalData();
            if (additionalDataManager != null) {
                final Object jsonObj = additionalDataManager.get("resourceProvisioningOptions");
                // Handle both JsonElement (SDK v5 style) and native List/Collection (SDK v6 style)
                if (jsonObj instanceof JsonElement jsonElement && jsonElement.isJsonArray()) {
                    final JsonArray array = jsonElement.getAsJsonArray();
                    for (int i = 0; i < array.size(); i++) {
                        if ("Team".equals(array.get(i).getAsString())) {
                            consumer.accept(g);
                            return;
                        }
                    }
                } else if (jsonObj instanceof java.util.Collection<?> collection) {
                    // Handle native collection objects (SDK v6 may provide List<String> directly)
                    for (Object item : collection) {
                        if ("Team".equals(String.valueOf(item))) {
                            consumer.accept(g);
                            return;
                        }
                    }
                } else if (jsonObj instanceof Object[] array) {
                    // Handle object arrays (another possible format)
                    for (Object item : array) {
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.groups().withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.teams().byTeamId(teamId).channels().withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.teams().byTeamId(teamId).channels().byChannelId(channelId).messages().withUrl(response.getOdataNextLink())
                        .get();
            } else {
                // No more pages, exit loop
                break;
            }
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.teams().byTeamId(teamId).channels().byChannelId(channelId).messages().byChatMessageId(messageId).replies()
                        .withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.teams().byTeamId(teamId).channels().byChannelId(channelId).members().withUrl(response.getOdataNextLink())
                        .get();
            } else {
                // No more pages, exit loop
                break;
            }
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.chats().withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
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
        // Microsoft Graph SDK v6 uses requestConfiguration instead of QueryOption
        ChatMessageCollectionResponse response = client.chats().byChatId(chatId).messages().get(requestConfiguration -> {
            // Select only essential fields to improve performance
            requestConfiguration.queryParameters.select =
                    new String[] { "id", "body", "from", "createdDateTime", "attachments", "messageType" };
            requestConfiguration.queryParameters.orderby = new String[] { "createdDateTime desc" };
        });

        // Handle pagination with odata.nextLink
        while (response != null && response.getValue() != null) {
            response.getValue().forEach(consumer::accept);

            // Check if there's a next page
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.chats().byChatId(chatId).messages().withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.chats().byChatId(chatId).messages().byChatMessageId(messageId).replies()
                        .withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
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
            if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                // Request the next page using the nextLink URL
                response = client.chats().byChatId(chatId).members().withUrl(response.getOdataNextLink()).get();
            } else {
                // No more pages, exit loop
                break;
            }
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
        final String id = "u!" + Base64.getUrlEncoder().encodeToString(attachment.getContentUrl().getBytes(Constants.CHARSET_UTF_8))
                .replaceFirst("=+$", StringUtil.EMPTY).replace('/', '_').replace('+', '-');
        try (InputStream in = client.shares().bySharedDriveItemId(id).driveItem().content().get()) {
            return ComponentUtil.getExtractorFactory().builder(in, null).filename(attachment.getName()).maxContentLength(maxContentLength)
                    .extract().getContent();
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
}

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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.Permission;
import com.microsoft.graph.models.PermissionCollectionResponse;
import com.microsoft.graph.models.User;

/**
 * This is an abstract base class for Microsoft 365 data stores.
 * It provides common functionality for accessing Microsoft 365 services,
 * such as user and group management, and thread pool creation.
 */
public abstract class Microsoft365DataStore extends AbstractDataStore {

    private static final Logger logger = LogManager.getLogger(Microsoft365DataStore.class);

    /**
     * Default constructor.
     */
    public Microsoft365DataStore() {
        super();
    }

    /**
     * Creates a Microsoft365Client instance with the specified parameters.
     *
     * @param paramMap the data store parameters
     * @return a new Microsoft365Client instance
     */
    protected Microsoft365Client createClient(final DataStoreParams paramMap) {
        return new Microsoft365Client(paramMap);
    }

    /**
     * Retrieves all licensed users and processes them with the provided consumer.
     * Since Microsoft Graph API doesn't support complex filters on assignedLicenses,
     * we retrieve all users and filter them client-side for licenses.
     *
     * @param client The Microsoft365Client to use for the request.
     * @param consumer A consumer to process each licensed User object.
     */
    protected void getLicensedUsers(final Microsoft365Client client, final Consumer<User> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting licensed users retrieval - using client-side filtering for licenses");
        }

        // Get all users without server-side filtering due to API limitations
        client.getUsers(Collections.emptyList(), user -> {

            if (logger.isDebugEnabled()) {
                logger.debug("Processing user: {} (ID: {}) - Licenses: {}", user.getDisplayName(), user.getId(),
                        user.getAssignedLicenses() != null ? user.getAssignedLicenses().size() : 0);
            }

            // Check if user has any assigned licenses client-side
            if (user.getAssignedLicenses() != null && !user.getAssignedLicenses().isEmpty()) {
                // User has licenses, process them
                consumer.accept(user);
            }
            // Skip users without licenses silently
        });

        if (logger.isDebugEnabled()) {
            logger.debug("Licensed users retrieval completed");
        }
    }

    /**
     * Creates a new fixed-size thread pool for executing tasks concurrently.
     *
     * @param nThreads The number of threads in the pool.
     * @return A new ExecutorService with a fixed thread pool.
     */
    protected ExecutorService newFixedThreadPool(final int nThreads) {
        if (logger.isDebugEnabled()) {
            logger.debug("Executor Thread Pool: {}", nThreads);
        }
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(nThreads),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * Checks if a user is licensed by their ID.
     * Uses optimized field selection to get only assignedLicenses field.
     *
     * @param client The Microsoft365Client to use for the request.
     * @param userId The ID of the user to check.
     * @return true if the user is licensed, false otherwise.
     */
    protected boolean isLicensedUser(final Microsoft365Client client, final String userId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Checking license status for user: {}", userId);
        }

        try {
            // Use getUserForLicenseCheck to get only assignedLicenses field for efficiency
            final User user = client.getUserForLicenseCheck(userId);
            final boolean isLicensed = user.getAssignedLicenses().stream().anyMatch(license -> Objects.nonNull(license.getSkuId()));

            if (logger.isDebugEnabled()) {
                logger.debug("User license check result - User: {}, Licensed: {}, License count: {}", userId, isLicensed,
                        user.getAssignedLicenses() != null ? user.getAssignedLicenses().size() : 0);
            }

            return isLicensed;
        } catch (Exception e) {
            logger.warn("Failed to check license status for user: {}", userId, e);
            return false;
        }
    }

    /**
     * Retrieves the roles for a user.
     *
     * @param user The user to retrieve roles for.
     * @return A list of role strings for the user.
     */
    protected List<String> getUserRoles(final User user) {
        if (logger.isDebugEnabled()) {
            logger.debug("Generating user roles for user: {} (ID: {})", user.getDisplayName(), user.getId());
        }

        final String role = ComponentUtil.getSystemHelper().getSearchRoleByUser(user.getId());
        final List<String> roles = Collections.singletonList(role);

        if (logger.isDebugEnabled()) {
            logger.debug("Generated role for user {}: {}", user.getDisplayName(), role);
        }

        return roles;
    }

    /**
     * Retrieves all Microsoft 365 groups and processes them with the provided consumer.
     * In Microsoft Graph SDK v6, the Microsoft365Client.getMicrosoft365Groups() already filters for Unified groups,
     * so no additional filtering is needed here.
     *
     * @param client The Microsoft365Client to use for the request.
     * @param consumer A consumer to process each Group object.
     */
    protected void getMicrosoft365Groups(final Microsoft365Client client, final Consumer<Group> consumer) {
        if (logger.isDebugEnabled()) {
            logger.debug("Starting Microsoft 365 groups retrieval - filtering for Unified groups");
        }

        // Microsoft365Client.getMicrosoft365Groups() in v6 already filters for Unified groups using:
        // filter: "groupTypes/any(c:c eq 'Unified')"
        // So no additional client-side filtering is needed
        client.getMicrosoft365Groups(group -> {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing Microsoft 365 group: {} (ID: {}) - Mail: {}", group.getDisplayName(), group.getId(),
                        group.getMail());
            }

            consumer.accept(group);
        });

        if (logger.isDebugEnabled()) {
            logger.debug("Microsoft 365 groups retrieval completed");
        }
    }

    /**
     * Retrieves the roles for a group.
     *
     * @param group The group to retrieve roles for.
     * @return A list of role strings for the group.
     */
    protected List<String> getGroupRoles(final Group group) {
        if (logger.isDebugEnabled()) {
            logger.debug("Generating group roles for group: {} (ID: {})", group.getDisplayName(), group.getId());
        }

        final String role = ComponentUtil.getSystemHelper().getSearchRoleByGroup(group.getId());
        final List<String> roles = Collections.singletonList(role);

        if (logger.isDebugEnabled()) {
            logger.debug("Generated role for group {}: {}", group.getDisplayName(), role);
        }

        return roles;
    }

    /**
     * Gets the permissions for a drive item.
     *
     * @param client The Microsoft365Client.
     * @param driveId The drive ID.
     * @param item The drive item.
     * @return A list of permissions.
     */
    protected List<String> getDriveItemPermissions(final Microsoft365Client client, final String driveId, final DriveItem item) {
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieving permissions for drive item - Drive: {}, Item: {}, ItemId: {}", driveId, item.getName(), item.getId());
        }

        final List<String> permissions = new ArrayList<>();
        try {
            PermissionCollectionResponse response = client.getDrivePermissions(driveId, item.getId());
            final Consumer<Permission> consumer = p -> {
                if (p.getGrantedToV2() != null) {
                    assignPermission(client, permissions, p);
                }
            };

            // Handle pagination with odata.nextLink
            while (response != null && response.getValue() != null) {
                response.getValue().forEach(consumer);

                // Check if there's a next page
                if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                    // Request the next page using a helper method in Microsoft365Client
                    try {
                        response = client.getDrivePermissionsByNextLink(driveId, item.getId(), response.getOdataNextLink());
                    } catch (final Exception e) {
                        logger.warn("Failed to get next page of permissions: {}", e.getMessage());
                        break;
                    }
                } else {
                    // No more pages, exit loop
                    break;
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully retrieved {} permissions for drive item: {}", permissions.size(), item.getName());
            }
        } catch (final Exception e) {
            logger.warn("Failed to retrieve permissions for drive item: {} - {}", item.getName(), e.getMessage());
        }
        return permissions;
    }

    /**
     * Retrieves and processes permissions for a SharePoint site, converting them to role strings.
     *
     * @param client The Microsoft365Client instance to use for API calls
     * @param siteId The ID of the SharePoint site
     * @return List of permission strings in the format "user:email" or "group:id"
     */
    protected List<String> getSitePermissions(final Microsoft365Client client, final String siteId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Retrieving permissions for site - SiteId: {}", siteId);
        }

        final List<String> permissions = new ArrayList<>();
        try {
            PermissionCollectionResponse response = client.getSitePermissions(siteId);
            final Consumer<Permission> consumer = p -> {
                if (p.getGrantedToV2() != null) {
                    assignPermission(client, permissions, p);
                }
            };

            // Handle pagination with odata.nextLink
            while (response != null && response.getValue() != null) {
                response.getValue().forEach(consumer);

                // Check if there's a next page
                if (response.getOdataNextLink() != null && !response.getOdataNextLink().isEmpty()) {
                    // Request the next page using a helper method in Microsoft365Client
                    try {
                        response = client.getSitePermissionsByNextLink(siteId, response.getOdataNextLink());
                    } catch (final Exception e) {
                        logger.warn("Failed to get next page of permissions: {}", e.getMessage());
                        break;
                    }
                } else {
                    // No more pages, exit loop
                    break;
                }
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully retrieved {} permissions for site: {}", permissions.size(), siteId);
            }
        } catch (final Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Failed to retrieve permissions for site: {}", siteId, e);
            } else {
                logger.warn("Failed to retrieve permissions for site: {} - {}", siteId, e.getMessage());
            }
        }
        return permissions;
    }

    /**
     * Assigns a permission to a user or group.
     *
     * @param client The Microsoft365Client.
     * @param permissions The list of permissions.
     * @param permission The permission to assign.
     */
    protected void assignPermission(final Microsoft365Client client, final List<String> permissions, final Permission permission) {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        if (permission.getGrantedToV2() != null) {
            if (permission.getGrantedToV2().getUser() != null) {
                final String oid = permission.getGrantedToV2().getUser().getId();
                permissions.add(systemHelper.getSearchRoleByUser(oid));
                final String principal = client.tryResolveUserPrincipalName(oid);
                if (StringUtil.isNotBlank(principal) && !principal.equals(oid)) {
                    permissions.add(systemHelper.getSearchRoleByUser(principal));
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Assigned permission to user - ID: {}, Principal: {}", oid, principal);

                }
                return;
            }
            if (permission.getGrantedToV2().getGroup() != null) {
                final String gid = permission.getGrantedToV2().getGroup().getId();
                permissions.add(systemHelper.getSearchRoleByGroup(gid));
                final String principal = client.tryResolveGroupName(gid);
                if (StringUtil.isNotBlank(principal) && !principal.equals(gid)) {
                    permissions.add(systemHelper.getSearchRoleByGroup(principal));
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Assigned permission to group - ID: {}, Principal: {}", gid, principal);
                }
                return;
            }
        }
        if (permission.getLink() != null) {
            final var link = permission.getLink();
            if ("organization".equalsIgnoreCase(link.getScope())) {
                permissions.add(systemHelper.getSearchRoleByGroup("EVERYONE_IN_TENANT"));
            }
            // "anonymous" ?
        }
    }

}

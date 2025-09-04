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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.User;

/**
 * This is an abstract base class for Microsoft 365 data stores.
 * It provides common functionality for accessing Microsoft 365 services,
 * such as user and group management, and thread pool creation.
 */
public abstract class Microsoft365DataStore extends AbstractDataStore {

    /**
     * Default constructor.
     */
    public Microsoft365DataStore() {
        super();
    }

    private static final Logger logger = LogManager.getLogger(Microsoft365DataStore.class);

    /**
     * Retrieves all licensed users and processes them with the provided consumer.
     * Since Microsoft Graph API doesn't support complex filters on assignedLicenses,
     * we retrieve all users and filter them client-side for licenses.
     *
     * @param client The Microsoft365Client to use for the request.
     * @param consumer A consumer to process each licensed User object.
     */
    protected void getLicensedUsers(final Microsoft365Client client, final Consumer<User> consumer) {
        // Get all users without server-side filtering due to API limitations
        client.getUsers(Collections.emptyList(), user -> {
            // Check if user has any assigned licenses client-side
            if (user.getAssignedLicenses() != null && !user.getAssignedLicenses().isEmpty()) {
                // User has licenses, process them
                consumer.accept(user);
            }
            // Skip users without licenses silently
        });
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
        // Use getUserForLicenseCheck to get only assignedLicenses field for efficiency
        final User user = client.getUserForLicenseCheck(userId);
        return user.getAssignedLicenses().stream().anyMatch(license -> Objects.nonNull(license.getSkuId()));
    }

    /**
     * Retrieves the roles for a user.
     *
     * @param user The user to retrieve roles for.
     * @return A list of role strings for the user.
     */
    protected List<String> getUserRoles(final User user) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByUser(user.getId()));
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
        // Microsoft365Client.getMicrosoft365Groups() in v6 already filters for Unified groups using:
        // filter: "groupTypes/any(c:c eq 'Unified')"
        // So no additional client-side filtering is needed
        client.getMicrosoft365Groups(consumer);
    }

    /**
     * Retrieves the roles for a group.
     *
     * @param group The group to retrieve roles for.
     * @return A list of role strings for the group.
     */
    protected List<String> getGroupRoles(final Group group) {
        return Collections.singletonList(ComponentUtil.getSystemHelper().getSearchRoleByGroup(group.getId()));
    }

}

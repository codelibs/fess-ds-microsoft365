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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.util.ComponentUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.microsoft.graph.models.Identity;
import com.microsoft.graph.models.Permission;
import com.microsoft.graph.models.PermissionCollectionResponse;
import com.microsoft.graph.models.SharePointIdentitySet;

/**
 * Covers how Graph permissions become Fess search roles. A role that is not
 * prefix-encoded matches no user, so an encoding slip makes documents silently
 * unreachable rather than raising anything.
 */
public class Microsoft365DataStorePermissionTest extends UnitDsTestCase {

    private SharePointPageDataStore pageDataStore;

    /** A stand-in for the real client: assignPermission always calls tryResolveUserPrincipalName/
     *  tryResolveGroupName on whatever client it is given, so a bare {@code null} would NPE before
     *  the encoding logic under test even runs. Both stub methods return null, i.e. "unresolved". */
    private Microsoft365Client client;

    @Override
    public void setUp(final TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        // systemHelper is not wired into test_app.xml (registering it as a real DI component
        // drags in the systemProperties component chain via SystemHelper#init(), which this
        // minimal test container does not have). getSearchRoleByUser/getSearchRoleByGroup do not
        // depend on init() having run, so a bare instance registered directly is sufficient.
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        pageDataStore = new SharePointPageDataStore();
        client = mock(Microsoft365Client.class);
        when(client.tryResolveUserPrincipalName(org.mockito.ArgumentMatchers.anyString())).thenReturn(null);
        when(client.tryResolveGroupName(org.mockito.ArgumentMatchers.anyString())).thenReturn(null);
    }

    private static Permission userPermission(final String oid) {
        final Identity user = new Identity();
        user.setId(oid);
        user.setDisplayName("Display Name Of " + oid);
        final SharePointIdentitySet granted = new SharePointIdentitySet();
        granted.setUser(user);
        final Permission permission = new Permission();
        permission.setGrantedToV2(granted);
        return permission;
    }

    private static Permission groupPermission(final String gid) {
        final Identity group = new Identity();
        group.setId(gid);
        group.setDisplayName("Display Name Of " + gid);
        final SharePointIdentitySet granted = new SharePointIdentitySet();
        granted.setGroup(group);
        final Permission permission = new Permission();
        permission.setGrantedToV2(granted);
        return permission;
    }

    @Test
    public void test_assignPermission_encodesUserId() {
        final List<String> permissions = new java.util.ArrayList<>();
        pageDataStore.assignPermission(client, permissions, userPermission("oid-1"));

        final String expected = ComponentUtil.getSystemHelper().getSearchRoleByUser("oid-1");
        assertTrue("expected the prefix-encoded user role, got " + permissions, permissions.contains(expected));
        assertFalse("the raw display name must never become a role: " + permissions, permissions.contains("Display Name Of oid-1"));
    }

    @Test
    public void test_assignPermission_encodesGroupId() {
        final List<String> permissions = new java.util.ArrayList<>();
        pageDataStore.assignPermission(client, permissions, groupPermission("gid-1"));

        final String expected = ComponentUtil.getSystemHelper().getSearchRoleByGroup("gid-1");
        assertTrue("expected the prefix-encoded group role, got " + permissions, permissions.contains(expected));
        assertFalse("the raw display name must never become a role: " + permissions, permissions.contains("Display Name Of gid-1"));
    }

    /**
     * Exercises {@link SharePointPageDataStore#getPagePermissions} itself, not just the base
     * class's {@code assignPermission}. Graph has no page-level permission endpoint, so this
     * method must delegate to the site's permissions; if the delegation regresses back to a
     * client-side shortcut that emits raw display names (the removed
     * {@code getSitePermissionsAsList}), this is the test that must go red.
     */
    @Test
    public void test_getPagePermissions_delegatesToSitePermissionsAndEncodesIds() {
        final String siteId = "site-1";
        final String pageId = "page-1";
        final String oid = "oid-42";

        final PermissionCollectionResponse response = new PermissionCollectionResponse();
        response.setValue(List.of(userPermission(oid)));
        when(client.getSitePermissions(siteId)).thenReturn(response);

        final List<String> permissions = pageDataStore.getPagePermissions(client, siteId, pageId, new DataStoreParams());

        final String expected = ComponentUtil.getSystemHelper().getSearchRoleByUser(oid);
        assertTrue("expected the prefix-encoded user role from the site's permissions, got " + permissions, permissions.contains(expected));
        assertFalse("the raw display name must never become a role: " + permissions, permissions.contains("Display Name Of " + oid));
    }

    @Test
    public void test_permissionFailurePolicy_defaultsToSkip() {
        final DataStoreParams paramMap = new DataStoreParams();
        assertEquals("skip", pageDataStore.getPermissionFailurePolicy(paramMap));
    }

    @Test
    public void test_handlePermissionFailure_skipThrowsCrawlingAccessException() {
        final DataStoreParams paramMap = new DataStoreParams();
        try {
            pageDataStore.handlePermissionFailure(paramMap, "https://example.com/doc", new RuntimeException("429"));
            fail("the default policy must not let the document be indexed without an ACL");
        } catch (final PermissionUnavailableException expected) {
            assertTrue("must extend CrawlingAccessException so the existing per-item handler records it",
                    expected instanceof org.codelibs.fess.crawler.exception.CrawlingAccessException);
        }
    }

    @Test
    public void test_handlePermissionFailure_indexWithoutAclReturnsNormally() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("permission_failure_policy", "index_without_acl");
        // returns normally == caller indexes the document with whatever it collected
        pageDataStore.handlePermissionFailure(paramMap, "https://example.com/doc", new RuntimeException("429"));
    }

    @Test
    public void test_handlePermissionFailure_failStopsTheJob() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("permission_failure_policy", "fail");
        try {
            pageDataStore.handlePermissionFailure(paramMap, "https://example.com/doc", new RuntimeException("429"));
            fail("the fail policy must stop the crawl");
        } catch (final PermissionUnavailableException e) {
            fail("the fail policy must not be swallowed as a per-item skip");
        } catch (final RuntimeException expected) {
            // DataStoreException or similar; the point is that it is not a per-item skip
        }
    }
}

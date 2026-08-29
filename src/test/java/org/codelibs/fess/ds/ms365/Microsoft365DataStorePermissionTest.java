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

import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.Identity;
import com.microsoft.graph.models.Permission;
import com.microsoft.graph.models.PermissionCollectionResponse;
import com.microsoft.graph.models.SharePointIdentity;
import com.microsoft.graph.models.SharePointIdentitySet;
import com.microsoft.graph.models.SharingLink;

/**
 * Covers how Graph permissions become Fess search roles. A role that is not
 * prefix-encoded matches no user, so an encoding slip makes documents silently
 * unreachable rather than raising anything.
 */
public class Microsoft365DataStorePermissionTest extends UnitDsTestCase {

    private SharePointPageDataStore pageDataStore;

    private SharePointDocLibDataStore docLibDataStore;

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
        docLibDataStore = new SharePointDocLibDataStore();
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

    // ===== The plural grantedToIdentitiesV2 collection must also be read =====

    @Test
    public void test_assignPermission_readsGrantedToIdentitiesV2Users() {
        final SharePointIdentitySet first = new SharePointIdentitySet();
        final Identity firstUser = new Identity();
        firstUser.setId("11111111-1111-1111-1111-111111111111");
        first.setUser(firstUser);

        final SharePointIdentitySet second = new SharePointIdentitySet();
        final Identity secondUser = new Identity();
        secondUser.setId("22222222-2222-2222-2222-222222222222");
        second.setUser(secondUser);

        final Permission permission = new Permission();
        permission.setGrantedToIdentitiesV2(List.of(first, second));

        final List<String> permissions = new java.util.ArrayList<>();
        pageDataStore.assignPermission(client, permissions, permission);

        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        assertEquals(List.of(systemHelper.getSearchRoleByUser("11111111-1111-1111-1111-111111111111"),
                systemHelper.getSearchRoleByUser("22222222-2222-2222-2222-222222222222")), permissions);
    }

    @Test
    public void test_assignPermission_readsGrantedToIdentitiesV2Groups() {
        final SharePointIdentitySet set = new SharePointIdentitySet();
        final Identity group = new Identity();
        group.setId("33333333-3333-3333-3333-333333333333");
        set.setGroup(group);

        final Permission permission = new Permission();
        permission.setGrantedToIdentitiesV2(List.of(set));

        final List<String> permissions = new java.util.ArrayList<>();
        pageDataStore.assignPermission(client, permissions, permission);

        assertEquals(List.of(ComponentUtil.getSystemHelper().getSearchRoleByGroup("33333333-3333-3333-3333-333333333333")), permissions);
    }

    @Test
    public void test_assignPermission_singularAndPluralAreBothRead() {
        final SharePointIdentitySet singular = new SharePointIdentitySet();
        final Identity singularUser = new Identity();
        singularUser.setId("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        singular.setUser(singularUser);

        final SharePointIdentitySet plural = new SharePointIdentitySet();
        final Identity pluralUser = new Identity();
        pluralUser.setId("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        plural.setUser(pluralUser);

        final Permission permission = new Permission();
        permission.setGrantedToV2(singular);
        permission.setGrantedToIdentitiesV2(List.of(plural));

        final List<String> permissions = new java.util.ArrayList<>();
        pageDataStore.assignPermission(client, permissions, permission);

        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        assertEquals(List.of(systemHelper.getSearchRoleByUser("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                systemHelper.getSearchRoleByUser("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")), permissions);
    }

    @Test
    public void test_assignPermission_linkRoleOnlyWhenNoIdentityMatched() {
        final SharePointIdentitySet set = new SharePointIdentitySet();
        final Identity user = new Identity();
        user.setId("cccccccc-cccc-cccc-cccc-cccccccccccc");
        set.setUser(user);

        final SharingLink link = new SharingLink();
        link.setScope("organization");

        final Permission permission = new Permission();
        permission.setGrantedToIdentitiesV2(List.of(set));
        permission.setLink(link);

        final List<String> permissions = new java.util.ArrayList<>();
        pageDataStore.assignPermission(client, permissions, permission);

        // The named grantee wins; EVERYONE_IN_TENANT is not also added.
        assertEquals(List.of(ComponentUtil.getSystemHelper().getSearchRoleByUser("cccccccc-cccc-cccc-cccc-cccccccccccc")), permissions);
    }

    /**
     * Pins the precedence documented on {@code assignIdentity}: within one identity set a user
     * wins over a group. Without the {@code return} after the user branch, a set naming both
     * would contribute both roles instead of just the user's.
     */
    @Test
    public void test_assignIdentity_userTakesPrecedenceOverGroupWithinOneIdentity() {
        final SharePointIdentitySet set = new SharePointIdentitySet();
        final Identity user = new Identity();
        user.setId("dddddddd-dddd-dddd-dddd-dddddddddddd");
        set.setUser(user);
        final Identity group = new Identity();
        group.setId("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee");
        set.setGroup(group);

        final List<String> permissions = new java.util.ArrayList<>();
        pageDataStore.assignIdentity(client, permissions, set);

        assertEquals(List.of(ComponentUtil.getSystemHelper().getSearchRoleByUser("dddddddd-dddd-dddd-dddd-dddddddddddd")), permissions);
    }

    // ===== SharePoint-local principals cannot be mapped, but must not vanish silently =====

    @Test
    public void test_describeUnmappableIdentity_namesEachUnsupportedPrincipalKind() {
        final SharePointIdentitySet siteGroupSet = new SharePointIdentitySet();
        final SharePointIdentity siteGroup = new SharePointIdentity();
        siteGroup.setId("17");
        siteGroup.setDisplayName("Test Site Group");
        siteGroupSet.setSiteGroup(siteGroup);
        assertEquals("siteGroup", pageDataStore.describeUnmappableIdentity(siteGroupSet));

        final SharePointIdentitySet siteUserSet = new SharePointIdentitySet();
        final SharePointIdentity siteUser = new SharePointIdentity();
        siteUser.setId("9");
        siteUserSet.setSiteUser(siteUser);
        assertEquals("siteUser", pageDataStore.describeUnmappableIdentity(siteUserSet));

        final SharePointIdentitySet appSet = new SharePointIdentitySet();
        final Identity app = new Identity();
        app.setId("89ea5c94-7736-4e25-95ad-3fa95f62b66e");
        appSet.setApplication(app);
        assertEquals("application", pageDataStore.describeUnmappableIdentity(appSet));

        final SharePointIdentitySet userSet = new SharePointIdentitySet();
        final Identity user = new Identity();
        user.setId("dddddddd-dddd-dddd-dddd-dddddddddddd");
        userSet.setUser(user);
        assertNull(pageDataStore.describeUnmappableIdentity(userSet));
    }

    @Test
    public void test_assignIdentity_unmappablePrincipalAddsNoRole() {
        final SharePointIdentitySet set = new SharePointIdentitySet();
        final SharePointIdentity siteGroup = new SharePointIdentity();
        siteGroup.setId("17");
        siteGroup.setDisplayName("Test Site Group");
        set.setSiteGroup(siteGroup);

        final List<String> permissions = new java.util.ArrayList<>();
        pageDataStore.assignIdentity(client, permissions, set);

        // A site-local principal id is not an Entra object id; encoding it would produce a role
        // that matches nobody. It must not silently become a role.
        assertTrue(permissions.isEmpty());
    }

    // ===== Sharing-link permissions must reach assignPermission from every ACL path =====

    @Test
    public void test_getDriveItemPermissions_organizationLinkReachesAssignPermission() {
        final SharingLink link = new SharingLink();
        link.setScope("organization");
        final Permission linkPermission = new Permission();
        linkPermission.setLink(link);
        // grantedToV2 is deliberately left null: this is the shape Graph returns for a
        // sharing link, and the shape the old gate discarded.

        final PermissionCollectionResponse response = new PermissionCollectionResponse();
        response.setValue(List.of(linkPermission));

        final Microsoft365Client mockClient = mock(Microsoft365Client.class);
        when(mockClient.getDrivePermissions("drive-1", "item-1")).thenReturn(response);

        final DriveItem item = new DriveItem();
        item.setId("item-1");
        item.setName("shared.docx");

        final List<String> roles = pageDataStore.getDriveItemPermissions(mockClient, "drive-1", item, new DataStoreParams());

        assertEquals(List.of(ComponentUtil.getSystemHelper().getSearchRoleByGroup("EVERYONE_IN_TENANT")), roles);
    }

    @Test
    public void test_getSitePermissions_organizationLinkReachesAssignPermission() {
        final SharingLink link = new SharingLink();
        link.setScope("organization");
        final Permission linkPermission = new Permission();
        linkPermission.setLink(link);
        // grantedToV2 is deliberately left null: this is the shape Graph returns for a
        // sharing link, and the shape the old gate discarded.

        final PermissionCollectionResponse response = new PermissionCollectionResponse();
        response.setValue(List.of(linkPermission));

        final String siteId = "site-1";
        final Microsoft365Client mockClient = mock(Microsoft365Client.class);
        when(mockClient.getSitePermissions(siteId)).thenReturn(response);

        final List<String> roles = pageDataStore.getSitePermissions(mockClient, siteId, new DataStoreParams());

        assertEquals(List.of(ComponentUtil.getSystemHelper().getSearchRoleByGroup("EVERYONE_IN_TENANT")), roles);
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

    /**
     * Pins the fix that removed the raw {@code default_permissions} addition from
     * {@code getPagePermissions} itself: the caller in {@code storeData} already adds it through
     * {@code permissionHelper::encode}, so a raw addition here would double it up (once encoded,
     * once raw). Without this test, reintroducing the raw addition leaves all other tests green.
     */
    @Test
    public void test_getPagePermissions_doesNotAddRawDefaultPermissions() {
        final String siteId = "site-1";
        final String pageId = "page-1";
        final String rawDefaultPermissions = "{role}admin";

        final PermissionCollectionResponse response = new PermissionCollectionResponse();
        response.setValue(List.of());
        when(client.getSitePermissions(siteId)).thenReturn(response);

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", rawDefaultPermissions);

        final List<String> permissions = pageDataStore.getPagePermissions(client, siteId, pageId, paramMap);

        assertFalse("getPagePermissions must not add the raw default_permissions config string itself; "
                + "the caller applies permissionHelper::encode, got " + permissions, permissions.contains(rawDefaultPermissions));
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

    // ===== Integration coverage for the three rewritten permission-fetch methods =====
    //
    // The tests above exercise handlePermissionFailure directly, which would stay green even
    // if a rewritten method's own catch block reverted to logging a warning and returning a
    // partial result instead of calling handlePermissionFailure at all. The tests below call
    // getSitePermissions / getDriveItemPermissions / getDrivePermissions themselves, through a
    // client mocked to fail, so a regression in any of those three catch blocks is caught here.

    @Test
    public void test_getSitePermissions_defaultPolicy_propagatesPermissionUnavailableException() {
        final String siteId = "site-1";
        when(client.getSitePermissions(siteId)).thenThrow(new RuntimeException("503"));

        try {
            pageDataStore.getSitePermissions(client, siteId, new DataStoreParams());
            fail("a failed lookup must not be indexed with an empty ACL under the default policy");
        } catch (final PermissionUnavailableException expected) {
            // expected
        }
    }

    @Test
    public void test_getSitePermissions_indexWithoutAcl_returnsWithoutThrowing() {
        final String siteId = "site-1";
        when(client.getSitePermissions(siteId)).thenThrow(new RuntimeException("503"));
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("permission_failure_policy", "index_without_acl");

        final List<String> permissions = pageDataStore.getSitePermissions(client, siteId, paramMap);
        assertTrue("index_without_acl must return whatever was collected instead of throwing, got " + permissions, permissions.isEmpty());
    }

    @Test
    public void test_getSitePermissions_nextLinkFailure_defaultPolicy_propagatesPermissionUnavailableException() {
        final String siteId = "site-1";
        final PermissionCollectionResponse firstPage = new PermissionCollectionResponse();
        firstPage.setValue(List.of(userPermission("oid-1")));
        firstPage.setOdataNextLink("https://graph.microsoft.com/v1.0/next-page");
        when(client.getSitePermissions(siteId)).thenReturn(firstPage);
        when(client.getSitePermissionsByNextLink(org.mockito.ArgumentMatchers.eq(siteId), org.mockito.ArgumentMatchers.anyString()))
                .thenThrow(new RuntimeException("429"));

        try {
            pageDataStore.getSitePermissions(client, siteId, new DataStoreParams());
            fail("a failure fetching page 2 must not let page 1's partial results stand in as the complete ACL");
        } catch (final PermissionUnavailableException expected) {
            // expected: the roles named only on page 2 (never fetched) must not be silently dropped
        }
    }

    @Test
    public void test_getDriveItemPermissions_defaultPolicy_propagatesPermissionUnavailableException() {
        final String driveId = "drive-1";
        final DriveItem item = new DriveItem();
        item.setId("item-1");
        item.setWebUrl("https://example.com/item-1");
        when(client.getDrivePermissions(driveId, "item-1")).thenThrow(new RuntimeException("503"));

        try {
            pageDataStore.getDriveItemPermissions(client, driveId, item, new DataStoreParams());
            fail("a failed lookup must not be indexed with an empty ACL under the default policy");
        } catch (final PermissionUnavailableException expected) {
            // expected
        }
    }

    @Test
    public void test_getDriveItemPermissions_indexWithoutAcl_returnsWithoutThrowing() {
        final String driveId = "drive-1";
        final DriveItem item = new DriveItem();
        item.setId("item-1");
        item.setWebUrl("https://example.com/item-1");
        when(client.getDrivePermissions(driveId, "item-1")).thenThrow(new RuntimeException("503"));
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("permission_failure_policy", "index_without_acl");

        final List<String> permissions = pageDataStore.getDriveItemPermissions(client, driveId, item, paramMap);
        assertTrue("index_without_acl must return whatever was collected instead of throwing, got " + permissions, permissions.isEmpty());
    }

    @Test
    public void test_docLibGetDrivePermissions_defaultPolicy_propagatesPermissionUnavailableException() {
        final String driveId = "drive-1";
        when(client.getDrivePermissions(driveId, "root")).thenThrow(new RuntimeException("503"));

        try {
            docLibDataStore.getDrivePermissions(client, driveId, new DataStoreParams());
            fail("a failed lookup must not be indexed with an empty ACL under the default policy");
        } catch (final PermissionUnavailableException expected) {
            // expected
        }
    }

    @Test
    public void test_docLibGetDrivePermissions_indexWithoutAcl_returnsWithoutThrowing() {
        final String driveId = "drive-1";
        when(client.getDrivePermissions(driveId, "root")).thenThrow(new RuntimeException("503"));
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("permission_failure_policy", "index_without_acl");

        final List<String> permissions = docLibDataStore.getDrivePermissions(client, driveId, paramMap);
        assertTrue("index_without_acl must return whatever was collected instead of throwing, got " + permissions, permissions.isEmpty());
    }
}

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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.codelibs.fess.crawler.entity.ExtractData;

import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.Identity;
import com.microsoft.graph.models.ItemReference;
import com.microsoft.graph.models.Permission;
import com.microsoft.graph.models.SensitivityLabel;
import com.microsoft.graph.models.SensitivityLabelAssignment;
import com.microsoft.graph.models.SharePointIdentitySet;

public class OneDriveDataStoreTest extends UnitDsTestCase {

    private static final Logger logger = LogManager.getLogger(OneDriveDataStoreTest.class);

    // for test
    public static final String tenant = "";
    public static final String clientId = "";
    public static final String clientSecret = "";

    private OneDriveDataStore dataStore;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new OneDriveDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    @Test
    public void test_getName() {
        assertEquals("OneDriveDataStore", dataStore.getName());
    }

    @Test
    public void test_getUrl() {
        Map<String, Object> configMap = new HashMap<>();
        DataStoreParams paramMap = new DataStoreParams();
        DriveItem item = new DriveItem();

        assertNull(dataStore.getUrl(configMap, paramMap, null, item));

        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_SHARED);
        item.setWebUrl(
                "https://n2sm.sharepoint.com/sites/test-site/_layouts/15/Doc.aspx?sourcedoc=%X-X-X-X-X%7D&file=test.doc&action=default&mobileredirect=true");
        ItemReference parentRef = new ItemReference();
        parentRef.setPath("/drive/root:/fess-testdata-master/msoffice");
        item.setParentReference(parentRef);
        item.setName("test.doc");
        assertEquals("https://n2sm.sharepoint.com/sites/test-site/Shared%20Documents/fess-testdata-master/msoffice/test.doc",
                dataStore.getUrl(configMap, paramMap, null, item));

        item.setWebUrl("https://n2sm.sharepoint.com/sites/test-site/Shared%20Documents/fess-testdata-master/msoffice/test.doc");
        assertEquals("https://n2sm.sharepoint.com/sites/test-site/Shared%20Documents/fess-testdata-master/msoffice/test.doc",
                dataStore.getUrl(configMap, paramMap, null, item));
    }

    /**
     * Builds a drive item whose webUrl is a {@code /_layouts/} viewer URL, so {@code getUrl} takes
     * the branch that rebuilds a path-based URL.
     *
     * @param parentPath the Graph {@code parentReference.path}, raw or percent-encoded
     * @param name the item's name, which Graph does not encode
     * @return the drive item
     */
    private static Drive drive(final String id) {
        final Drive drive = new Drive();
        drive.setId(id);
        return drive;
    }

    private DriveItem layoutsItem(final String parentPath, final String name) {
        final DriveItem item = new DriveItem();
        item.setWebUrl("https://contoso.sharepoint.com/sites/test-site/_layouts/15/Doc.aspx?sourcedoc=%X-X-X%7D&file=x&action=default");
        final ItemReference parentRef = new ItemReference();
        parentRef.setPath(parentPath);
        item.setParentReference(parentRef);
        item.setName(name);
        return item;
    }

    @Test
    public void test_getUrl_encodesRawParentPath() {
        // Graph returns parentReference.path raw in practice. The rebuilt URL must be encoded like
        // the path-based webUrl of a .txt in the same folder, or one include_pattern cannot match
        // both files.
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_GROUP);

        assertEquals("A raw folder name containing a space must be encoded",
                "https://contoso.sharepoint.com/sites/test-site/Shared%20Documents/Test%20Folder/a.docx",
                dataStore.getUrl(configMap, new DataStoreParams(), null, layoutsItem("/drive/root:/Test Folder", "a.docx")));
        assertEquals("A raw non-ASCII folder name must be encoded",
                "https://contoso.sharepoint.com/sites/test-site/Shared%20Documents/%E8%B3%87%E6%96%99/a.docx",
                dataStore.getUrl(configMap, new DataStoreParams(), null, layoutsItem("/drive/root:/資料", "a.docx")));
    }

    @Test
    public void test_getUrl_rawParentPathWithPlusAndPercent() {
        // A raw "+" is not a space, and a raw "%" that is not an escape must survive decoding.
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_SHARED);

        assertEquals("https://contoso.sharepoint.com/sites/test-site/Shared%20Documents/A%2BB/100%25/a.docx",
                dataStore.getUrl(configMap, new DataStoreParams(), null, layoutsItem("/drive/root:/A+B/100%", "a.docx")));
    }

    @Test
    public void test_getUrl_doesNotDoubleEncodeParentPath() {
        // Graph documents parentReference.path as a percent-encoded path
        // (e.g. "/drive/root:/Documents/my%20file.docx"). Encoding such a segment again turns %20
        // into %2520 and breaks the link.
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_SHARED);

        assertEquals("A folder name containing a space must not be encoded twice",
                "https://contoso.sharepoint.com/sites/test-site/Shared%20Documents/My%20Folder/Sub%20Dir/my%20file.docx", dataStore
                        .getUrl(configMap, new DataStoreParams(), null, layoutsItem("/drive/root:/My%20Folder/Sub%20Dir", "my file.docx")));
    }

    @Test
    public void test_getUrl_doesNotDoubleEncodeNonAsciiParentPath() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_SHARED);

        // "/drive/root:/資料" as Graph percent-encodes it.
        assertEquals("A non-ASCII folder name must not be encoded twice",
                "https://contoso.sharepoint.com/sites/test-site/Shared%20Documents/%E8%B3%87%E6%96%99/%E8%B3%87%E6%96%99.docx",
                dataStore.getUrl(configMap, new DataStoreParams(), null, layoutsItem("/drive/root:/%E8%B3%87%E6%96%99", "資料.docx")));
    }

    @Test
    public void test_getUrl_encodesItemNameExactlyOnce() {
        // DriveItem.name is the raw file name, not a percent-encoded one, so it does need encoding.
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_SHARED);

        assertEquals("The item name is raw and must be encoded once",
                "https://contoso.sharepoint.com/sites/test-site/Shared%20Documents/docs/my%20file.docx",
                dataStore.getUrl(configMap, new DataStoreParams(), null, layoutsItem("/drive/root:/docs", "my file.docx")));
    }

    @Test
    public void test_getUrl_driveCrawlerUsesDriveWebUrl() {
        // A drive's URL segment is fixed at creation; Drive.name is a read-write display name.
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_DRIVE);
        final Drive drive = new Drive();
        drive.setName("Marketing Assets");
        drive.setWebUrl("https://contoso.sharepoint.com/sites/test-site/MktAssets");

        assertEquals("The drive's own webUrl must be used, not its display name",
                "https://contoso.sharepoint.com/sites/test-site/MktAssets/docs/a.docx",
                dataStore.getUrl(configMap, new DataStoreParams(), drive, layoutsItem("/drive/root:/docs", "a.docx")));
    }

    @Test
    public void test_getUrl_driveCrawlerEncodesNameWhenWebUrlMissing() {
        // Without a webUrl the display name is all there is - it must at least be encoded.
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_DRIVE);
        final Drive drive = new Drive();
        drive.setName("Marketing Assets");

        assertEquals("A drive name spliced into a URL must be encoded",
                "https://contoso.sharepoint.com/sites/test-site/Marketing%20Assets/docs/a.docx",
                dataStore.getUrl(configMap, new DataStoreParams(), drive, layoutsItem("/drive/root:/docs", "a.docx")));
    }

    @Test
    public void test_getUrl_sharedCrawlerUsesEachLibrarysWebUrl() {
        // The shared crawler walks every document library of every site, not only
        // "Shared Documents", so an Office file in another library must keep that library's segment.
        // Otherwise include_pattern=.*/DocLib/.* rejects it and the item is indexed under a URL
        // that belongs to a file in "Shared Documents".
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_SHARED);
        final Drive drive = new Drive();
        drive.setName("Crawl Test");
        drive.setWebUrl("https://contoso.sharepoint.com/sites/test-site/DocLib");

        assertEquals("https://contoso.sharepoint.com/sites/test-site/DocLib/msoffice/test.docx",
                dataStore.getUrl(configMap, new DataStoreParams(), drive, layoutsItem("/drives/b!abc/root:/msoffice", "test.docx")));
    }

    @Test
    public void test_getUrl_userDriveKeepsEnglishLibrarySegment() {
        // A library's URL segment does not localize, so the hardcoded English segment is correct.
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_USER);

        assertEquals("https://contoso.sharepoint.com/sites/test-site/Documents/docs/a.docx",
                dataStore.getUrl(configMap, new DataStoreParams(), null, layoutsItem("/drive/root:/docs", "a.docx")));
    }

    @Test
    public void test_getUrlFilter() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test with no include/exclude patterns - should return a UrlFilter instance but behavior depends on implementation
        try {
            UrlFilter filter = dataStore.getUrlFilter(paramMap);
            // UrlFilter is created by ComponentUtil.getComponent() so it may throw exception in test environment
            // This is expected behavior in isolated test environment
            assertNotNull(filter);
        } catch (Exception e) {
            // Expected in test environment where ComponentUtil dependencies are not available
            assertTrue("Expected ComponentNotFoundException or similar",
                    e.getMessage().contains("ComponentNotFound") || e.getMessage().contains("Component"));
        }
    }

    @Test
    public void test_isSharedDocumentsDriveCrawler() {
        DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isSharedDocumentsDriveCrawler(paramMap)); // default is true based on implementation

        paramMap.put(OneDriveDataStore.SHARED_DOCUMENTS_DRIVE_CRAWLER, "false");
        assertFalse(dataStore.isSharedDocumentsDriveCrawler(paramMap));

        paramMap.put(OneDriveDataStore.SHARED_DOCUMENTS_DRIVE_CRAWLER, "true");
        assertTrue(dataStore.isSharedDocumentsDriveCrawler(paramMap));
    }

    @Test
    public void test_isUserDriveCrawler() {
        DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isUserDriveCrawler(paramMap)); // default is true

        paramMap.put(OneDriveDataStore.USER_DRIVE_CRAWLER, "false");
        assertFalse(dataStore.isUserDriveCrawler(paramMap));

        paramMap.put(OneDriveDataStore.USER_DRIVE_CRAWLER, "true");
        assertTrue(dataStore.isUserDriveCrawler(paramMap));
    }

    @Test
    public void test_isGroupDriveCrawler() {
        DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isGroupDriveCrawler(paramMap)); // default is true

        paramMap.put(OneDriveDataStore.GROUP_DRIVE_CRAWLER, "false");
        assertFalse(dataStore.isGroupDriveCrawler(paramMap));

        paramMap.put(OneDriveDataStore.GROUP_DRIVE_CRAWLER, "true");
        assertTrue(dataStore.isGroupDriveCrawler(paramMap));
    }

    @Test
    public void test_isIgnoreFolder() {
        DataStoreParams paramMap = new DataStoreParams();

        assertTrue(dataStore.isIgnoreFolder(paramMap)); // default is true

        paramMap.put(OneDriveDataStore.IGNORE_FOLDER, "false");
        assertFalse(dataStore.isIgnoreFolder(paramMap));

        paramMap.put(OneDriveDataStore.IGNORE_FOLDER, "true");
        assertTrue(dataStore.isIgnoreFolder(paramMap));
    }

    @Test
    public void test_isIgnoreError() {
        DataStoreParams paramMap = new DataStoreParams();

        assertFalse(dataStore.isIgnoreError(paramMap)); // default is false for consistency

        paramMap.put(OneDriveDataStore.IGNORE_ERROR, "false");
        assertFalse(dataStore.isIgnoreError(paramMap));

        paramMap.put(OneDriveDataStore.IGNORE_ERROR, "true");
        assertTrue(dataStore.isIgnoreError(paramMap));
    }

    @Test
    public void test_getMaxSize() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test default value
        assertEquals(OneDriveDataStore.DEFAULT_MAX_SIZE, dataStore.getMaxSize(paramMap));

        // Test custom value
        paramMap.put(OneDriveDataStore.MAX_CONTENT_LENGTH, "1024");
        assertEquals(1024L, dataStore.getMaxSize(paramMap));

        // Test invalid value (non-numeric)
        paramMap.put(OneDriveDataStore.MAX_CONTENT_LENGTH, "invalid");
        assertEquals(OneDriveDataStore.DEFAULT_MAX_SIZE, dataStore.getMaxSize(paramMap));
    }

    /**
     * A malformed {@code max_content_length} used to fall back to {@link OneDriveDataStore#DEFAULT_MAX_SIZE}
     * with no log line at all, unlike every other malformed-value path in this plugin. Pins that a
     * WARN is now emitted, naming the parameter and the value that failed to parse.
     */
    @Test
    public void test_getMaxSize_malformedValueLogsWarning() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.MAX_CONTENT_LENGTH, "invalid");

        final List<LogEvent> events = captureDataStoreWarnings(() -> dataStore.getMaxSize(paramMap));

        assertEquals("a malformed max_content_length must log exactly one warning, got " + messagesOf(events), 1, events.size());
        final String message = events.get(0).getMessage().getFormattedMessage();
        assertTrue("warning must name the parameter, got: " + message, message.contains(OneDriveDataStore.MAX_CONTENT_LENGTH));
        assertTrue("warning must name the offending value, got: " + message, message.contains("invalid"));
    }

    @Test
    public void test_getSupportedMimeTypes() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test default (should return ".*" as array)
        String[] mimeTypes = dataStore.getSupportedMimeTypes(paramMap);
        assertNotNull(mimeTypes);
        assertEquals(1, mimeTypes.length);
        assertEquals(".*", mimeTypes[0]);

        // Test single mime type
        paramMap.put(OneDriveDataStore.SUPPORTED_MIMETYPES, "text/plain");
        mimeTypes = dataStore.getSupportedMimeTypes(paramMap);
        assertNotNull(mimeTypes);
        assertEquals(1, mimeTypes.length);
        assertEquals("text/plain", mimeTypes[0]);

        // Test multiple mime types
        paramMap.put(OneDriveDataStore.SUPPORTED_MIMETYPES, "text/plain,application/pdf,image/jpeg");
        mimeTypes = dataStore.getSupportedMimeTypes(paramMap);
        assertNotNull(mimeTypes);
        assertEquals(3, mimeTypes.length);
        assertEquals("text/plain", mimeTypes[0]);
        assertEquals("application/pdf", mimeTypes[1]);
        assertEquals("image/jpeg", mimeTypes[2]);
    }

    @Test
    public void test_isTargetDrive_skipsSystemLibrariesByDefault() {
        // isSystemLibrary and isIgnoreSystemLibraries existed but were only ever evaluated
        // inside debug log statements, so system libraries were crawled regardless.
        final OneDriveDataStore dataStore = new OneDriveDataStore();
        final DataStoreParams paramMap = new DataStoreParams();

        assertFalse("a style library must be skipped by default",
                dataStore.isTargetDrive(paramMap, driveWithUrl("https://contoso.sharepoint.com/sites/test/Style%20Library")));
        assertTrue("an ordinary document library must be crawled",
                dataStore.isTargetDrive(paramMap, driveWithUrl("https://contoso.sharepoint.com/sites/test/Shared%20Documents")));
    }

    @Test
    public void test_isTargetDrive_ignoreSystemLibrariesFalseKeepsThem() {
        final OneDriveDataStore dataStore = new OneDriveDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_system_libraries", "false");

        assertTrue(dataStore.isTargetDrive(paramMap, driveWithUrl("https://contoso.sharepoint.com/sites/test/Style%20Library")));
    }

    @Test
    public void test_isTargetDrive_skipsPersonalOneDrive() {
        // GET /sites also lists every user's personal site; its drive is that user's OneDrive.
        final OneDriveDataStore dataStore = new OneDriveDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        final Drive drive = driveWithUrl("https://contoso-my.sharepoint.com/personal/user_contoso_com/Documents");
        drive.setDriveType("business");

        assertFalse(dataStore.isTargetDrive(paramMap, drive));
        paramMap.put("ignore_system_libraries", "false");
        assertFalse(dataStore.isTargetDrive(paramMap, drive));
    }

    private static Drive driveWithUrl(final String webUrl) {
        final Drive drive = new Drive();
        drive.setWebUrl(webUrl);
        drive.setDriveType("documentLibrary");
        return drive;
    }

    @Test
    public void test_getUserEmail() {
        // Test with null permission - this will cause NullPointerException based on implementation
        try {
            dataStore.getUserEmail(null);
            fail("Should have thrown NullPointerException");
        } catch (NullPointerException e) {
            // Expected - implementation doesn't handle null input
            assertTrue("Expected NullPointerException", true);
        }

        // Test with permission but no grantedToV2
        Permission permission = new Permission();
        assertNull(dataStore.getUserEmail(permission));

        // Test with user email in id field
        permission = new Permission();
        SharePointIdentitySet identitySet = new SharePointIdentitySet();
        Identity user = new Identity();
        user.setId("user@example.com");
        user.setDisplayName("User Name");
        identitySet.setUser(user);
        permission.setGrantedToV2(identitySet);
        assertEquals("user@example.com", dataStore.getUserEmail(permission));

        // Test with user display name only (no email in id)
        permission = new Permission();
        identitySet = new SharePointIdentitySet();
        user = new Identity();
        user.setId("12345");
        user.setDisplayName("User Display Name");
        identitySet.setUser(user);
        permission.setGrantedToV2(identitySet);
        assertEquals("User Display Name", dataStore.getUserEmail(permission));
    }

    @Test
    public void test_encodeUrl() {
        // Test normal URL encoding - URLEncoder.encode uses + for spaces, then replaces with %20
        assertEquals("hello%20world", dataStore.encodeUrl("hello world"));
        assertEquals("test%2Fpath", dataStore.encodeUrl("test/path"));
        assertEquals("file%26name", dataStore.encodeUrl("file&name"));

        // Test already encoded URLs - these will be double encoded
        assertEquals("hello%2520world", dataStore.encodeUrl("hello%20world"));

        // Test special characters
        assertEquals("test%3Dvalue", dataStore.encodeUrl("test=value"));
        assertEquals("query%3Fparam", dataStore.encodeUrl("query?param"));

        // Test null and empty
        assertEquals("", dataStore.encodeUrl(""));
        assertNull(dataStore.encodeUrl(null)); // encodeUrl returns null for null input
    }

    @Test
    public void testStoreData() {
        // doStoreData();
    }

    @Test
    public void test_driveIdParameter() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test with no drive ID
        assertNull(paramMap.getAsString(OneDriveDataStore.DRIVE_ID));

        // Test with drive ID
        paramMap.put(OneDriveDataStore.DRIVE_ID, "drive123");
        assertEquals("drive123", paramMap.getAsString(OneDriveDataStore.DRIVE_ID));
    }

    @Test
    public void test_defaultPermissions() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test with no default permissions
        assertNull(paramMap.getAsString(OneDriveDataStore.DEFAULT_PERMISSIONS));

        // Test with default permissions
        paramMap.put(OneDriveDataStore.DEFAULT_PERMISSIONS, "{role}admin,{role}user");
        assertEquals("{role}admin,{role}user", paramMap.getAsString(OneDriveDataStore.DEFAULT_PERMISSIONS));
    }

    @Test
    public void test_numberOfThreads() {
        DataStoreParams paramMap = new DataStoreParams();

        // Test default value
        assertEquals("1", paramMap.getAsString(OneDriveDataStore.NUMBER_OF_THREADS, "1"));

        // Test custom value
        paramMap.put(OneDriveDataStore.NUMBER_OF_THREADS, "5");
        assertEquals("5", paramMap.getAsString(OneDriveDataStore.NUMBER_OF_THREADS));
    }

    /*
    private void doStoreData() {
        final TikaExtractor tikaExtractor = new TikaExtractor();
        tikaExtractor.init();
        ComponentUtil.register(tikaExtractor, "tikaExtractor");

        final DataConfig dataConfig = new DataConfig();
        final Map<String, String> paramMap = new HashMap<>();
        paramMap.put("tenant", tenant);
        paramMap.put("client_id", clientId);
        paramMap.put("client_secret", clientSecret);
        final Map<String, String> scriptMap = new HashMap<>();
        final Map<String, Object> defaultDataMap = new HashMap<>();

        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        scriptMap.put(fessConfig.getIndexFieldTitle(), "files.name");
        scriptMap.put(fessConfig.getIndexFieldContent(), "files.description + \"\\n\"+ files.contents");
        scriptMap.put(fessConfig.getIndexFieldMimetype(), "files.mimetype");
        scriptMap.put(fessConfig.getIndexFieldCreated(), "files.created");
        scriptMap.put(fessConfig.getIndexFieldLastModified(), "files.last_modified");
        scriptMap.put(fessConfig.getIndexFieldContentLength(), "files.size");
        scriptMap.put(fessConfig.getIndexFieldUrl(), "files.web_url");
        scriptMap.put(fessConfig.getIndexFieldRole(), "files.roles");

        dataStore.storeData(dataConfig, new TestCallback() {
            @Override
            public void test(Map<String, String> paramMap, Map<String, Object> dataMap) {
                logger.debug(dataMap.toString());
            }
        }, paramMap, scriptMap, defaultDataMap);
    }
    */

    /**
     * {@code processDriveItem} logged "Crawling Access Exception at : {}" from BOTH catch arms,
     * which made OneDrive the only one of the six data stores whose two failure paths could not be
     * told apart in the crawler log. Pins that the texts differ and that the {@code Throwable} arm
     * names what it actually caught.
     *
     * <p>Both stay at {@code WARN} on purpose: {@code ERROR} from {@code org.codelibs} is wired to
     * operator notification in this project, and a single item failing is not one.</p>
     */
    @Test
    public void test_processDriveItem_theTwoCatchArmsAreDistinguishableInTheLog() {
        registerDriveItemProcessingComponents();

        final List<LogEvent> accessArm = captureDataStoreWarnings(() -> processFailingDriveItem(new CrawlingAccessException("denied")));
        final List<LogEvent> throwableArm = captureDataStoreWarnings(() -> processFailingDriveItem(new IllegalStateException("boom")));

        assertEquals("the CrawlingAccessException arm must report once, got " + messagesOf(accessArm), 1, accessArm.size());
        assertEquals("the Throwable arm must report once, got " + messagesOf(throwableArm), 1, throwableArm.size());

        final String accessMessage = accessArm.get(0).getMessage().getFormattedMessage();
        final String throwableMessage = throwableArm.get(0).getMessage().getFormattedMessage();
        assertFalse("the two arms must not be indistinguishable in the log, both said: " + accessMessage,
                accessMessage.equals(throwableMessage));
        assertTrue(accessMessage, accessMessage.startsWith("Crawling Access Exception at : "));
        assertTrue(throwableMessage, throwableMessage.startsWith("Processing exception at : "));

        assertEquals("a per-item failure must not become an operator notification", Level.WARN, accessArm.get(0).getLevel());
        assertEquals("a per-item failure must not become an operator notification", Level.WARN, throwableArm.get(0).getLevel());
    }

    /**
     * A OneDrive item's ACL is assembled from three sources in one place: the item's own Graph
     * permissions, the roles its drive contributed, and the operator-configured
     * {@code default_permissions}; the data config's own Permissions field (seeded into
     * {@code defaultDataMap} under the role index field) is then folded on top.
     *
     * <p>Nothing asserted the roles a OneDrive item is actually indexed with --
     * {@code test_defaultPermissions} above only round-trips a {@link DataStoreParams} entry and
     * never reaches the data store -- so dropping either half left every OneDrive document with a
     * narrower ACL and the suite green. Pins all four contributions and their order.</p>
     */
    @Test
    public void test_processDriveItem_assemblesRolesFromAllFourSources() {
        registerDriveItemProcessingComponents();
        final TestablePermissionHelper permissionHelper = new TestablePermissionHelper();
        permissionHelper.useSystemHelper(ComponentUtil.getSystemHelper());
        ComponentUtil.register(permissionHelper, "permissionHelper");

        // convertValue's real path goes through ComponentUtil.getScriptEngineFactory(), which this
        // unit test has no business standing up -- see OneNoteDataStoreTest's identical seam.
        // "files.roles" is the only template used here, so it is resolved with a direct nested map
        // lookup instead; processDriveItem itself, including the role assembly under test, runs
        // completely unmodified. getDriveItemPermissions and getDriveItemContents are stubbed
        // because they are the only two members that would reach Graph.
        final OneDriveDataStore roleAwareDataStore = new OneDriveDataStore() {
            @Override
            protected List<String> getDriveItemPermissions(final Microsoft365Client client, final String driveId, final DriveItem item,
                    final DataStoreParams paramMap) {
                return new ArrayList<>(List.of("1item-permission"));
            }

            @Override
            protected String getDriveItemContents(final Microsoft365Client client, final String driveId, final DriveItem item,
                    final long maxContentLength, final boolean ignoreError) {
                return "content";
            }

            @Override
            protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
                if ("files.roles".equals(template) && resultMap.get(FILE) instanceof final Map<?, ?> filesMap) {
                    return filesMap.get(FILE_ROLES);
                }
                return super.convertValue(scriptType, template, resultMap);
            }
        };

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.IGNORE_FOLDER, Boolean.FALSE);
        configMap.put(OneDriveDataStore.IGNORE_ERROR, Boolean.FALSE);
        configMap.put(OneDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });
        configMap.put(OneDriveDataStore.MAX_CONTENT_LENGTH, Long.valueOf(1000000L));

        final DriveItem item = new DriveItem();
        item.setId("item-1");
        item.setName("item-1.txt");
        item.setWebUrl("https://example.com/item-1");

        final String roleField = ComponentUtil.getFessConfig().getIndexFieldRole();
        final Map<String, Object> defaultDataMap = new HashMap<>();
        defaultDataMap.put(roleField, List.of("1config-role"));

        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put(roleField, "files.roles");

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.DEFAULT_PERMISSIONS, "{role}admin,{group}sales");

        final List<Map<String, Object>> captured = new ArrayList<>();
        final TestCallback callback = new TestCallback() {
            @Override
            void test(final DataStoreParams params, final Map<String, Object> dataMap) {
                captured.add(dataMap);
            }
        };

        roleAwareDataStore.processDriveItem(new DataConfig(), callback, configMap, paramMap, scriptMap, defaultDataMap, null,
                drive("drive-1"), item, List.of("1drive-role"));

        assertEquals("processDriveItem must have indexed the item exactly once", 1, captured.size());

        @SuppressWarnings("unchecked")
        final List<String> roles = (List<String>) captured.get(0).get(roleField);
        assertEquals("the item's ACL must hold, in order: item permissions, drive roles, default_permissions, then the config's own roles",
                List.of("1item-permission", "1drive-role", permissionHelper.encode("{role}admin"), permissionHelper.encode("{group}sales"),
                        "1config-role"),
                roles);
    }

    /**
     * {@code PermissionHelper#systemHelper} is {@code @Resource}-injected, which plain
     * {@code ComponentUtil.register(...)} does not perform in this minimal test container; this
     * subclass exposes a same-package-crossing setter so the field can be wired by hand.
     */
    private static final class TestablePermissionHelper extends org.codelibs.fess.helper.PermissionHelper {
        void useSystemHelper(final SystemHelper systemHelper) {
            this.systemHelper = systemHelper;
        }
    }

    /**
     * {@code processDriveItem} resolves the stats helper from the container, which in turn needs
     * the system helper; the failure paths resolve {@code FailureUrlService}, which
     * {@code test_app.xml} answers with {@link CapturingFailureUrlService}.
     */
    private static void registerDriveItemProcessingComponents() {
        CapturingFailureUrlService.empty();
        ComponentUtil.register(new SystemHelper(), "systemHelper");
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
    }

    /**
     * Runs one drive item through {@code processDriveItem} with {@code getUrl} rigged to fail, so
     * both catch arms are entered at exactly the same point.
     *
     * @param failure the failure {@code getUrl} raises.
     */
    private void processFailingDriveItem(final RuntimeException failure) {
        final OneDriveDataStore failingDataStore = new OneDriveDataStore() {
            @Override
            protected String getUrl(final Map<String, Object> configMap, final DataStoreParams paramMap, final Drive drive,
                    final DriveItem item) {
                throw failure;
            }
        };

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.IGNORE_FOLDER, Boolean.FALSE);
        configMap.put(OneDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });

        final DriveItem item = new DriveItem();
        item.setId("item-1");
        item.setName("item-1.txt");
        item.setWebUrl("https://example.com/item-1");

        failingDataStore.processDriveItem(new DataConfig(), null, configMap, new DataStoreParams(), Collections.emptyMap(), new HashMap<>(),
                null, drive("drive-1"), item, Collections.emptyList());
    }

    /**
     * Runs {@code action}, returning every record {@link OneDriveDataStore} logged at {@code WARN}
     * or worse while it ran, in order.
     *
     * @param action the code whose logging should be captured.
     * @return the captured records.
     */
    private static List<LogEvent> captureDataStoreWarnings(final Runnable action) {
        final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());
        final org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(OneDriveDataStore.class);
        final AbstractAppender appender = new AbstractAppender("test-ms365-onedrive-capture", null, null, false, Property.EMPTY_ARRAY) {
            @Override
            public void append(final LogEvent event) {
                if (event.getLevel().isMoreSpecificThan(Level.WARN)) {
                    events.add(event.toImmutable());
                }
            }
        };
        appender.start();
        coreLogger.addAppender(appender);
        try {
            action.run();
        } finally {
            coreLogger.removeAppender(appender);
            appender.stop();
        }
        return events;
    }

    /**
     * @param events the captured records.
     * @return their formatted messages, for an assertion failure that can be read.
     */
    private static List<String> messagesOf(final List<LogEvent> events) {
        return events.stream().map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
    }

    /**
     * {@code getUrlFilter} hands include_pattern/exclude_pattern to fess-crawler's
     * {@code UrlFilterImpl}, which logs one WARN for a pattern that does not compile and then
     * drops it - leaving the crawl running with no filter, so a mistyped {@code exclude_pattern}
     * indexes exactly what it was meant to keep out. Pins that the crawl fails at its start
     * instead, the same way the three {@code getPattern} DataStores now do.
     */
    @Test
    public void test_storeData_malformedExcludePatternFailsBeforeAnyGraphCall() {
        final java.util.concurrent.atomic.AtomicInteger clientsCreated = new java.util.concurrent.atomic.AtomicInteger();
        final OneDriveDataStore testDataStore = new OneDriveDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                clientsCreated.incrementAndGet();
                throw new AssertionError("storeData must fail on the malformed pattern before creating a client");
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_pattern", ".*secret.*[");

        final DataStoreException e = assertThrows(DataStoreException.class,
                () -> testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>()));
        assertTrue("the failure must name the parameter, got: " + e.getMessage(), e.getMessage().contains("exclude_pattern"));
        assertEquals("no Graph client may be created for a crawl that cannot honour its own filter", 0, clientsCreated.get());
    }

    @Test
    public void test_buildExtractedContent_includesAllowedMetadataAndExcludesTechnicalMetadata() {
        final ExtractData extractData = new ExtractData("body-text");

        extractData.putValue("dc:creator", "Test Author");
        extractData.putValue("custom-keywords", "alpha beta gamma");
        extractData.putValue("X-TIKA:Parsed-By", "should-not-be-searchable");
        extractData.putValue("resourceName", "should-not-be-searchable.txt");

        final String content = dataStore.buildExtractedContent(extractData, "example.jpg");

        assertTrue(content.contains("body-text"));
        assertTrue(content.contains("Test Author"));
        assertTrue(content.contains("alpha beta gamma"));
        assertFalse(content.contains("should-not-be-searchable"));
    }

    // ===== sensitivity labels =====

    private static final String LABEL_ID_CONFIDENTIAL = "0e7d7d2b-1c3e-4f5a-9b8c-1234567890ab";
    private static final String LABEL_ID_SECRET = "a1b2c3d4-e5f6-4a5b-8c9d-0123456789ef";
    private static final String DOCX_MIMETYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

    /** The same stand-in for {@code PermissionHelper#encode} that {@link SensitivityLabelPolicyTest} uses. */
    private static SensitivityLabelPolicy labelPolicy(final String value) {
        return SensitivityLabelPolicy.parse(value, s -> s.replace("{group}", "2").replace("{user}", "1").replace("{role}", "R"));
    }

    private static SensitivityLabelPolicy.Label confidentialLabel() {
        return new SensitivityLabelPolicy.Label(LABEL_ID_CONFIDENTIAL, List.of("Confidential", "conf"), Boolean.FALSE, true);
    }

    /**
     * A {@link OneDriveDataStore} whose Graph-facing members are stubbed and which records how
     * often each was reached. {@code convertValue} resolves {@code file.<key>} templates with a
     * direct map lookup, for the reason given in
     * {@link #test_processDriveItem_assemblesRolesFromAllFourSources()}.
     */
    private static class LabelAwareDataStore extends OneDriveDataStore {
        /** The labels to report, or {@code null} to run the real lookup against the client. */
        private final List<SensitivityLabelPolicy.Label> labels;
        private final java.util.concurrent.atomic.AtomicInteger labelLookups = new java.util.concurrent.atomic.AtomicInteger();
        private final java.util.concurrent.atomic.AtomicInteger contentFetches = new java.util.concurrent.atomic.AtomicInteger();
        /** What the {@code broader.roles} template evaluates to. */
        private Object broaderRoles;
        private Map<?, ?> lastFilesMap;

        LabelAwareDataStore(final List<SensitivityLabelPolicy.Label> labels) {
            this.labels = labels;
        }

        @Override
        protected List<SensitivityLabelPolicy.Label> getDriveItemSensitivityLabels(final Microsoft365Client client, final String driveId,
                final DriveItem item, final SensitivityLabelPolicy policy, final DataStoreParams paramMap) {
            labelLookups.incrementAndGet();
            if (labels == null) {
                return super.getDriveItemSensitivityLabels(client, driveId, item, policy, paramMap);
            }
            return labels;
        }

        @Override
        protected List<String> getDriveItemPermissions(final Microsoft365Client client, final String driveId, final DriveItem item,
                final DataStoreParams paramMap) {
            return new ArrayList<>(List.of("1alice", "2sales", "2hr"));
        }

        @Override
        protected String getDriveItemContents(final Microsoft365Client client, final String driveId, final DriveItem item,
                final long maxContentLength, final boolean ignoreError) {
            contentFetches.incrementAndGet();
            return "content";
        }

        @Override
        protected Object convertValue(final String scriptType, final String template, final Map<String, Object> resultMap) {
            if (resultMap.get(FILE) instanceof final Map<?, ?> filesMap) {
                lastFilesMap = filesMap;
                if (template.startsWith("file.")) {
                    return filesMap.get(template.substring("file.".length()));
                }
            }
            if ("broader.roles".equals(template)) {
                return broaderRoles;
            }
            return super.convertValue(scriptType, template, resultMap);
        }
    }

    private static DriveItem labeledItem(final String name) {
        final DriveItem item = new DriveItem();
        item.setId("item-1");
        item.setName(name);
        item.setWebUrl("https://example.com/" + name);
        item.setSize(100L);
        final com.microsoft.graph.models.File file = new com.microsoft.graph.models.File();
        file.setMimeType(DOCX_MIMETYPE);
        item.setFile(file);
        return item;
    }

    private Map<String, Object> labelConfigMap(final SensitivityLabelPolicy policy, final long maxContentLength) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.IGNORE_FOLDER, Boolean.FALSE);
        configMap.put(OneDriveDataStore.IGNORE_ERROR, Boolean.FALSE);
        configMap.put(OneDriveDataStore.SUPPORTED_MIMETYPES, new String[] { ".*" });
        configMap.put(OneDriveDataStore.MAX_CONTENT_LENGTH, Long.valueOf(maxContentLength));
        configMap.put(OneDriveDataStore.SENSITIVITY_LABEL_POLICY, policy);
        configMap.put(OneDriveDataStore.SENSITIVITY_LABEL_EXTENSIONS, dataStore.getSensitivityLabelExtensions(new DataStoreParams()));
        return configMap;
    }

    private static void registerLabelProcessingComponents() {
        registerDriveItemProcessingComponents();
        final TestablePermissionHelper permissionHelper = new TestablePermissionHelper();
        permissionHelper.useSystemHelper(ComponentUtil.getSystemHelper());
        ComponentUtil.register(permissionHelper, "permissionHelper");
    }

    private static List<Map<String, Object>> processLabeled(final LabelAwareDataStore store, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Microsoft365Client client, final DriveItem item, final List<String> driveRoles) {
        final List<Map<String, Object>> captured = new ArrayList<>();
        final TestCallback callback = new TestCallback() {
            @Override
            void test(final DataStoreParams params, final Map<String, Object> dataMap) {
                captured.add(dataMap);
            }
        };
        store.processDriveItem(new DataConfig(), callback, configMap, paramMap, scriptMap, defaultDataMap, client, drive("drive-1"), item,
                driveRoles);
        return captured;
    }

    private static Map<String, String> roleScriptMap() {
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put(ComponentUtil.getFessConfig().getIndexFieldRole(), "file.roles");
        scriptMap.put("content", "file.contents");
        return scriptMap;
    }

    @Test
    public void test_processDriveItem_sensitivityLabelSkipRuleStoresNothing() {
        registerLabelProcessingComponents();
        final CapturingFailureUrlService failures = CapturingFailureUrlService.empty();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel()));

        final List<Map<String, Object>> captured = processLabeled(store, labelConfigMap(labelPolicy("Confidential=skip"), 1000000L),
                new DataStoreParams(), roleScriptMap(), new HashMap<>(), null, labeledItem("report.docx"), List.of());

        assertEquals("a skipped file must not be stored", 0, captured.size());
        assertEquals("a policy skip is not a failure", List.of(), failures.getStoredFailures());
        assertEquals("the labels must have been read once", 1, store.labelLookups.get());
        assertEquals("a skipped file's content must not be downloaded", 0, store.contentFetches.get());
    }

    @Test
    public void test_processDriveItem_sensitivityLabelNoContentRuleStoresWithoutContent() {
        registerLabelProcessingComponents();
        final CapturingFailureUrlService failures = CapturingFailureUrlService.empty();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel()));
        final DriveItem item = labeledItem("report.docx");
        // larger than max_content_length: without content there is nothing to exceed it
        item.setSize(10_000_000L);

        final List<Map<String, Object>> captured = processLabeled(store, labelConfigMap(labelPolicy("Confidential=no_content"), 1000L),
                new DataStoreParams(), roleScriptMap(), new HashMap<>(), null, item, List.of());

        assertEquals("the file must still be indexed", 1, captured.size());
        assertEquals("", captured.get(0).get("content"));
        assertEquals("the content must not be downloaded", 0, store.contentFetches.get());
        assertEquals(List.of(), failures.getStoredFailures());
    }

    @Test
    public void test_processDriveItem_oversizedUnlabeledFileStillFailsLengthCheck() {
        // control for the test above: the size check is lifted only for no_content files
        registerLabelProcessingComponents();
        final CapturingFailureUrlService failures = CapturingFailureUrlService.empty();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of());
        final DriveItem item = labeledItem("report.docx");
        item.setSize(10_000_000L);

        final List<Map<String, Object>> captured = processLabeled(store, labelConfigMap(labelPolicy("Confidential=no_content"), 1000L),
                new DataStoreParams(), roleScriptMap(), new HashMap<>(), null, item, List.of());

        assertEquals(0, captured.size());
        assertEquals(1, failures.getStoredFailures().size());
        assertEquals(0, store.contentFetches.get());
    }

    @Test
    public void test_processDriveItem_sensitivityLabelRestrictNarrowsRolesAndExposesLabelFields() {
        registerLabelProcessingComponents();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel()));
        final String roleField = ComponentUtil.getFessConfig().getIndexFieldRole();
        final Map<String, String> scriptMap = roleScriptMap();
        scriptMap.put("label", "file.sensitivity_label_names");
        scriptMap.put("label_id", "file.sensitivity_label_ids");
        scriptMap.put("label_protected", "file.sensitivity_label_protected");

        final List<Map<String, Object>> captured =
                processLabeled(store, labelConfigMap(labelPolicy("Confidential=restrict:{group}legal,{group}sales"), 1000000L),
                        new DataStoreParams(), scriptMap, new HashMap<>(), null, labeledItem("report.docx"), List.of("2legal"));

        assertEquals(1, captured.size());
        final Map<String, Object> dataMap = captured.get(0);
        assertEquals("only the allowed roles, in the ACL's own order", List.of("2sales", "2legal"), dataMap.get(roleField));
        assertEquals(List.of("2sales", "2legal"), store.lastFilesMap.get(OneDriveDataStore.FILE_ROLES));
        assertEquals(List.of("Confidential"), dataMap.get("label"));
        assertEquals(List.of(LABEL_ID_CONFIDENTIAL), dataMap.get("label_id"));
        assertEquals(Boolean.FALSE, dataMap.get("label_protected"));
        assertEquals("content", dataMap.get("content"));
    }

    @Test
    public void test_processDriveItem_sensitivityLabelProtectedFieldIsTrueForEncryptedLabel() {
        registerLabelProcessingComponents();
        final SensitivityLabelPolicy.Label encrypted =
                new SensitivityLabelPolicy.Label(LABEL_ID_SECRET, List.of("Secret"), Boolean.TRUE, true);
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel(), encrypted));
        final Map<String, String> scriptMap = roleScriptMap();
        scriptMap.put("label", "file.sensitivity_label_names");
        scriptMap.put("label_protected", "file.sensitivity_label_protected");

        final List<Map<String, Object>> captured = processLabeled(store, labelConfigMap(labelPolicy(""), 1000000L), new DataStoreParams(),
                scriptMap, new HashMap<>(), null, labeledItem("report.docx"), List.of());

        assertEquals(1, captured.size());
        assertEquals(List.of("Confidential", "Secret"), captured.get(0).get("label"));
        assertEquals(Boolean.TRUE, captured.get(0).get("label_protected"));
        assertEquals("an encrypted label is indexed without content by default", "", captured.get(0).get("content"));
        assertEquals(0, store.contentFetches.get());
    }

    @Test
    public void test_processDriveItem_sensitivityLabelRestrictWithNoRoleLeftIsDiscarded() {
        registerLabelProcessingComponents();
        final CapturingFailureUrlService failures = CapturingFailureUrlService.empty();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel()));

        final List<Map<String, Object>> captured =
                processLabeled(store, labelConfigMap(labelPolicy("Confidential=restrict:{group}nobody"), 1000000L), new DataStoreParams(),
                        roleScriptMap(), new HashMap<>(), null, labeledItem("report.docx"), List.of("2legal"));

        assertEquals("a file no role may see must not be indexed", 0, captured.size());
        assertEquals("a policy discard is not a failure", List.of(), failures.getStoredFailures());
    }

    @Test
    public void test_processDriveItem_sensitivityLabelRestrictNarrowsBroaderScriptRoleField() {
        registerLabelProcessingComponents();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel()));
        store.broaderRoles = List.of("1alice", "2sales", "2hr", "Rguest");
        final String roleField = ComponentUtil.getFessConfig().getIndexFieldRole();
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put(roleField, "broader.roles");

        final List<Map<String, Object>> captured =
                processLabeled(store, labelConfigMap(labelPolicy("Confidential=restrict:{group}sales,{role}guest"), 1000000L),
                        new DataStoreParams(), scriptMap, new HashMap<>(), null, labeledItem("report.docx"), List.of());

        assertEquals(1, captured.size());
        assertEquals("the script must not widen a restricted file's ACL", List.of("2sales", "Rguest"), captured.get(0).get(roleField));
    }

    @Test
    public void test_processDriveItem_sensitivityLabelRestrictNarrowsScalarScriptRoleField() {
        registerLabelProcessingComponents();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel()));
        final String roleField = ComponentUtil.getFessConfig().getIndexFieldRole();
        final Map<String, String> scriptMap = new HashMap<>();
        scriptMap.put(roleField, "broader.roles");

        store.broaderRoles = "Rguest";
        final List<Map<String, Object>> allowed =
                processLabeled(store, labelConfigMap(labelPolicy("Confidential=restrict:{role}guest"), 1000000L), new DataStoreParams(),
                        scriptMap, new HashMap<>(), null, labeledItem("report.docx"), List.of());
        assertEquals(1, allowed.size());
        assertEquals(List.of("Rguest"), allowed.get(0).get(roleField));

        store.broaderRoles = new String[] { "2hr", null, "Rguest" };
        final List<Map<String, Object>> fromArray =
                processLabeled(store, labelConfigMap(labelPolicy("Confidential=restrict:{role}guest"), 1000000L), new DataStoreParams(),
                        scriptMap, new HashMap<>(), null, labeledItem("report.docx"), List.of());
        assertEquals(1, fromArray.size());
        assertEquals(List.of("Rguest"), fromArray.get(0).get(roleField));

        store.broaderRoles = "Reveryone";
        final List<Map<String, Object>> denied =
                processLabeled(store, labelConfigMap(labelPolicy("Confidential=restrict:{role}guest"), 1000000L), new DataStoreParams(),
                        scriptMap, new HashMap<>(), null, labeledItem("report.docx"), List.of());
        assertEquals(0, denied.size());
    }

    @Test
    public void test_processDriveItem_sensitivityLabelRestrictNarrowsDataConfigRolesWithoutScript() {
        registerLabelProcessingComponents();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel()));
        final String roleField = ComponentUtil.getFessConfig().getIndexFieldRole();
        final Map<String, Object> defaultDataMap = new HashMap<>();
        defaultDataMap.put(roleField, List.of("2sales", "2hr"));

        final List<Map<String, Object>> captured =
                processLabeled(store, labelConfigMap(labelPolicy("Confidential=restrict:{group}sales"), 1000000L), new DataStoreParams(),
                        new HashMap<>(), defaultDataMap, null, labeledItem("report.docx"), List.of());

        assertEquals(1, captured.size());
        assertEquals(List.of("2sales"), captured.get(0).get(roleField));
    }

    @Test
    public void test_processDriveItem_sensitivityLabelsDisabledAddsNothing() {
        registerLabelProcessingComponents();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel()));
        final String roleField = ComponentUtil.getFessConfig().getIndexFieldRole();

        final List<Map<String, Object>> captured = processLabeled(store, labelConfigMap(null, 1000000L), new DataStoreParams(),
                roleScriptMap(), new HashMap<>(), null, labeledItem("report.docx"), List.of("2legal"));

        assertEquals(1, captured.size());
        assertEquals("the labels must not be read when the feature is disabled", 0, store.labelLookups.get());
        assertEquals(1, store.contentFetches.get());
        assertEquals(List.of("1alice", "2sales", "2hr", "2legal"), captured.get(0).get(roleField));
        assertFalse(store.lastFilesMap.containsKey(OneDriveDataStore.FILE_SENSITIVITY_LABEL_IDS));
        assertFalse(store.lastFilesMap.containsKey(OneDriveDataStore.FILE_SENSITIVITY_LABEL_NAMES));
        assertFalse(store.lastFilesMap.containsKey(OneDriveDataStore.FILE_SENSITIVITY_LABEL_PROTECTED));
    }

    @Test
    public void test_processDriveItem_sensitivityLabelNonTargetExtensionIsNotLookedUp() {
        registerLabelProcessingComponents();
        final LabelAwareDataStore store = new LabelAwareDataStore(List.of(confidentialLabel()));

        final List<Map<String, Object>> captured = processLabeled(store, labelConfigMap(labelPolicy("Confidential=skip"), 1000000L),
                new DataStoreParams(), roleScriptMap(), new HashMap<>(), null, labeledItem("notes.txt"), List.of());

        assertEquals(1, captured.size());
        assertEquals(0, store.labelLookups.get());
        assertEquals(1, store.contentFetches.get());
        assertEquals(List.of(), store.lastFilesMap.get(OneDriveDataStore.FILE_SENSITIVITY_LABEL_IDS));
        assertEquals(List.of(), store.lastFilesMap.get(OneDriveDataStore.FILE_SENSITIVITY_LABEL_NAMES));
        assertEquals(Boolean.FALSE, store.lastFilesMap.get(OneDriveDataStore.FILE_SENSITIVITY_LABEL_PROTECTED));
    }

    @Test
    public void test_processDriveItem_unreadableSensitivityLabelsRecordFailure() {
        registerLabelProcessingComponents();
        final CapturingFailureUrlService failures = CapturingFailureUrlService.empty();
        final LabelAwareDataStore store = new LabelAwareDataStore(null);
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenThrow(new IllegalStateException("graph down"));
        final DriveItem item = labeledItem("report.docx");

        final List<Map<String, Object>> captured = processLabeled(store, labelConfigMap(labelPolicy(""), 1000000L), new DataStoreParams(),
                roleScriptMap(), new HashMap<>(), client, item, List.of());

        assertEquals("a file whose labels cannot be read must not be indexed by default", 0, captured.size());
        assertEquals(0, store.contentFetches.get());
        final List<CapturingFailureUrlService.StoredFailure> stored = failures.getStoredFailures();
        assertEquals(1, stored.size());
        assertEquals(item.getWebUrl(), stored.get(0).url());
        assertTrue(String.valueOf(stored.get(0).throwable()), stored.get(0).throwable() instanceof SensitivityLabelUnavailableException);
    }

    @Test
    public void test_processDriveItem_unreadableSensitivityLabelsIndexedUnderIndexWithoutLabel() {
        registerLabelProcessingComponents();
        final CapturingFailureUrlService failures = CapturingFailureUrlService.empty();
        final LabelAwareDataStore store = new LabelAwareDataStore(null);
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenThrow(new IllegalStateException("graph down"));
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_FAILURE_POLICY, "index_without_label");

        final List<Map<String, Object>> captured = processLabeled(store, labelConfigMap(labelPolicy("*=skip"), 1000000L), paramMap,
                roleScriptMap(), new HashMap<>(), client, labeledItem("report.docx"), List.of());

        assertEquals(1, captured.size());
        assertEquals(List.of(), failures.getStoredFailures());
        assertEquals(List.of(), store.lastFilesMap.get(OneDriveDataStore.FILE_SENSITIVITY_LABEL_IDS));
    }

    // ----- getDriveItemSensitivityLabels -----

    private static DriveItem labelLookupItem() {
        final DriveItem item = labeledItem("report.docx");
        item.setId("item-1");
        return item;
    }

    private static SensitivityLabelAssignment assignment(final String labelId) {
        final SensitivityLabelAssignment assignment = new SensitivityLabelAssignment();
        assignment.setSensitivityLabelId(labelId);
        return assignment;
    }

    private static SensitivityLabel definition(final String id, final String displayName, final String name, final Boolean hasProtection) {
        final SensitivityLabel label = new SensitivityLabel();
        label.setId(id);
        label.setDisplayName(displayName);
        label.setName(name);
        label.setHasProtection(hasProtection);
        return label;
    }

    private static Microsoft365Client.SensitivityLabelEntry entry(final SensitivityLabel label) {
        return new Microsoft365Client.SensitivityLabelEntry(label, null);
    }

    @Test
    public void test_getDriveItemSensitivityLabels_extractFailureThrowsByDefault() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        final IllegalStateException cause = new IllegalStateException("graph down");
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenThrow(cause);

        final SensitivityLabelUnavailableException e = assertThrows(SensitivityLabelUnavailableException.class, () -> dataStore
                .getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(), labelPolicy(""), new DataStoreParams()));
        assertSame(cause, e.getCause());
        assertTrue(e.getMessage(), e.getMessage().contains("https://example.com/report.docx"));
    }

    @Test
    public void test_getDriveItemSensitivityLabels_extractFailureUnknownPolicyTreatedAsSkip() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenThrow(new IllegalStateException("graph down"));
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_FAILURE_POLICY, "index_anyway");

        assertThrows(SensitivityLabelUnavailableException.class,
                () -> dataStore.getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(), labelPolicy(""), paramMap));
    }

    @Test
    public void test_getDriveItemSensitivityLabels_extractFailureUnderIndexWithoutLabelIsEmpty() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenThrow(new IllegalStateException("graph down"));
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_FAILURE_POLICY, " Index_Without_Label ");

        assertEquals(List.of(), dataStore.getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(), labelPolicy(""), paramMap));
    }

    @Test
    public void test_getDriveItemSensitivityLabels_unreadableDefinitionWithNameRuleFails() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenReturn(List.of(assignment(LABEL_ID_CONFIDENTIAL)));
        when(client.getSensitivityLabel(LABEL_ID_CONFIDENTIAL)).thenReturn(null);

        final SensitivityLabelUnavailableException e =
                assertThrows(SensitivityLabelUnavailableException.class, () -> dataStore.getDriveItemSensitivityLabels(client, "drive-1",
                        labelLookupItem(), labelPolicy("Confidential=skip"), new DataStoreParams()));
        assertTrue(e.getMessage(), e.getMessage().contains(LABEL_ID_CONFIDENTIAL));

        final DataStoreParams lenient = new DataStoreParams();
        lenient.put(OneDriveDataStore.SENSITIVITY_LABEL_FAILURE_POLICY, "index_without_label");
        assertEquals("under index_without_label the label is kept, unresolved",
                List.of(SensitivityLabelPolicy.Label.unresolved(LABEL_ID_CONFIDENTIAL)),
                dataStore.getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(), labelPolicy("Confidential=skip"), lenient));
    }

    @Test
    public void test_getDriveItemSensitivityLabels_entryWithoutLabelIsUnresolved() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenReturn(List.of(assignment(LABEL_ID_CONFIDENTIAL)));
        when(client.getSensitivityLabel(LABEL_ID_CONFIDENTIAL)).thenReturn(new Microsoft365Client.SensitivityLabelEntry(null, null));

        assertEquals(List.of(SensitivityLabelPolicy.Label.unresolved(LABEL_ID_CONFIDENTIAL)), dataStore
                .getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(), labelPolicy("*=skip"), new DataStoreParams()));
    }

    @Test
    public void test_getDriveItemSensitivityLabels_indexWithoutLabelKeepsEvaluableLabels() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1"))
                .thenReturn(List.of(assignment(LABEL_ID_CONFIDENTIAL), assignment(LABEL_ID_SECRET)));
        when(client.getSensitivityLabel(LABEL_ID_CONFIDENTIAL)).thenReturn(null);
        when(client.getSensitivityLabel(LABEL_ID_SECRET))
                .thenReturn(entry(definition(LABEL_ID_SECRET, "Highly Confidential", "secret", true)));

        final DataStoreParams lenient = new DataStoreParams();
        lenient.put(OneDriveDataStore.SENSITIVITY_LABEL_FAILURE_POLICY, "index_without_label");
        final SensitivityLabelPolicy policy = labelPolicy("Confidential=index\n" + LABEL_ID_SECRET + "=skip");
        final List<SensitivityLabelPolicy.Label> labels =
                dataStore.getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(), policy, lenient);

        // The unreadable label is kept unresolved; the GUID skip rule of the other label still applies.
        assertEquals(
                List.of(SensitivityLabelPolicy.Label.unresolved(LABEL_ID_CONFIDENTIAL),
                        new SensitivityLabelPolicy.Label(LABEL_ID_SECRET, List.of("Highly Confidential", "secret"), Boolean.TRUE, true)),
                labels);
        assertTrue(policy.decide(labels).skip());
    }

    @Test
    public void test_getDriveItemSensitivityLabels_indexWithoutLabelAppliesAnyRuleToUnresolvedLabel() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenReturn(List.of(assignment(LABEL_ID_CONFIDENTIAL)));
        when(client.getSensitivityLabel(LABEL_ID_CONFIDENTIAL)).thenReturn(null);

        final DataStoreParams lenient = new DataStoreParams();
        lenient.put(OneDriveDataStore.SENSITIVITY_LABEL_FAILURE_POLICY, "index_without_label");
        final SensitivityLabelPolicy policy = labelPolicy("Confidential=skip\n*=restrict:{group}a");
        final List<SensitivityLabelPolicy.Label> labels =
                dataStore.getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(), policy, lenient);

        assertEquals(List.of(SensitivityLabelPolicy.Label.unresolved(LABEL_ID_CONFIDENTIAL)), labels);
        final SensitivityLabelPolicy.Decision decision = policy.decide(labels);
        assertFalse("the name rule cannot match an unresolved label", decision.skip());
        assertEquals(Set.of("2a"), decision.allowedRoles());
    }

    @Test
    public void test_getDriveItemSensitivityLabels_unreadableDefinitionWithIdRuleForOtherLabelFails() {
        // An ID rule can match a sublabel through its parent, so it needs the definition too.
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenReturn(List.of(assignment(LABEL_ID_CONFIDENTIAL)));
        when(client.getSensitivityLabel(LABEL_ID_CONFIDENTIAL)).thenReturn(null);

        assertThrows(SensitivityLabelUnavailableException.class, () -> dataStore.getDriveItemSensitivityLabels(client, "drive-1",
                labelLookupItem(), labelPolicy(LABEL_ID_SECRET + "=skip"), new DataStoreParams()));
    }

    @Test
    public void test_getDriveItemSensitivityLabels_unreadableDefinitionWithOwnIdRuleIsUnresolved() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenReturn(List.of(assignment(LABEL_ID_CONFIDENTIAL)));
        when(client.getSensitivityLabel(LABEL_ID_CONFIDENTIAL)).thenReturn(null);

        assertEquals(List.of(SensitivityLabelPolicy.Label.unresolved(LABEL_ID_CONFIDENTIAL)), dataStore.getDriveItemSensitivityLabels(
                client, "drive-1", labelLookupItem(), labelPolicy(LABEL_ID_CONFIDENTIAL + "=skip\nSecret=index"), new DataStoreParams()));
        assertEquals("a policy with only * needs no definition", List.of(SensitivityLabelPolicy.Label.unresolved(LABEL_ID_CONFIDENTIAL)),
                dataStore.getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(), labelPolicy("*=index"),
                        new DataStoreParams()));
    }

    @Test
    public void test_getDriveItemSensitivityLabels_sublabelCarriesParent() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenReturn(List.of(assignment(LABEL_ID_SECRET)));
        when(client.getSensitivityLabel(LABEL_ID_SECRET))
                .thenReturn(new Microsoft365Client.SensitivityLabelEntry(definition(LABEL_ID_SECRET, "All Employees", "conf-all", true),
                        definition(LABEL_ID_CONFIDENTIAL, "Confidential", "conf", false)));

        final SensitivityLabelPolicy policy = labelPolicy(LABEL_ID_CONFIDENTIAL + "=skip");
        final List<SensitivityLabelPolicy.Label> labels =
                dataStore.getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(), policy, new DataStoreParams());

        assertEquals(List.of(new SensitivityLabelPolicy.Label(LABEL_ID_SECRET, List.of("All Employees", "conf-all"), Boolean.TRUE, true,
                LABEL_ID_CONFIDENTIAL, List.of("Confidential", "conf"))), labels);
        assertEquals("Confidential\\All Employees", labels.get(0).displayName());
        assertTrue("the parent's ID rule applies to the sublabel", policy.decide(labels).skip());
    }

    @Test
    public void test_processDriveItem_unreadableLabelDefinitionUnderIndexWithoutLabelKeepsLabelId() {
        registerLabelProcessingComponents();
        final CapturingFailureUrlService failures = CapturingFailureUrlService.empty();
        final LabelAwareDataStore store = new LabelAwareDataStore(null);
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenReturn(List.of(assignment(LABEL_ID_CONFIDENTIAL)));
        when(client.getSensitivityLabel(LABEL_ID_CONFIDENTIAL)).thenReturn(null);
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_FAILURE_POLICY, "index_without_label");
        final String roleField = ComponentUtil.getFessConfig().getIndexFieldRole();

        final List<Map<String, Object>> captured =
                processLabeled(store, labelConfigMap(labelPolicy("Confidential=skip\n*=restrict:{group}sales"), 1000000L), paramMap,
                        roleScriptMap(), new HashMap<>(), client, labeledItem("report.docx"), List.of());

        assertEquals(1, captured.size());
        assertEquals(List.of(), failures.getStoredFailures());
        assertEquals(List.of("2sales"), captured.get(0).get(roleField));
        assertEquals(List.of(LABEL_ID_CONFIDENTIAL), store.lastFilesMap.get(OneDriveDataStore.FILE_SENSITIVITY_LABEL_IDS));
        assertEquals(List.of(LABEL_ID_CONFIDENTIAL), store.lastFilesMap.get(OneDriveDataStore.FILE_SENSITIVITY_LABEL_NAMES));
    }

    @Test
    public void test_getDriveItemSensitivityLabels_resolvesDefinition() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1"))
                .thenReturn(java.util.Arrays.asList(assignment(LABEL_ID_CONFIDENTIAL), null, assignment(" "), assignment(LABEL_ID_SECRET)));
        when(client.getSensitivityLabel(LABEL_ID_CONFIDENTIAL))
                .thenReturn(entry(definition(LABEL_ID_CONFIDENTIAL, "Confidential", "conf", Boolean.FALSE)));
        when(client.getSensitivityLabel(LABEL_ID_SECRET))
                .thenReturn(entry(definition(LABEL_ID_SECRET, "Highly Confidential", "secret", true)));

        final List<SensitivityLabelPolicy.Label> labels = dataStore.getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(),
                labelPolicy("Confidential=skip\n@protected=index"), new DataStoreParams());

        assertEquals(
                List.of(new SensitivityLabelPolicy.Label(LABEL_ID_CONFIDENTIAL, List.of("Confidential", "conf"), Boolean.FALSE, true),
                        new SensitivityLabelPolicy.Label(LABEL_ID_SECRET, List.of("Highly Confidential", "secret"), Boolean.TRUE, true)),
                labels);
        verify(client, never()).getSensitivityLabel(" ");
    }

    @Test
    public void test_getDriveItemSensitivityLabels_unlabeledFileIsEmpty() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.extractSensitivityLabels("drive-1", "item-1")).thenReturn(List.of());

        assertEquals(List.of(), dataStore.getDriveItemSensitivityLabels(client, "drive-1", labelLookupItem(),
                labelPolicy("Confidential=skip"), new DataStoreParams()));
        verify(client, never()).getSensitivityLabel(org.mockito.ArgumentMatchers.anyString());
    }

    // ----- getSensitivityLabelPolicy / extensions -----

    @Test
    public void test_getSensitivityLabelPolicy_disabledIsNull() {
        final DataStoreParams paramMap = new DataStoreParams();
        assertNull("disabled by default", dataStore.getSensitivityLabelPolicy(paramMap));

        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_POLICY, "Confidential=skip");
        assertNull("a policy without sensitivity_label_enabled=true is ignored", dataStore.getSensitivityLabelPolicy(paramMap));

        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_ENABLED, "false");
        assertNull(dataStore.getSensitivityLabelPolicy(paramMap));

        // a malformed policy does not matter while the feature is off
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_POLICY, "Confidential");
        assertNull(dataStore.getSensitivityLabelPolicy(paramMap));
    }

    @Test
    public void test_getSensitivityLabelPolicy_enabledEncodesPermissions() {
        registerLabelProcessingComponents();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_ENABLED, " TRUE ");
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_POLICY, "Confidential=restrict:{group}sales");

        final SensitivityLabelPolicy policy = dataStore.getSensitivityLabelPolicy(paramMap);

        assertNotNull(policy);
        final String expectedRole = ComponentUtil.getPermissionHelper().encode("{group}sales");
        assertEquals(Set.of(expectedRole), policy.findRule(confidentialLabel()).restrictRoles());
    }

    @Test
    public void test_getSensitivityLabelPolicy_enabledWithoutRulesIsEmpty() {
        registerLabelProcessingComponents();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_ENABLED, "true");

        final SensitivityLabelPolicy policy = dataStore.getSensitivityLabelPolicy(paramMap);
        assertNotNull("enabled with no rules still applies the default rule for encrypted labels", policy);
        assertTrue(policy.isEmpty());
    }

    @Test
    public void test_getSensitivityLabelPolicy_malformedPolicyFailsTheCrawl() {
        registerLabelProcessingComponents();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_ENABLED, "true");
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_POLICY, "Confidential=delete");

        final DataStoreException e = assertThrows(DataStoreException.class, () -> dataStore.getSensitivityLabelPolicy(paramMap));
        assertTrue(e.getMessage(), e.getMessage().contains(OneDriveDataStore.SENSITIVITY_LABEL_POLICY));
        assertTrue(String.valueOf(e.getCause()), e.getCause() instanceof IllegalArgumentException);
    }

    @Test
    public void test_getSensitivityLabelExtensions_defaults() {
        final Set<String> extensions = dataStore.getSensitivityLabelExtensions(new DataStoreParams());
        for (final String ext : new String[] { "docx", "xlsx", "pptx", "pdf", "doc", "xlsb", "potm" }) {
            assertTrue(ext, extensions.contains(ext));
        }
        assertFalse(extensions.contains("txt"));
        assertFalse(extensions.contains("csv"));
    }

    @Test
    public void test_getSensitivityLabelExtensions_customList() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_EXTENSIONS, " .DOCX, pdf ,, .Xlsx , ");
        assertEquals(Set.of("docx", "pdf", "xlsx"), dataStore.getSensitivityLabelExtensions(paramMap));

        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_EXTENSIONS, "  ");
        assertTrue("blank falls back to the defaults", dataStore.getSensitivityLabelExtensions(paramMap).contains("docx"));
    }

    @Test
    public void test_isSensitivityLabelTarget() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put(OneDriveDataStore.SENSITIVITY_LABEL_EXTENSIONS, ".DOCX,pdf");
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(OneDriveDataStore.SENSITIVITY_LABEL_EXTENSIONS, dataStore.getSensitivityLabelExtensions(paramMap));

        assertTrue(dataStore.isSensitivityLabelTarget(configMap, labeledItem("Report.DOCX")));
        assertTrue(dataStore.isSensitivityLabelTarget(configMap, labeledItem("scan.final.pdf")));
        assertFalse(dataStore.isSensitivityLabelTarget(configMap, labeledItem("notes.txt")));
        assertFalse(dataStore.isSensitivityLabelTarget(configMap, labeledItem("report.docx.txt")));
        assertFalse(dataStore.isSensitivityLabelTarget(configMap, labeledItem("README")));
        assertFalse(dataStore.isSensitivityLabelTarget(configMap, labeledItem("report.")));
        assertFalse(dataStore.isSensitivityLabelTarget(configMap, labeledItem(null)));

        final DriveItem folder = new DriveItem();
        folder.setName("archive.docx");
        folder.setFolder(new com.microsoft.graph.models.Folder());
        assertFalse("a folder is never a target", dataStore.isSensitivityLabelTarget(configMap, folder));

        assertFalse("no extension set configured", dataStore.isSensitivityLabelTarget(new HashMap<>(), labeledItem("Report.docx")));
    }

    static abstract class TestCallback implements IndexUpdateCallback {
        private long documentSize = 0;
        private long executeTime = 0;

        abstract void test(DataStoreParams paramMap, Map<String, Object> dataMap);

        @Override
        public void store(DataStoreParams paramMap, Map<String, Object> dataMap) {
            final long startTime = System.currentTimeMillis();
            test(paramMap, dataMap);
            executeTime += System.currentTimeMillis() - startTime;
            documentSize++;
        }

        @Override
        public long getDocumentSize() {
            return documentSize;
        }

        @Override
        public long getExecuteTime() {
            return executeTime;
        }

        @Override
        public void commit() {
        }
    }
}

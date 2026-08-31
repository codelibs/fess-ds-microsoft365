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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.Identity;
import com.microsoft.graph.models.ItemReference;
import com.microsoft.graph.models.Permission;
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

        assertNull(dataStore.getUrl(configMap, paramMap, item));

        configMap.put(OneDriveDataStore.CURRENT_CRAWLER, OneDriveDataStore.CRAWLER_TYPE_SHARED);
        item.setWebUrl(
                "https://n2sm.sharepoint.com/sites/test-site/_layouts/15/Doc.aspx?sourcedoc=%X-X-X-X-X%7D&file=test.doc&action=default&mobileredirect=true");
        ItemReference parentRef = new ItemReference();
        parentRef.setPath("/drive/root:/fess-testdata-master/msoffice");
        item.setParentReference(parentRef);
        item.setName("test.doc");
        assertEquals("https://n2sm.sharepoint.com/sites/test-site/Shared%20Documents/fess-testdata-master/msoffice/test.doc",
                dataStore.getUrl(configMap, paramMap, item));

        item.setWebUrl("https://n2sm.sharepoint.com/sites/test-site/Shared%20Documents/fess-testdata-master/msoffice/test.doc");
        assertEquals("https://n2sm.sharepoint.com/sites/test-site/Shared%20Documents/fess-testdata-master/msoffice/test.doc",
                dataStore.getUrl(configMap, paramMap, item));
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
                dataStore.isTargetDrive(paramMap, driveWithUrl("https://contoso.sharepoint.com/sites/test/Style%20Library/")));
        assertTrue("an ordinary document library must be crawled",
                dataStore.isTargetDrive(paramMap, driveWithUrl("https://contoso.sharepoint.com/sites/test/Shared%20Documents")));
    }

    @Test
    public void test_isTargetDrive_ignoreSystemLibrariesFalseKeepsThem() {
        final OneDriveDataStore dataStore = new OneDriveDataStore();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_system_libraries", "false");

        assertTrue(dataStore.isTargetDrive(paramMap, driveWithUrl("https://contoso.sharepoint.com/sites/test/Style%20Library/")));
    }

    private static Drive driveWithUrl(final String webUrl) {
        final Drive drive = new Drive();
        drive.setWebUrl(webUrl);
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

        roleAwareDataStore.processDriveItem(new DataConfig(), callback, configMap, paramMap, scriptMap, defaultDataMap, null, "drive-1",
                item, List.of("1drive-role"));

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
            protected String getUrl(final Map<String, Object> configMap, final DataStoreParams paramMap, final DriveItem item) {
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
                null, "drive-1", item, Collections.emptyList());
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
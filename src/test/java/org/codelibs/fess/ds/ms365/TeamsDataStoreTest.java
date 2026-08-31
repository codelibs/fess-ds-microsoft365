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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.ChatMessage;
import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.ConversationMember;
import com.microsoft.graph.models.Group;

public class TeamsDataStoreTest extends UnitDsTestCase {

    private static final Logger logger = LogManager.getLogger(TeamsDataStoreTest.class);

    private TeamsDataStore dataStore;

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
        dataStore = new TeamsDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        dataStore = null;
        super.tearDown(testInfo);
    }

    @Test
    public void test_getName() {
        assertEquals("TeamsDataStore", dataStore.getName());
    }

    // Test normalizeTextContent method
    @Test
    public void test_normalizeTextContent_nullInput() {
        assertEquals("", dataStore.normalizeTextContent(null));
    }

    @Test
    public void test_normalizeTextContent_emptyInput() {
        assertEquals("", dataStore.normalizeTextContent(""));
        assertEquals("", dataStore.normalizeTextContent(" "));
        assertEquals("", dataStore.normalizeTextContent("   "));
    }

    @Test
    public void test_normalizeTextContent_simpleText() {
        assertEquals("test", dataStore.normalizeTextContent(" test "));
        assertEquals("hello world", dataStore.normalizeTextContent("hello world"));
        assertEquals("test message", dataStore.normalizeTextContent("  test message  "));
    }

    @Test
    public void test_normalizeTextContent_withAttachmentTags() {
        assertEquals("test", dataStore.normalizeTextContent(" test <attachment></attachment>"));
        assertEquals("before  after", dataStore.normalizeTextContent("before <attachment></attachment> after"));
        assertEquals("text", dataStore.normalizeTextContent("<attachment></attachment>text<attachment></attachment>"));
    }

    @Test
    public void test_normalizeTextContent_withAttachmentAttributes() {
        assertEquals("test", dataStore.normalizeTextContent(" test <attachment id=\"123\"></attachment>"));
        assertEquals("message", dataStore.normalizeTextContent("<attachment name=\"file.pdf\"></attachment> message "));
        assertEquals("content", dataStore.normalizeTextContent("content<attachment id=\"abc\" name=\"doc.docx\"></attachment>"));
    }

    @Test
    public void test_normalizeTextContent_multipleAttachments() {
        assertEquals("text  between", dataStore
                .normalizeTextContent("<attachment></attachment> text <attachment></attachment> between <attachment></attachment>"));
        assertEquals("start  end", dataStore.normalizeTextContent("start <attachment></attachment><attachment></attachment> end"));
    }

    @Test
    public void test_normalizeTextContent_preserveOtherHtml() {
        // Other HTML tags should be preserved (only attachment tags are removed)
        assertEquals("<p>test</p>", dataStore.normalizeTextContent("<p>test</p>"));
        assertEquals("<div>content</div>", dataStore.normalizeTextContent("<div>content</div>"));
        assertEquals("<strong>bold</strong> text", dataStore.normalizeTextContent("<strong>bold</strong> text"));
    }

    // Test getTeamId method
    @Test
    public void test_getTeamId_withValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("team_id", "test-team-id");

        assertEquals("test-team-id", dataStore.getTeamId(paramMap));
    }

    @Test
    public void test_getTeamId_withoutValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertNull(dataStore.getTeamId(paramMap));
    }

    // Test getExcludeTeamIds method
    @Test
    public void test_getExcludeTeamIds_singleTeam() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_team_ids", "team-1");

        final String[] excludeIds = dataStore.getExcludeTeamIds(paramMap);
        assertNotNull(excludeIds);
        assertEquals(1, excludeIds.length);
        assertEquals("team-1", excludeIds[0]);
    }

    @Test
    public void test_getExcludeTeamIds_multipleTeams() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_team_ids", "team-1,team-2,team-3");

        final String[] excludeIds = dataStore.getExcludeTeamIds(paramMap);
        assertNotNull(excludeIds);
        assertEquals(3, excludeIds.length);
        assertEquals("team-1", excludeIds[0]);
        assertEquals("team-2", excludeIds[1]);
        assertEquals("team-3", excludeIds[2]);
    }

    @Test
    public void test_getExcludeTeamIds_withSpaces() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_team_ids", " team-1 , team-2 , team-3 ");

        final String[] excludeIds = dataStore.getExcludeTeamIds(paramMap);
        assertNotNull(excludeIds);
        assertEquals(3, excludeIds.length);
        assertEquals("team-1", excludeIds[0]);
        assertEquals("team-2", excludeIds[1]);
        assertEquals("team-3", excludeIds[2]);
    }

    @Test
    public void test_getExcludeTeamIds_emptyString() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_team_ids", "");

        final String[] excludeIds = dataStore.getExcludeTeamIds(paramMap);
        assertNotNull(excludeIds);
        assertEquals(0, excludeIds.length);
    }

    @Test
    public void test_getExcludeTeamIds_notSet() {
        final DataStoreParams paramMap = new DataStoreParams();

        final String[] excludeIds = dataStore.getExcludeTeamIds(paramMap);
        assertNotNull(excludeIds);
        assertEquals(0, excludeIds.length);
    }

    // Test getIncludeVisibilities method
    @Test
    public void test_getIncludeVisibilities_singleVisibility() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_visibility", "Public");

        final String[] visibilities = dataStore.getIncludeVisibilities(paramMap);
        assertNotNull(visibilities);
        assertEquals(1, visibilities.length);
        assertEquals("Public", visibilities[0]);
    }

    @Test
    public void test_getIncludeVisibilities_multipleVisibilities() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_visibility", "Public,Private");

        final String[] visibilities = dataStore.getIncludeVisibilities(paramMap);
        assertNotNull(visibilities);
        assertEquals(2, visibilities.length);
        assertEquals("Public", visibilities[0]);
        assertEquals("Private", visibilities[1]);
    }

    @Test
    public void test_getIncludeVisibilities_withSpaces() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_visibility", " Public , Private , HiddenMembership ");

        final String[] visibilities = dataStore.getIncludeVisibilities(paramMap);
        assertNotNull(visibilities);
        assertEquals(3, visibilities.length);
        assertEquals("Public", visibilities[0]);
        assertEquals("Private", visibilities[1]);
        assertEquals("HiddenMembership", visibilities[2]);
    }

    @Test
    public void test_getIncludeVisibilities_emptyString() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_visibility", "");

        final String[] visibilities = dataStore.getIncludeVisibilities(paramMap);
        assertNotNull(visibilities);
        assertEquals(0, visibilities.length);
    }

    @Test
    public void test_getIncludeVisibilities_notSet() {
        final DataStoreParams paramMap = new DataStoreParams();

        final String[] visibilities = dataStore.getIncludeVisibilities(paramMap);
        assertNotNull(visibilities);
        assertEquals(0, visibilities.length);
    }

    // Test getChannelId method
    @Test
    public void test_getChannelId_withValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("channel_id", "test-channel-id");

        assertEquals("test-channel-id", dataStore.getChannelId(paramMap));
    }

    @Test
    public void test_getChannelId_withoutValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertNull(dataStore.getChannelId(paramMap));
    }

    // Test getChatId method
    @Test
    public void test_getChatId_withValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("chat_id", "test-chat-id");

        assertEquals("test-chat-id", dataStore.getChatId(paramMap));
    }

    @Test
    public void test_getChatId_withoutValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertNull(dataStore.getChatId(paramMap));
    }

    // Test isIgnoreReplies method
    @Test
    public void test_isIgnoreReplies_true() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_replies", "true");

        assertTrue(dataStore.isIgnoreReplies(paramMap));
    }

    @Test
    public void test_isIgnoreReplies_false() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_replies", "false");

        assertFalse(dataStore.isIgnoreReplies(paramMap));
    }

    @Test
    public void test_isIgnoreReplies_defaultValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertFalse("Default should be false", dataStore.isIgnoreReplies(paramMap));
    }

    @Test
    public void test_isIgnoreReplies_caseInsensitive() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_replies", "TRUE");
        assertTrue(dataStore.isIgnoreReplies(paramMap1));

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_replies", "False");
        assertFalse(dataStore.isIgnoreReplies(paramMap2));
    }

    // Test isAppendAttachment method
    @Test
    public void test_isAppendAttachment_true() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("append_attachment", "true");

        assertEquals(Boolean.TRUE, dataStore.isAppendAttachment(paramMap));
    }

    @Test
    public void test_isAppendAttachment_false() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("append_attachment", "false");

        assertEquals(Boolean.FALSE, dataStore.isAppendAttachment(paramMap));
    }

    @Test
    public void test_isAppendAttachment_defaultValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertEquals("Default should be true", Boolean.TRUE, dataStore.isAppendAttachment(paramMap));
    }

    @Test
    public void test_isAppendAttachment_caseInsensitive() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("append_attachment", "TRUE");
        assertEquals(Boolean.TRUE, dataStore.isAppendAttachment(paramMap1));

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("append_attachment", "False");
        assertEquals(Boolean.FALSE, dataStore.isAppendAttachment(paramMap2));
    }

    // Test isIgnoreSystemEvents method
    @Test
    public void test_isIgnoreSystemEvents_true() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_system_events", "true");

        assertEquals(Boolean.TRUE, dataStore.isIgnoreSystemEvents(paramMap));
    }

    @Test
    public void test_isIgnoreSystemEvents_false() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_system_events", "false");

        assertEquals(Boolean.FALSE, dataStore.isIgnoreSystemEvents(paramMap));
    }

    @Test
    public void test_isIgnoreSystemEvents_defaultValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertEquals("Default should be true", Boolean.TRUE, dataStore.isIgnoreSystemEvents(paramMap));
    }

    @Test
    public void test_isIgnoreSystemEvents_caseInsensitive() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_system_events", "TRUE");
        assertEquals(Boolean.TRUE, dataStore.isIgnoreSystemEvents(paramMap1));

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_system_events", "False");
        assertEquals(Boolean.FALSE, dataStore.isIgnoreSystemEvents(paramMap2));
    }

    // Test getTitleDateformat method
    @Test
    public void test_getTitleDateformat_defaultFormat() {
        final DataStoreParams paramMap = new DataStoreParams();

        final DateTimeFormatter formatter = dataStore.getTitleDateformat(paramMap);
        assertNotNull(formatter);

        // Test that default format works with a sample date
        try {
            formatter.format(java.time.OffsetDateTime.now());
        } catch (Exception e) {
            fail("Default date formatter should work: " + e.getMessage());
        }
    }

    @Test
    public void test_getTitleDateformat_customFormat() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("title_dateformat", "yyyy-MM-dd HH:mm:ss");

        final DateTimeFormatter formatter = dataStore.getTitleDateformat(paramMap);
        assertNotNull(formatter);

        // Verify custom format works
        try {
            final String formatted = formatter.format(java.time.OffsetDateTime.now());
            assertNotNull(formatted);
            assertTrue("Formatted date should contain year", formatted.contains("20"));
        } catch (Exception e) {
            fail("Custom date formatter should work: " + e.getMessage());
        }
    }

    @Test
    public void test_getTitleDateformat_iso8601Format() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("title_dateformat", "yyyy-MM-dd'T'HH:mm:ss");

        final DateTimeFormatter formatter = dataStore.getTitleDateformat(paramMap);
        assertNotNull(formatter);

        // Verify ISO 8601 format works
        try {
            final String formatted = formatter.format(java.time.OffsetDateTime.now());
            assertTrue("ISO 8601 format should contain 'T' separator", formatted.contains("T"));
        } catch (Exception e) {
            fail("ISO 8601 formatter should work: " + e.getMessage());
        }
    }

    // Test getTitleTimezone method
    @Test
    public void test_getTitleTimezone_defaultUTC() {
        final DataStoreParams paramMap = new DataStoreParams();

        final ZoneOffset offset = dataStore.getTitleTimezone(paramMap);
        assertNotNull(offset);
        assertEquals(ZoneOffset.UTC, offset);
    }

    @Test
    public void test_getTitleTimezone_customOffset() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("title_timezone_offset", "+09:00");

        final ZoneOffset offset = dataStore.getTitleTimezone(paramMap);
        assertNotNull(offset);
        assertEquals(ZoneOffset.of("+09:00"), offset);
    }

    @Test
    public void test_getTitleTimezone_negativeOffset() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("title_timezone_offset", "-05:00");

        final ZoneOffset offset = dataStore.getTitleTimezone(paramMap);
        assertNotNull(offset);
        assertEquals(ZoneOffset.of("-05:00"), offset);
    }

    @Test
    public void test_getTitleTimezone_variousFormats() {
        // Test +HH:MM format
        DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("title_timezone_offset", "+01:00");
        assertEquals(ZoneOffset.of("+01:00"), dataStore.getTitleTimezone(paramMap));

        // Test -HH:MM format
        paramMap = new DataStoreParams();
        paramMap.put("title_timezone_offset", "-08:00");
        assertEquals(ZoneOffset.of("-08:00"), dataStore.getTitleTimezone(paramMap));

        // Test Z (UTC) format
        paramMap = new DataStoreParams();
        paramMap.put("title_timezone_offset", "Z");
        assertEquals(ZoneOffset.UTC, dataStore.getTitleTimezone(paramMap));
    }

    // Test isTargetVisibility method
    @Test
    public void test_isTargetVisibility_emptyVisibilities() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[0]);

        // Empty visibilities should accept all
        assertTrue(dataStore.isTargetVisibility(configMap, "Public"));
        assertTrue(dataStore.isTargetVisibility(configMap, "Private"));
        assertTrue(dataStore.isTargetVisibility(configMap, "HiddenMembership"));
    }

    @Test
    public void test_isTargetVisibility_singleVisibility() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[] { "Public" });

        assertTrue(dataStore.isTargetVisibility(configMap, "Public"));
        assertFalse(dataStore.isTargetVisibility(configMap, "Private"));
        assertFalse(dataStore.isTargetVisibility(configMap, "HiddenMembership"));
    }

    @Test
    public void test_isTargetVisibility_multipleVisibilities() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[] { "Public", "Private" });

        assertTrue(dataStore.isTargetVisibility(configMap, "Public"));
        assertTrue(dataStore.isTargetVisibility(configMap, "Private"));
        assertFalse(dataStore.isTargetVisibility(configMap, "HiddenMembership"));
    }

    @Test
    public void test_isTargetVisibility_caseInsensitive() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[] { "Public" });

        assertTrue(dataStore.isTargetVisibility(configMap, "public"));
        assertTrue(dataStore.isTargetVisibility(configMap, "PUBLIC"));
        assertTrue(dataStore.isTargetVisibility(configMap, "PuBlIc"));
    }

    @Test
    public void test_isTargetVisibility_nullVisibility() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[] { "Public" });

        assertFalse(dataStore.isTargetVisibility(configMap, null));
    }

    // Test numberOfThreads parameter
    @Test
    public void test_numberOfThreads_parameter() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("number_of_threads", "1");
        assertEquals("1", paramMap1.getAsString("number_of_threads", "1"));

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("number_of_threads", "5");
        assertEquals("5", paramMap2.getAsString("number_of_threads", "1"));

        final DataStoreParams paramMap3 = new DataStoreParams();
        assertEquals("1", paramMap3.getAsString("number_of_threads", "1"));
    }

    // Test default permissions parameter
    @Test
    public void test_defaultPermissions_parameter() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin,{role}user");

        assertEquals("{role}admin,{role}user", paramMap.getAsString("default_permissions"));
    }

    @Test
    public void test_defaultPermissions_notSet() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertNull(paramMap.getAsString("default_permissions"));
    }

    // Test stripHtmlTags method
    @Test
    public void test_stripHtmlTags_nullInput() {
        assertEquals("", dataStore.stripHtmlTags(null));
    }

    @Test
    public void test_stripHtmlTags_emptyInput() {
        assertEquals("", dataStore.stripHtmlTags(""));
    }

    @Test
    public void test_stripHtmlTags_plainText() {
        assertEquals("plain text", dataStore.stripHtmlTags("plain text"));
        assertEquals("no html here", dataStore.stripHtmlTags("no html here"));
    }

    @Test
    public void test_stripHtmlTags_simpleHtml() {
        assertEquals("bold text", dataStore.stripHtmlTags("<strong>bold text</strong>").trim());
        assertEquals("paragraph", dataStore.stripHtmlTags("<p>paragraph</p>").trim());
        assertEquals("link text", dataStore.stripHtmlTags("<a href=\"url\">link text</a>").trim());
    }

    @Test
    public void test_stripHtmlTags_complexHtml() {
        final String html = "<div><p>This is <strong>bold</strong> and <em>italic</em> text.</p></div>";
        final String expected = "This is bold and italic text.";
        assertEquals(expected, dataStore.stripHtmlTags(html).trim());
    }

    @Test
    public void test_stripHtmlTags_withLineBreaks() {
        // HTMLStripCharFilter converts <br/> and <br> to newlines, not spaces
        final String result1 = dataStore.stripHtmlTags("line1<br/>line2");
        assertTrue("Result should contain line1", result1.contains("line1"));
        assertTrue("Result should contain line2", result1.contains("line2"));

        final String result2 = dataStore.stripHtmlTags("line1<br>line2");
        assertTrue("Result should contain line1", result2.contains("line1"));
        assertTrue("Result should contain line2", result2.contains("line2"));
    }

    @Test
    public void test_stripHtmlTags_noHtmlBrackets() {
        // If no HTML brackets, should return as-is
        assertEquals("text without html", dataStore.stripHtmlTags("text without html"));
        assertEquals("some text", dataStore.stripHtmlTags("some text"));
    }

    @Test
    public void test_stripHtmlTags_withEntities() {
        // HTML entities might be processed depending on HTMLStripCharFilter implementation
        final String result = dataStore.stripHtmlTags("&lt;test&gt;");
        assertNotNull(result);
    }

    // Test buildMessageRoles method

    @Test
    public void test_buildMessageRoles_doesNotModifyTheCallersList() {
        registerPermissionHelper();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin");

        final List<String> channelRoles = new ArrayList<>(List.of("1alice"));

        final List<String> first = dataStore.buildMessageRoles(paramMap, new HashMap<>(), channelRoles);
        final List<String> second = dataStore.buildMessageRoles(paramMap, new HashMap<>(), channelRoles);

        assertEquals("the caller's list must be untouched", 1, channelRoles.size());
        assertEquals(first, second);
        assertEquals(2, first.size());
    }

    @Test
    public void test_buildMessageRoles_appendsDefaultPermissions() {
        registerPermissionHelper();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin,{group}everyone");

        final List<String> roles = dataStore.buildMessageRoles(paramMap, new HashMap<>(), List.of("1alice"));

        final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
        assertEquals(List.of("1alice", permissionHelper.encode("{role}admin"), permissionHelper.encode("{group}everyone")), roles);
    }

    @Test
    public void test_buildMessageRoles_deduplicates() {
        registerPermissionHelper();
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin");

        final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
        final List<String> roles = dataStore.buildMessageRoles(paramMap, new HashMap<>(), List.of(permissionHelper.encode("{role}admin")));

        assertEquals(1, roles.size());
    }

    /**
     * TeamsDataStore was the only one of the six data stores that never folded the data config's
     * own Permissions field -- seeded into {@code defaultDataMap} under the role index field --
     * into a message's ACL, so those roles were silently dropped for Teams documents only. This is
     * a behaviour change: Teams messages gain roles they did not carry before.
     */
    @Test
    public void test_buildMessageRoles_mergesDefaultDataMapRoles() {
        registerPermissionHelper();

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin");

        final Map<String, Object> defaultDataMap = new HashMap<>();
        defaultDataMap.put(ComponentUtil.getFessConfig().getIndexFieldRole(), List.of("1cfgRole"));

        final List<String> roles = dataStore.buildMessageRoles(paramMap, defaultDataMap, new ArrayList<>(List.of("1member")));

        assertTrue("the data config's Permissions roles must reach the message ACL", roles.contains("1cfgRole"));
        assertTrue("membership-derived roles must be kept", roles.contains("1member"));
        assertTrue("default_permissions must still be applied", roles.contains(ComponentUtil.getPermissionHelper().encode("{role}admin")));
    }

    /**
     * The order the six data stores agreed on in Task 5: membership-derived roles, then
     * {@code default_permissions}, then the data config's Permissions field, with {@code distinct()}
     * strictly last.
     */
    @Test
    public void test_buildMessageRoles_ordersDefaultDataMapRolesLast() {
        registerPermissionHelper();

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin");

        final Map<String, Object> defaultDataMap = new HashMap<>();
        defaultDataMap.put(ComponentUtil.getFessConfig().getIndexFieldRole(), List.of("1cfgRole", "1member"));

        final List<String> roles = dataStore.buildMessageRoles(paramMap, defaultDataMap, new ArrayList<>(List.of("1member")));

        assertEquals(List.of("1member", ComponentUtil.getPermissionHelper().encode("{role}admin"), "1cfgRole"), roles);
    }

    /**
     * permissionHelper is not wired into test_app.xml, and it in turn needs systemHelper (also
     * not wired) via its {@code @Resource} field, which plain {@link ComponentUtil#register} does
     * not auto-inject -- the same pattern {@code OneNoteDataStoreTest} and
     * {@code Microsoft365DataStorePermissionTest} use. {@link TestablePermissionHelper} exposes a
     * same-package-crossing setter so the field can be wired by hand.
     */
    private static void registerPermissionHelper() {
        final SystemHelper systemHelper = new SystemHelper();
        ComponentUtil.register(systemHelper, "systemHelper");
        final TestablePermissionHelper permissionHelper = new TestablePermissionHelper();
        permissionHelper.useSystemHelper(systemHelper);
        ComponentUtil.register(permissionHelper, "permissionHelper");
    }

    private static final class TestablePermissionHelper extends PermissionHelper {
        void useSystemHelper(final SystemHelper systemHelper) {
            this.systemHelper = systemHelper;
        }
    }

    // Test processChannelMessages resolves channel membership once per channel

    /**
     * {@code getGroupRoles(client, teamId, channelId)} pages {@code GET
     * /teams/{id}/channels/{id}/members} to exhaustion. Before this task it was called once per
     * message and once per reply, every call returning the same membership -- a channel with 500
     * messages and 2000 replies issued 2500 identical listings.
     *
     * <p>Uses a {@link CountingMicrosoft365Client} subclass -- the fallback the brief offered --
     * rather than a {@code GraphMockServer}: a {@code GraphMockServer} is a strict FIFO
     * {@code MockWebServer} with no path-based dispatcher, so a fixture ordered for the post-hoist
     * call sequence cannot survive the pre-hoist sequence long enough for a count assertion to run
     * (it derails into a {@code CrawlerStatsHelper} exception on mismatched response content
     * instead). Counting calls directly on the client removes that coupling entirely: the
     * assertion below is the only thing that can fail, regardless of call order.
     */
    @Test
    public void test_processChannelMessages_resolvesChannelMembersOncePerChannel() throws Exception {
        registerPermissionHelper();
        registerCrawlerStatsHelper();

        final ChatMessage message1 = new ChatMessage();
        message1.setId("msg-1");
        message1.setWebUrl("https://example.com/msg-1");
        final ChatMessage message2 = new ChatMessage();
        message2.setId("msg-2");
        message2.setWebUrl("https://example.com/msg-2");

        try (CountingMicrosoft365Client client = new CountingMicrosoft365Client(dummyParams(), List.of(message1, message2))) {
            final Group group = new Group();
            group.setId("team-1");
            group.setDisplayName("Team One");

            final Channel channel = new Channel();
            channel.setId("channel-1");
            channel.setDisplayName("General");

            final DataStoreParams paramMap = new DataStoreParams();
            final Map<String, Object> configMap = new HashMap<>();
            configMap.put("ignore_replies", dataStore.isIgnoreReplies(paramMap));
            configMap.put("append_attachment", dataStore.isAppendAttachment(paramMap));
            configMap.put("title_dateformat", dataStore.getTitleDateformat(paramMap));
            configMap.put("title_timezone_offset", dataStore.getTitleTimezone(paramMap));
            configMap.put("ignore_system_events", dataStore.isIgnoreSystemEvents(paramMap));

            final IndexUpdateCallback callback = new IndexUpdateCallback() {
                @Override
                public void store(final DataStoreParams storeParamMap, final Map<String, Object> dataMap) {
                    // no-op: this test asserts only on how many times getChannelMembers is called
                }

                @Override
                public long getDocumentSize() {
                    return 0;
                }

                @Override
                public long getExecuteTime() {
                    return 0;
                }

                @Override
                public void commit() {
                    // do nothing
                }
            };

            dataStore.processChannelMessages(new DataConfig(), callback, paramMap, new HashMap<>(), new HashMap<>(), configMap, client,
                    group, channel);

            assertEquals("channel membership must be fetched once, but was fetched " + client.getChannelMembersCallCount() + " times", 1,
                    client.getChannelMembersCallCount());
        }
    }

    /**
     * Hoisting the membership lookup out of the per-message lambda made it unconditional: a channel
     * that yields no messages started issuing -- and could newly fail on -- a
     * {@code GET /teams/{id}/channels/{id}/members} whose result nothing would ever read. Resolving
     * it on first use keeps "once per channel" without paying for a channel that yields nothing.
     */
    @Test
    public void test_processChannelMessages_doesNotResolveMembersForAChannelWithNoMessages() throws Exception {
        registerPermissionHelper();
        registerCrawlerStatsHelper();

        try (CountingMicrosoft365Client client = new CountingMicrosoft365Client(dummyParams(), List.of())) {
            final Group group = new Group();
            group.setId("team-1");
            group.setDisplayName("Team One");

            final Channel channel = new Channel();
            channel.setId("channel-empty");
            channel.setDisplayName("Empty");

            final DataStoreParams paramMap = new DataStoreParams();
            final Map<String, Object> configMap = new HashMap<>();
            configMap.put("ignore_replies", dataStore.isIgnoreReplies(paramMap));
            configMap.put("append_attachment", dataStore.isAppendAttachment(paramMap));
            configMap.put("title_dateformat", dataStore.getTitleDateformat(paramMap));
            configMap.put("title_timezone_offset", dataStore.getTitleTimezone(paramMap));
            configMap.put("ignore_system_events", dataStore.isIgnoreSystemEvents(paramMap));

            final IndexUpdateCallback callback = new IndexUpdateCallback() {
                @Override
                public void store(final DataStoreParams storeParamMap, final Map<String, Object> dataMap) {
                    fail("a channel with no messages must not store anything");
                }

                @Override
                public long getDocumentSize() {
                    return 0;
                }

                @Override
                public long getExecuteTime() {
                    return 0;
                }

                @Override
                public void commit() {
                    // do nothing
                }
            };

            dataStore.processChannelMessages(new DataConfig(), callback, paramMap, new HashMap<>(), new HashMap<>(), configMap, client,
                    group, channel);

            assertEquals("a channel with no messages must issue no members request, but issued " + client.getChannelMembersCallCount(), 0,
                    client.getChannelMembersCallCount());
        }
    }

    @Test
    public void test_isIgnoreError_defaultIsFalse() {
        assertFalse(dataStore.isIgnoreError(new DataStoreParams()));
    }

    @Test
    public void test_isIgnoreError_true() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_error", "true");
        assertTrue(dataStore.isIgnoreError(paramMap));

        paramMap.put("ignore_error", "TRUE");
        assertTrue("the canonical parser is case-insensitive", dataStore.isIgnoreError(paramMap));
    }

    @Test
    public void test_processChannelMessages_rethrowsByDefault() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        doThrow(new RuntimeException("channel unavailable")).when(client).getTeamMessages(any(), any(), eq("team-1"), eq("channel-1"));

        final Group group = new Group();
        group.setId("team-1");
        group.setDisplayName("Team One");
        final Channel channel = new Channel();
        channel.setId("channel-1");
        channel.setDisplayName("General");

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("ignore_replies", Boolean.FALSE);
        // "ignore_error" deliberately absent: an absent key must mean the default, false.

        try {
            dataStore.processChannelMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap,
                    client, group, channel);
            fail("a channel failure must abort the crawl when ignore_error is unset");
        } catch (final DataStoreException e) {
            assertTrue("expected the channel id in the message, got: " + e.getMessage(), e.getMessage().contains("channel-1"));
        }
    }

    @Test
    public void test_processChannelMessages_ignoreErrorSuppressesChannelFailure() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        doThrow(new RuntimeException("channel unavailable")).when(client).getTeamMessages(any(), any(), eq("team-1"), eq("channel-1"));

        final Group group = new Group();
        group.setId("team-1");
        group.setDisplayName("Team One");
        final Channel channel = new Channel();
        channel.setId("channel-1");
        channel.setDisplayName("General");

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("ignore_replies", Boolean.FALSE);
        configMap.put("ignore_error", Boolean.TRUE);

        // Must return normally: with ignore_error=true the failed channel is skipped, not fatal.
        dataStore.processChannelMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, client,
                group, channel);
    }

    /**
     * {@code processChatMessages} had no error handling at all and {@code storeData}'s
     * try-with-resources has no {@code catch}, so an unreachable {@code chat_id} aborted the whole
     * crawl even with {@code ignore_error=true}. Gated the same way the four team and channel
     * sites are - the path already threw, so gating it leaves an unset {@code ignore_error}
     * aborting exactly as before.
     */
    @Test
    public void test_processChatMessages_rethrowsByDefault() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        doThrow(new RuntimeException("chat unavailable")).when(client).getChatMessages(any(), any(), eq("chat-1"));

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("chat_id", "chat-1");
        // "ignore_error" deliberately absent: an absent key must mean the default, false.

        try {
            dataStore.processChatMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                    client);
            fail("a chat failure must abort the crawl when ignore_error is unset");
        } catch (final DataStoreException e) {
            assertTrue("expected the chat id in the message, got: " + e.getMessage(), e.getMessage().contains("chat-1"));
        }
    }

    @Test
    public void test_processChatMessages_ignoreErrorSuppressesChatFailure() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        doThrow(new RuntimeException("chat unavailable")).when(client).getChatMessages(any(), any(), eq("chat-1"));

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("chat_id", "chat-1");
        configMap.put("ignore_error", Boolean.TRUE);

        // Must return normally: with ignore_error=true the failed chat is skipped, not fatal.
        dataStore.processChatMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                client);
    }

    @Test
    public void test_processTeamMessages_unknownTeamRethrowsByDefault() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.getGroupById("team-missing")).thenReturn(null);

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("team_id", "team-missing");

        try {
            dataStore.processTeamMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                    client);
            fail("an unresolvable team_id must abort the crawl when ignore_error is unset");
        } catch (final DataStoreException e) {
            assertTrue("expected the team id in the message, got: " + e.getMessage(), e.getMessage().contains("team-missing"));
        }
    }

    @Test
    public void test_processTeamMessages_unknownTeamIsSkippedWhenIgnoreError() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.getGroupById("team-missing")).thenReturn(null);

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("team_id", "team-missing");
        configMap.put("ignore_error", Boolean.TRUE);

        // Must return normally, without ever reaching the executor (which is null here).
        dataStore.processTeamMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                client);
    }

    @Test
    public void test_processTeamMessages_unknownChannelRethrowsByDefault() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        final Group group = new Group();
        group.setId("team-1");
        group.setDisplayName("Team One");
        when(client.getGroupById("team-1")).thenReturn(group);
        when(client.getChannelById("team-1", "channel-missing")).thenReturn(null);

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("team_id", "team-1");
        configMap.put("channel_id", "channel-missing");
        configMap.put("include_visibility", new String[0]);

        try {
            dataStore.processTeamMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                    client);
            fail("an unresolvable channel_id must abort the crawl when ignore_error is unset");
        } catch (final DataStoreException e) {
            assertTrue("expected the channel id in the message, got: " + e.getMessage(), e.getMessage().contains("channel-missing"));
        }
    }

    @Test
    public void test_processTeamMessages_unknownChannelIsSkippedWhenIgnoreError() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        final Group group = new Group();
        group.setId("team-1");
        group.setDisplayName("Team One");
        when(client.getGroupById("team-1")).thenReturn(group);
        when(client.getChannelById("team-1", "channel-missing")).thenReturn(null);

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("team_id", "team-1");
        configMap.put("channel_id", "channel-missing");
        configMap.put("include_visibility", new String[0]);
        configMap.put("ignore_error", Boolean.TRUE);

        // Must return normally, without ever reaching the executor (which is null here).
        dataStore.processTeamMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                client);
    }

    @Test
    public void test_processTeamMessages_specificTeamChannelListingRethrowsByDefault() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        final Group group = new Group();
        group.setId("team-1");
        group.setDisplayName("Team One");
        when(client.getGroupById("team-1")).thenReturn(group);
        doThrow(new RuntimeException("channels unavailable")).when(client).getChannels(any(), any(), eq("team-1"));

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("team_id", "team-1");
        configMap.put("channel_id", null);
        configMap.put("include_visibility", new String[0]);

        try {
            dataStore.processTeamMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                    client);
            fail("a channel-listing failure for an explicitly configured team must abort the crawl when ignore_error is unset");
        } catch (final DataStoreException e) {
            assertTrue("expected the team id in the message, got: " + e.getMessage(), e.getMessage().contains("team-1"));
        }
    }

    @Test
    public void test_processTeamMessages_specificTeamChannelListingIsSkippedWhenIgnoreError() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        final Group group = new Group();
        group.setId("team-1");
        group.setDisplayName("Team One");
        when(client.getGroupById("team-1")).thenReturn(group);
        doThrow(new RuntimeException("channels unavailable")).when(client).getChannels(any(), any(), eq("team-1"));

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("team_id", "team-1");
        configMap.put("channel_id", null);
        configMap.put("include_visibility", new String[0]);
        configMap.put("ignore_error", Boolean.TRUE);

        // Must return normally, without ever reaching the executor (which is null here).
        dataStore.processTeamMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                client);
    }

    /**
     * Hazard H5: the all-teams path swallows channel-enumeration failures and continues today.
     * ignore_error defaults to false, so gating that site would turn a tolerated failure into a
     * crawl abort for every existing config that sets nothing. Pins that it stays tolerant.
     */
    @Test
    public void test_processTeamMessages_allTeamsPathStillToleratesChannelFailure() {
        final Group group = new Group();
        group.setId("team-1");
        group.setDisplayName("Team One");

        final Microsoft365Client client = mock(Microsoft365Client.class);
        doAnswer(invocation -> {
            final Consumer<Group> consumer = invocation.getArgument(1);
            consumer.accept(group);
            return null;
        }).when(client).getTeams(any(), any());
        doThrow(new RuntimeException("channels unavailable")).when(client).getChannels(any(), any(), eq("team-1"));

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("team_id", null);
        configMap.put("exclude_team_ids", new String[0]);
        configMap.put("include_visibility", new String[0]);
        // ignore_error deliberately absent: the default must still complete without throwing.

        dataStore.processTeamMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                client);
    }

    /**
     * Hazard H5 again, from the other side: the all-teams path must stay tolerant even when the
     * operator asked for ignore_error. That site never threw, so there is nothing for the flag to
     * relax, and gating it either way would change behaviour nobody asked to change.
     */
    @Test
    public void test_processTeamMessages_allTeamsPathToleratesChannelFailureWithIgnoreError() {
        final Group group = new Group();
        group.setId("team-1");
        group.setDisplayName("Team One");

        final Microsoft365Client client = mock(Microsoft365Client.class);
        doAnswer(invocation -> {
            final Consumer<Group> consumer = invocation.getArgument(1);
            consumer.accept(group);
            return null;
        }).when(client).getTeams(any(), any());
        doThrow(new RuntimeException("channels unavailable")).when(client).getChannels(any(), any(), eq("team-1"));

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("team_id", null);
        configMap.put("exclude_team_ids", new String[0]);
        configMap.put("include_visibility", new String[0]);
        configMap.put("ignore_error", Boolean.TRUE);

        dataStore.processTeamMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                client);
    }

    /**
     * Defect: {@code team_id=} (present but empty) matched neither the specific-team branch
     * ({@code StringUtil.isNotBlank(teamId)}) nor the all-teams branch ({@code teamId == null}), so
     * it fell through both and crawled zero teams while the job still reported success.
     * {@link DataStoreParams#getAsString} returns {@code ""}, not {@code null}, for a parameter
     * that is present but empty, so a bare {@code team_id=} in the data config reaches this.
     * Pins that an empty team_id now behaves exactly like an absent one: the all-teams path runs.
     */
    @Test
    public void test_processTeamMessages_emptyTeamIdCrawlsAllTeamsLikeAbsent() {
        final Group group = new Group();
        group.setId("team-1");
        group.setDisplayName("Team One");

        final Microsoft365Client client = mock(Microsoft365Client.class);
        doAnswer(invocation -> {
            final Consumer<Group> consumer = invocation.getArgument(1);
            consumer.accept(group);
            return null;
        }).when(client).getTeams(any(), any());

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("team_id", "");
        configMap.put("exclude_team_ids", new String[0]);
        configMap.put("include_visibility", new String[0]);

        dataStore.processTeamMessages(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>(), configMap, null,
                client);

        verify(client).getTeams(any(), any());
        verify(client, never()).getGroupById(any());
    }

    /**
     * ignore_error must never widen the crawl. An exclude_team_ids entry that cannot be resolved
     * still aborts even with ignore_error=true, because skipping the lookup would silently crawl a
     * team the operator explicitly asked to exclude.
     */
    @Test
    public void test_getExcludeGroupIdSet_stillThrowsWithIgnoreError() {
        final Microsoft365Client client = mock(Microsoft365Client.class);
        when(client.getGroupById("team-missing")).thenReturn(null);

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("exclude_team_ids", new String[] { "team-missing" });
        configMap.put("ignore_error", Boolean.TRUE);

        try {
            dataStore.getExcludeGroupIdSet(configMap, client);
            fail("an unresolvable exclude_team_ids entry must abort the crawl even when ignore_error is enabled");
        } catch (final DataStoreException e) {
            assertTrue("expected the team id in the message, got: " + e.getMessage(), e.getMessage().contains("team-missing"));
        }
    }

    // Test start_date / end_date

    @Test
    public void test_getStartDate_dateOnlyIsUtcStartOfDay() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("start_date", "2026-01-15");

        final OffsetDateTime startDate = dataStore.getStartDate(paramMap);
        assertEquals(OffsetDateTime.parse("2026-01-15T00:00:00Z"), startDate);
    }

    @Test
    public void test_getEndDate_dateOnlyIsUtcEndOfDay() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("end_date", "2026-01-15");

        final OffsetDateTime endDate = dataStore.getEndDate(paramMap);
        assertEquals(OffsetDateTime.parse("2026-01-15T23:59:59.999999999Z"), endDate);
    }

    @Test
    public void test_getStartDate_offsetDateTimeIsUsedVerbatim() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("start_date", "2026-01-15T09:00:00+09:00");

        assertEquals(OffsetDateTime.parse("2026-01-15T09:00:00+09:00"), dataStore.getStartDate(paramMap));
    }

    /**
     * A malformed bound follows the pattern this plugin already uses for a malformed
     * {@code include_pattern} and a malformed {@code max_content_length}: warn once and fall back
     * to the unset behaviour. It must never abort a crawl, and it must never narrow one either --
     * an operator who typoed a date gets today's unfiltered crawl, not an empty index.
     */
    @Test
    public void test_getStartDate_unsetAndUnparseableMeanNoBound() {
        assertNull("an unset bound must not filter anything", dataStore.getStartDate(new DataStoreParams()));

        final DataStoreParams blank = new DataStoreParams();
        blank.put("start_date", "   ");
        assertNull("a blank bound must not filter anything", dataStore.getStartDate(blank));

        final DataStoreParams garbage = new DataStoreParams();
        garbage.put("start_date", "15/01/2026");
        assertNull("an unparseable bound must be ignored, not thrown", dataStore.getStartDate(garbage));
    }

    @Test
    public void test_getEndDate_unparseableMeansNoBound() {
        final DataStoreParams garbage = new DataStoreParams();
        garbage.put("end_date", "yesterday");
        assertNull("an unparseable bound must be ignored, not thrown", dataStore.getEndDate(garbage));
    }

    @Test
    public void test_isTargetMessageDate_noBoundsAcceptsEverything() {
        final Map<String, Object> configMap = new HashMap<>();
        assertTrue(dataStore.isTargetMessageDate(configMap, messageCreatedAt("2020-01-01T00:00:00Z")));
    }

    @Test
    public void test_isTargetMessageDate_outsideRangeIsRejected() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("start_date", OffsetDateTime.parse("2026-01-01T00:00:00Z"));
        configMap.put("end_date", OffsetDateTime.parse("2026-01-31T23:59:59.999999999Z"));

        assertFalse("before start_date", dataStore.isTargetMessageDate(configMap, messageCreatedAt("2025-12-31T23:59:59Z")));
        assertFalse("after end_date", dataStore.isTargetMessageDate(configMap, messageCreatedAt("2026-02-01T00:00:00Z")));
        assertTrue("inside the range", dataStore.isTargetMessageDate(configMap, messageCreatedAt("2026-01-15T12:00:00Z")));
        assertTrue("start_date is inclusive", dataStore.isTargetMessageDate(configMap, messageCreatedAt("2026-01-01T00:00:00Z")));
        assertTrue("end_date is inclusive", dataStore.isTargetMessageDate(configMap, messageCreatedAt("2026-01-31T23:59:59Z")));
        assertTrue("end_date is inclusive at the exact bound instant",
                dataStore.isTargetMessageDate(configMap, messageCreatedAt("2026-01-31T23:59:59.999999999Z")));
    }

    /**
     * The Graph SDK types {@code createdDateTime} as an {@link OffsetDateTime}: a value it cannot
     * parse throws inside the client's deserializer and never reaches this predicate (that failure
     * lands in the channel-level catch, which {@code ignore_error} gates). What does reach the
     * predicate is an absent timestamp, and a missing timestamp must never be a reason to drop a
     * document -- silently shrinking the index is worse than indexing one extra message.
     */
    @Test
    public void test_isTargetMessageDate_fallsBackToLastModifiedAndFailsOpen() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("start_date", OffsetDateTime.parse("2026-01-01T00:00:00Z"));

        final ChatMessage lastModifiedOnly = new ChatMessage();
        lastModifiedOnly.setLastModifiedDateTime(OffsetDateTime.parse("2026-06-01T00:00:00Z"));
        assertTrue("createdDateTime null must fall back to lastModifiedDateTime",
                dataStore.isTargetMessageDate(configMap, lastModifiedOnly));

        final ChatMessage noTimestamp = new ChatMessage();
        assertTrue("a message with no timestamp at all must be kept, not dropped", dataStore.isTargetMessageDate(configMap, noTimestamp));
    }

    /**
     * The predicate being correct proves nothing if {@code processChatMessage} never calls it. Pins
     * that an out-of-range message is skipped before any indexing work happens.
     */
    @Test
    public void test_processChatMessage_skipsMessageOutsideDateRange() {
        registerPermissionHelper();
        registerCrawlerStatsHelper();

        final DataStoreParams paramMap = new DataStoreParams();
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("ignore_system_events", dataStore.isIgnoreSystemEvents(paramMap));
        configMap.put("append_attachment", Boolean.FALSE);
        configMap.put("title_dateformat", dataStore.getTitleDateformat(paramMap));
        configMap.put("title_timezone_offset", dataStore.getTitleTimezone(paramMap));
        configMap.put("start_date", OffsetDateTime.parse("2026-01-01T00:00:00Z"));

        final ChatMessage message = messageCreatedAt("2025-06-01T00:00:00Z");
        message.setId("message-old");
        message.setWebUrl("https://teams.microsoft.com/l/message/old");

        final CountingIndexUpdateCallback callback = new CountingIndexUpdateCallback();
        final Map<String, Object> result = dataStore.processChatMessage(new DataConfig(), callback, configMap, paramMap, new HashMap<>(),
                new HashMap<>(), new ArrayList<>(), message, map -> {}, null);

        assertNull("an out-of-range message must not be indexed", result);
        assertEquals("an out-of-range message must not reach the index callback", 0, callback.getStoreCount());
    }

    /**
     * Messages versus replies. The filter lives in {@code processChatMessage}, so every message is
     * tested against the window -- root messages and replies alike. Replies are only fetched for a
     * root that was itself indexed, so a root outside the window also excludes its replies, even a
     * reply that would have fallen inside it.
     *
     * <p>That is the deliberate choice, not an accident: it keeps the existing "no parent, no reply
     * fetch" invariant (a reply is never indexed with a {@code parent} that was never processed),
     * and it is the one place where the range actually saves Graph traffic -- the reply listing for
     * an out-of-range root is never issued. The lossy direction is bounded: a reply is always at or
     * after its root, so an {@code end_date} that excludes a root correctly excludes its replies;
     * only {@code start_date} can drop an in-window reply, and only of a conversation whose opening
     * message the operator asked not to index.
     */
    @Test
    public void test_processChannelMessages_outOfRangeRootAlsoExcludesItsReplies() throws Exception {
        registerPermissionHelper();
        registerCrawlerStatsHelper();

        final ChatMessage oldRoot = messageCreatedAt("2025-06-01T00:00:00Z");
        oldRoot.setId("root-old");
        oldRoot.setWebUrl("https://example.com/root-old");

        final ChatMessage recentReply = messageCreatedAt("2026-02-01T00:00:00Z");
        recentReply.setId("reply-recent");
        recentReply.setWebUrl("https://example.com/reply-recent");

        try (ReplyingMicrosoft365Client client = new ReplyingMicrosoft365Client(dummyParams(), List.of(oldRoot), List.of(recentReply))) {
            final Group group = new Group();
            group.setId("team-1");
            group.setDisplayName("Team One");

            final Channel channel = new Channel();
            channel.setId("channel-1");
            channel.setDisplayName("General");

            final DataStoreParams paramMap = new DataStoreParams();
            final Map<String, Object> configMap = new HashMap<>();
            configMap.put("ignore_replies", dataStore.isIgnoreReplies(paramMap));
            configMap.put("append_attachment", dataStore.isAppendAttachment(paramMap));
            configMap.put("title_dateformat", dataStore.getTitleDateformat(paramMap));
            configMap.put("title_timezone_offset", dataStore.getTitleTimezone(paramMap));
            configMap.put("ignore_system_events", dataStore.isIgnoreSystemEvents(paramMap));
            configMap.put("start_date", OffsetDateTime.parse("2026-01-01T00:00:00Z"));

            final CountingIndexUpdateCallback callback = new CountingIndexUpdateCallback();
            dataStore.processChannelMessages(new DataConfig(), callback, paramMap, new HashMap<>(), new HashMap<>(), configMap, client,
                    group, channel);

            assertEquals("nothing may be indexed for an out-of-range root", 0, callback.getStoreCount());
            assertEquals("the reply listing must not even be issued for an out-of-range root", 0, client.getReplyListingCount());
        }
    }

    /**
     * The predicate and its wiring into {@code processChatMessage} are both pinned above, but every
     * one of those tests builds its own {@code configMap}. Without this test the two
     * {@code configMap.put} calls in {@code storeData} could be deleted and the whole suite would
     * stay green while the parameters did nothing. Also pins that an absent parameter puts a
     * {@code null} bound -- the value {@code isTargetMessageDate} reads as "no filtering".
     */
    @Test
    public void test_storeData_parsesBothBoundsIntoTheConfigMap() throws Exception {
        final Map<String, Object> captured = new HashMap<>();
        final Microsoft365Client client = mock(Microsoft365Client.class);
        final TeamsDataStore testDataStore = new TeamsDataStore() {
            @Override
            protected Microsoft365Client createClient(final DataStoreParams paramMap) {
                return client;
            }

            @Override
            protected void processTeamMessages(final DataConfig dataConfig, final IndexUpdateCallback callback,
                    final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
                    final Map<String, Object> configMap, final java.util.concurrent.ExecutorService executorService,
                    final Microsoft365Client c) {
                captured.putAll(configMap);
                captured.put("start_date_present", configMap.containsKey("start_date"));
                captured.put("end_date_present", configMap.containsKey("end_date"));
            }

            @Override
            protected void processChatMessages(final DataConfig dataConfig, final IndexUpdateCallback callback,
                    final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
                    final Map<String, Object> configMap, final java.util.concurrent.ExecutorService executorService,
                    final Microsoft365Client c) {
                // no-op: this test asserts only on what storeData put into the configMap
            }
        };

        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("start_date", "2026-01-01");
        paramMap.put("end_date", "2026-01-31T18:00:00+09:00");
        testDataStore.storeData(new DataConfig(), null, paramMap, new HashMap<>(), new HashMap<>());

        assertEquals("storeData must parse start_date into the configMap", OffsetDateTime.parse("2026-01-01T00:00:00Z"),
                captured.get("start_date"));
        assertEquals("storeData must parse end_date into the configMap", OffsetDateTime.parse("2026-01-31T18:00:00+09:00"),
                captured.get("end_date"));

        captured.clear();
        testDataStore.storeData(new DataConfig(), null, new DataStoreParams(), new HashMap<>(), new HashMap<>());
        assertEquals("an unset start_date must still be put, as null", Boolean.TRUE, captured.get("start_date_present"));
        assertEquals("an unset end_date must still be put, as null", Boolean.TRUE, captured.get("end_date_present"));
        assertNull("an unset start_date must mean no lower bound", captured.get("start_date"));
        assertNull("an unset end_date must mean no upper bound", captured.get("end_date"));
    }

    // Test the inverted date range

    /**
     * {@code start_date} later than {@code end_date} matches no message at all. Applied, it would
     * index nothing while reporting a green crawl, leaving one DEBUG line per skipped message as
     * the only trace. It is reported once and both bounds are dropped instead, so the operator
     * gets the unfiltered crawl that existed before the parameters rather than an empty index.
     */
    @Test
    public void test_putDateRange_invertedRangeWarnsOnceAndIsIgnored() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("start_date", "2026-02-01");
        paramMap.put("end_date", "2026-01-01");

        final Map<String, Object> configMap = new HashMap<>();
        final List<String> warnings = captureTeamsDataStoreWarnings(() -> dataStore.putDateRange(configMap, paramMap));

        final List<String> inverted = warnings.stream().filter(message -> message.contains("inverted")).collect(Collectors.toList());
        assertEquals("expected exactly one inverted-range WARN, got: " + warnings, 1, inverted.size());
        assertNull("an inverted start_date must be dropped, not applied", configMap.get("start_date"));
        assertNull("an inverted end_date must be dropped, not applied", configMap.get("end_date"));
        assertTrue("an inverted range must leave the crawl unfiltered",
                dataStore.isTargetMessageDate(configMap, messageCreatedAt("2020-01-01T00:00:00Z")));
    }

    /**
     * The counterpart: a well-ordered range, and a range with only one bound, must be applied in
     * silence. A warning on a valid configuration would be as bad as no warning on a broken one.
     */
    @Test
    public void test_putDateRange_validRangeIsAppliedWithoutWarning() {
        final DataStoreParams ordered = new DataStoreParams();
        ordered.put("start_date", "2026-01-01");
        ordered.put("end_date", "2026-02-01");

        final Map<String, Object> configMap = new HashMap<>();
        final List<String> warnings = captureTeamsDataStoreWarnings(() -> dataStore.putDateRange(configMap, ordered));

        assertTrue("a well-ordered range must not warn, got: " + warnings,
                warnings.stream().noneMatch(message -> message.contains("inverted")));
        assertEquals(OffsetDateTime.parse("2026-01-01T00:00:00Z"), configMap.get("start_date"));
        assertEquals(OffsetDateTime.parse("2026-02-01T23:59:59.999999999Z"), configMap.get("end_date"));

        // Equal bounds are a one-nanosecond window, not an inverted range.
        final DataStoreParams equal = new DataStoreParams();
        equal.put("start_date", "2026-01-01T00:00:00Z");
        equal.put("end_date", "2026-01-01T00:00:00Z");
        final Map<String, Object> equalConfigMap = new HashMap<>();
        final List<String> equalWarnings = captureTeamsDataStoreWarnings(() -> dataStore.putDateRange(equalConfigMap, equal));
        assertTrue("equal bounds must not warn, got: " + equalWarnings,
                equalWarnings.stream().noneMatch(message -> message.contains("inverted")));
        assertNotNull("equal bounds must still be applied", equalConfigMap.get("start_date"));

        // Only one bound set cannot be inverted.
        final DataStoreParams startOnly = new DataStoreParams();
        startOnly.put("start_date", "2026-01-01");
        final List<String> startOnlyWarnings = captureTeamsDataStoreWarnings(() -> dataStore.putDateRange(new HashMap<>(), startOnly));
        assertTrue("a lone start_date must not warn, got: " + startOnlyWarnings,
                startOnlyWarnings.stream().noneMatch(message -> message.contains("inverted")));
    }

    /**
     * Captures {@code WARN} and above emitted by {@link TeamsDataStore} while {@code action} runs,
     * the same appender-attachment pattern {@code OneNoteDataStoreTest} uses.
     *
     * @param action the code to run.
     * @return the formatted messages, so an assertion failure can be read.
     */
    private static List<String> captureTeamsDataStoreWarnings(final Runnable action) {
        final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());
        final org.apache.logging.log4j.core.Logger coreLogger =
                (org.apache.logging.log4j.core.Logger) LogManager.getLogger(TeamsDataStore.class);
        final AbstractAppender appender =
                new AbstractAppender("test-teams-datastore-warn-capture", null, null, false, Property.EMPTY_ARRAY) {
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
        return events.stream().map(event -> event.getMessage().getFormattedMessage()).collect(Collectors.toList());
    }

    // Test the date range on the consolidated chat document

    /**
     * A chat is indexed as one document, so the range has to be decided for the conversation as a
     * whole. It used to be decided by the consolidated document's timestamp, which
     * {@link TeamsDataStore#createChatMessage} inherits from {@code msgList.get(0)} -- and
     * {@code getChatMessages} sets no {@code $orderby} and does not sort, so that is whichever
     * message Graph returned first, documented as the most recently modified one. A chat running
     * from 2024 to 2026 crawled with {@code end_date=2024-12-31} was therefore judged by its 2026
     * message and dropped whole, every in-range message with it.
     */
    @Test
    public void test_processChatMessages_keepsChatWhenAnyMessageIsInRange() throws Exception {
        registerPermissionHelper();
        registerCrawlerStatsHelper();

        // Graph's default ordering puts the newest message first, and it is far outside the range.
        final ChatMessage newest = chatMessageCreatedAt("2026-08-01T00:00:00Z");
        final ChatMessage inRange = chatMessageCreatedAt("2024-06-15T00:00:00Z");

        final CountingIndexUpdateCallback callback = new CountingIndexUpdateCallback();
        runChatCrawl(List.of(newest, inRange), OffsetDateTime.parse("2024-12-31T23:59:59.999999999Z"), callback);

        assertEquals("a chat containing an in-range message must be indexed", 1, callback.getStoreCount());
    }

    /**
     * The counterpart: the range must still exclude a chat, or it would not be a filter at all.
     */
    @Test
    public void test_processChatMessages_dropsChatWithNoMessageInRange() throws Exception {
        registerPermissionHelper();
        registerCrawlerStatsHelper();

        final ChatMessage newest = chatMessageCreatedAt("2026-08-01T00:00:00Z");
        final ChatMessage alsoOutOfRange = chatMessageCreatedAt("2025-06-15T00:00:00Z");

        final CountingIndexUpdateCallback callback = new CountingIndexUpdateCallback();
        runChatCrawl(List.of(newest, alsoOutOfRange), OffsetDateTime.parse("2024-12-31T23:59:59.999999999Z"), callback);

        assertEquals("a chat with no message in range must not be indexed", 0, callback.getStoreCount());
    }

    @Test
    public void test_isTargetChatDate_noBoundsAcceptsEverything() {
        final Map<String, Object> configMap = new HashMap<>();
        assertTrue("with no bounds a chat must never be filtered",
                dataStore.isTargetChatDate(configMap, List.of(messageCreatedAt("2020-01-01T00:00:00Z"))));
    }

    /**
     * The bounds must be cleared for the consolidated document, or the per-message guard in
     * {@code processChatMessage} would re-decide the range from the one synthetic timestamp the
     * consolidated document carries and overturn the chat-level decision.
     */
    @Test
    public void test_withoutDateRange_clearsBothBoundsAndKeepsEverythingElse() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("start_date", OffsetDateTime.parse("2026-01-01T00:00:00Z"));
        configMap.put("end_date", OffsetDateTime.parse("2026-01-31T00:00:00Z"));
        configMap.put("append_attachment", Boolean.TRUE);

        final Map<String, Object> copy = dataStore.withoutDateRange(configMap);

        assertNull("start_date must be cleared", copy.get("start_date"));
        assertNull("end_date must be cleared", copy.get("end_date"));
        assertEquals("every other setting must survive", Boolean.TRUE, copy.get("append_attachment"));
        assertNotNull("the caller's map must not be modified", configMap.get("start_date"));
    }

    /**
     * Runs one {@code chat_id} crawl against a stubbed client and waits for the submitted task.
     *
     * @param messages the chat's messages, in the order the client yields them.
     * @param endDate the upper bound to crawl with.
     * @param callback the callback counting what was indexed.
     */
    private void runChatCrawl(final List<ChatMessage> messages, final OffsetDateTime endDate, final CountingIndexUpdateCallback callback)
            throws Exception {
        try (ChattingMicrosoft365Client client = new ChattingMicrosoft365Client(dummyParams(), messages)) {
            final DataStoreParams paramMap = new DataStoreParams();
            final Map<String, Object> configMap = new HashMap<>();
            configMap.put("chat_id", "chat-1");
            configMap.put("append_attachment", Boolean.FALSE);
            configMap.put("title_dateformat", dataStore.getTitleDateformat(paramMap));
            configMap.put("title_timezone_offset", dataStore.getTitleTimezone(paramMap));
            configMap.put("ignore_system_events", dataStore.isIgnoreSystemEvents(paramMap));
            configMap.put("end_date", endDate);

            final ExecutorService executorService = Executors.newSingleThreadExecutor();
            try {
                dataStore.processChatMessages(new DataConfig(), callback, paramMap, new HashMap<>(), new HashMap<>(), configMap,
                        executorService, client);
            } finally {
                executorService.shutdown();
                assertTrue("the chat task must finish", executorService.awaitTermination(10, TimeUnit.SECONDS));
            }
        }
    }

    /**
     * {@code createChatMessage} concatenates each message's attachments, mentions and reactions,
     * so those must be non-null lists for a fixture that reaches it.
     */
    private static ChatMessage chatMessageCreatedAt(final String isoInstant) {
        final ChatMessage message = messageCreatedAt(isoInstant);
        message.setAttachments(new ArrayList<>());
        message.setMentions(new ArrayList<>());
        message.setReactions(new ArrayList<>());
        return message;
    }

    /** Feeds a fixed list of chat messages and no members, without issuing any Graph traffic. */
    private static final class ChattingMicrosoft365Client extends Microsoft365Client {
        private final List<ChatMessage> messages;

        ChattingMicrosoft365Client(final DataStoreParams params, final List<ChatMessage> messages) {
            super(params);
            this.messages = messages;
        }

        @Override
        public void getChatMessages(final List<Object> options, final Consumer<ChatMessage> consumer, final String chatId) {
            messages.forEach(consumer::accept);
        }

        @Override
        public void getChatMembers(final List<Object> options, final Consumer<ConversationMember> consumer, final String chatId) {
            // No members are needed: these tests assert only on what is indexed.
        }
    }

    private static ChatMessage messageCreatedAt(final String isoInstant) {
        final ChatMessage message = new ChatMessage();
        message.setCreatedDateTime(OffsetDateTime.parse(isoInstant));
        return message;
    }

    /**
     * An {@link IndexUpdateCallback} that counts {@code store} calls, so a test can assert a
     * message was <em>not</em> indexed without relying on an exception from a null callback.
     */
    private static final class CountingIndexUpdateCallback implements IndexUpdateCallback {
        private int storeCount;

        int getStoreCount() {
            return storeCount;
        }

        @Override
        public void store(final DataStoreParams storeParamMap, final Map<String, Object> dataMap) {
            storeCount++;
        }

        @Override
        public long getDocumentSize() {
            return storeCount;
        }

        @Override
        public long getExecuteTime() {
            return 0;
        }

        @Override
        public void commit() {
            // do nothing
        }
    }

    /**
     * Feeds a fixed list of root messages and, for any root whose replies are requested, a fixed
     * list of replies, counting how many times the reply listing was issued.
     */
    private static final class ReplyingMicrosoft365Client extends Microsoft365Client {
        private final List<ChatMessage> messages;
        private final List<ChatMessage> replies;
        private int replyListingCount;

        ReplyingMicrosoft365Client(final DataStoreParams params, final List<ChatMessage> messages, final List<ChatMessage> replies) {
            super(params);
            this.messages = messages;
            this.replies = replies;
        }

        int getReplyListingCount() {
            return replyListingCount;
        }

        @Override
        public void getChannelMembers(final List<Object> options, final Consumer<ConversationMember> consumer, final String teamId,
                final String channelId) {
            // No members are needed: this fixture asserts only on what is indexed.
        }

        @Override
        public void getTeamMessages(final List<Object> options, final Consumer<ChatMessage> consumer, final String teamId,
                final String channelId) {
            messages.forEach(consumer::accept);
        }

        @Override
        public void getTeamReplyMessages(final List<Object> options, final Consumer<ChatMessage> consumer, final String teamId,
                final String channelId, final String messageId) {
            replyListingCount++;
            replies.forEach(consumer::accept);
        }
    }

    /**
     * Credentials are never used: {@code ClientSecretCredential} acquires tokens lazily, so
     * construction is offline.
     */
    private static DataStoreParams dummyParams() {
        final DataStoreParams params = new DataStoreParams();
        params.put("tenant", "dummy-tenant");
        params.put("client_id", "dummy-client-id");
        params.put("client_secret", "dummy-client-secret");
        return params;
    }

    /**
     * crawlerStatsHelper is not wired into test_app.xml either -- {@code processChatMessage}
     * calls {@code ComponentUtil.getCrawlerStatsHelper()} directly, the same pattern
     * {@code SharePointPageDataStoreTest} and {@code SharePointListDataStoreTest} use.
     */
    private static void registerCrawlerStatsHelper() {
        final CrawlerStatsHelper crawlerStatsHelper = new CrawlerStatsHelper();
        crawlerStatsHelper.init();
        ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
    }

    /**
     * A {@link Microsoft365Client} that counts {@code getChannelMembers} calls instead of issuing
     * any Graph traffic, and feeds a fixed list of messages with no replies. Overriding at the
     * client layer (rather than mocking with Mockito) keeps {@link TeamsDataStore#processChannelMessages}
     * exercised completely unmodified, exactly as {@code MockableMicrosoft365Client}-style
     * subclasses do elsewhere in this test suite (see {@code OneNoteDataStoreTest},
     * {@code SharePointPageDataStoreTest}) -- just without the {@code GraphMockServer} wiring,
     * since no HTTP traffic needs to flow for this test's assertion.
     */
    private static final class CountingMicrosoft365Client extends Microsoft365Client {
        private final List<ChatMessage> messages;
        private int channelMembersCallCount;

        CountingMicrosoft365Client(final DataStoreParams params, final List<ChatMessage> messages) {
            super(params);
            this.messages = messages;
        }

        int getChannelMembersCallCount() {
            return channelMembersCallCount;
        }

        @Override
        public void getChannelMembers(final List<Object> options, final Consumer<ConversationMember> consumer, final String teamId,
                final String channelId) {
            channelMembersCallCount++;
            // No members are needed: this test only cares about how many times this is called.
        }

        @Override
        public void getTeamMessages(final List<Object> options, final Consumer<ChatMessage> consumer, final String teamId,
                final String channelId) {
            messages.forEach(consumer::accept);
        }

        @Override
        public void getTeamReplyMessages(final List<Object> options, final Consumer<ChatMessage> consumer, final String teamId,
                final String channelId, final String messageId) {
            // No replies in this fixture.
        }
    }
}

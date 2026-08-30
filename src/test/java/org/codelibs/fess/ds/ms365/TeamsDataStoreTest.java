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
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

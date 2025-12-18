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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.codelibs.fess.ds.ms365;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

public class TeamsDataStoreTest extends LastaFluteTestCase {

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
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new TeamsDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        dataStore = null;
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("TeamsDataStore", dataStore.getName());
    }

    // Test normalizeTextContent method
    public void test_normalizeTextContent_nullInput() {
        assertEquals("", dataStore.normalizeTextContent(null));
    }

    public void test_normalizeTextContent_emptyInput() {
        assertEquals("", dataStore.normalizeTextContent(""));
        assertEquals("", dataStore.normalizeTextContent(" "));
        assertEquals("", dataStore.normalizeTextContent("   "));
    }

    public void test_normalizeTextContent_simpleText() {
        assertEquals("test", dataStore.normalizeTextContent(" test "));
        assertEquals("hello world", dataStore.normalizeTextContent("hello world"));
        assertEquals("test message", dataStore.normalizeTextContent("  test message  "));
    }

    public void test_normalizeTextContent_withAttachmentTags() {
        assertEquals("test", dataStore.normalizeTextContent(" test <attachment></attachment>"));
        assertEquals("before  after", dataStore.normalizeTextContent("before <attachment></attachment> after"));
        assertEquals("text", dataStore.normalizeTextContent("<attachment></attachment>text<attachment></attachment>"));
    }

    public void test_normalizeTextContent_withAttachmentAttributes() {
        assertEquals("test", dataStore.normalizeTextContent(" test <attachment id=\"123\"></attachment>"));
        assertEquals("message", dataStore.normalizeTextContent("<attachment name=\"file.pdf\"></attachment> message "));
        assertEquals("content", dataStore.normalizeTextContent("content<attachment id=\"abc\" name=\"doc.docx\"></attachment>"));
    }

    public void test_normalizeTextContent_multipleAttachments() {
        assertEquals("text  between", dataStore
                .normalizeTextContent("<attachment></attachment> text <attachment></attachment> between <attachment></attachment>"));
        assertEquals("start  end", dataStore.normalizeTextContent("start <attachment></attachment><attachment></attachment> end"));
    }

    public void test_normalizeTextContent_preserveOtherHtml() {
        // Other HTML tags should be preserved (only attachment tags are removed)
        assertEquals("<p>test</p>", dataStore.normalizeTextContent("<p>test</p>"));
        assertEquals("<div>content</div>", dataStore.normalizeTextContent("<div>content</div>"));
        assertEquals("<strong>bold</strong> text", dataStore.normalizeTextContent("<strong>bold</strong> text"));
    }

    // Test getTeamId method
    public void test_getTeamId_withValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("team_id", "test-team-id");

        assertEquals("test-team-id", dataStore.getTeamId(paramMap));
    }

    public void test_getTeamId_withoutValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertNull(dataStore.getTeamId(paramMap));
    }

    // Test getExcludeTeamIds method
    public void test_getExcludeTeamIds_singleTeam() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_team_ids", "team-1");

        final String[] excludeIds = dataStore.getExcludeTeamIds(paramMap);
        assertNotNull(excludeIds);
        assertEquals(1, excludeIds.length);
        assertEquals("team-1", excludeIds[0]);
    }

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

    public void test_getExcludeTeamIds_emptyString() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("exclude_team_ids", "");

        final String[] excludeIds = dataStore.getExcludeTeamIds(paramMap);
        assertNotNull(excludeIds);
        assertEquals(0, excludeIds.length);
    }

    public void test_getExcludeTeamIds_notSet() {
        final DataStoreParams paramMap = new DataStoreParams();

        final String[] excludeIds = dataStore.getExcludeTeamIds(paramMap);
        assertNotNull(excludeIds);
        assertEquals(0, excludeIds.length);
    }

    // Test getIncludeVisibilities method
    public void test_getIncludeVisibilities_singleVisibility() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_visibility", "Public");

        final String[] visibilities = dataStore.getIncludeVisibilities(paramMap);
        assertNotNull(visibilities);
        assertEquals(1, visibilities.length);
        assertEquals("Public", visibilities[0]);
    }

    public void test_getIncludeVisibilities_multipleVisibilities() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_visibility", "Public,Private");

        final String[] visibilities = dataStore.getIncludeVisibilities(paramMap);
        assertNotNull(visibilities);
        assertEquals(2, visibilities.length);
        assertEquals("Public", visibilities[0]);
        assertEquals("Private", visibilities[1]);
    }

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

    public void test_getIncludeVisibilities_emptyString() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("include_visibility", "");

        final String[] visibilities = dataStore.getIncludeVisibilities(paramMap);
        assertNotNull(visibilities);
        assertEquals(0, visibilities.length);
    }

    public void test_getIncludeVisibilities_notSet() {
        final DataStoreParams paramMap = new DataStoreParams();

        final String[] visibilities = dataStore.getIncludeVisibilities(paramMap);
        assertNotNull(visibilities);
        assertEquals(0, visibilities.length);
    }

    // Test getChannelId method
    public void test_getChannelId_withValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("channel_id", "test-channel-id");

        assertEquals("test-channel-id", dataStore.getChannelId(paramMap));
    }

    public void test_getChannelId_withoutValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertNull(dataStore.getChannelId(paramMap));
    }

    // Test getChatId method
    public void test_getChatId_withValue() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("chat_id", "test-chat-id");

        assertEquals("test-chat-id", dataStore.getChatId(paramMap));
    }

    public void test_getChatId_withoutValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertNull(dataStore.getChatId(paramMap));
    }

    // Test isIgnoreReplies method
    public void test_isIgnoreReplies_true() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_replies", "true");

        assertTrue(dataStore.isIgnoreReplies(paramMap));
    }

    public void test_isIgnoreReplies_false() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_replies", "false");

        assertFalse(dataStore.isIgnoreReplies(paramMap));
    }

    public void test_isIgnoreReplies_defaultValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertFalse("Default should be false", dataStore.isIgnoreReplies(paramMap));
    }

    public void test_isIgnoreReplies_caseInsensitive() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_replies", "TRUE");
        assertTrue(dataStore.isIgnoreReplies(paramMap1));

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_replies", "False");
        assertFalse(dataStore.isIgnoreReplies(paramMap2));
    }

    // Test isAppendAttachment method
    public void test_isAppendAttachment_true() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("append_attachment", "true");

        assertEquals(Boolean.TRUE, dataStore.isAppendAttachment(paramMap));
    }

    public void test_isAppendAttachment_false() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("append_attachment", "false");

        assertEquals(Boolean.FALSE, dataStore.isAppendAttachment(paramMap));
    }

    public void test_isAppendAttachment_defaultValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertEquals("Default should be true", Boolean.TRUE, dataStore.isAppendAttachment(paramMap));
    }

    public void test_isAppendAttachment_caseInsensitive() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("append_attachment", "TRUE");
        assertEquals(Boolean.TRUE, dataStore.isAppendAttachment(paramMap1));

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("append_attachment", "False");
        assertEquals(Boolean.FALSE, dataStore.isAppendAttachment(paramMap2));
    }

    // Test isIgnoreSystemEvents method
    public void test_isIgnoreSystemEvents_true() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_system_events", "true");

        assertEquals(Boolean.TRUE, dataStore.isIgnoreSystemEvents(paramMap));
    }

    public void test_isIgnoreSystemEvents_false() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("ignore_system_events", "false");

        assertEquals(Boolean.FALSE, dataStore.isIgnoreSystemEvents(paramMap));
    }

    public void test_isIgnoreSystemEvents_defaultValue() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertEquals("Default should be true", Boolean.TRUE, dataStore.isIgnoreSystemEvents(paramMap));
    }

    public void test_isIgnoreSystemEvents_caseInsensitive() {
        final DataStoreParams paramMap1 = new DataStoreParams();
        paramMap1.put("ignore_system_events", "TRUE");
        assertEquals(Boolean.TRUE, dataStore.isIgnoreSystemEvents(paramMap1));

        final DataStoreParams paramMap2 = new DataStoreParams();
        paramMap2.put("ignore_system_events", "False");
        assertEquals(Boolean.FALSE, dataStore.isIgnoreSystemEvents(paramMap2));
    }

    // Test getTitleDateformat method
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
    public void test_getTitleTimezone_defaultUTC() {
        final DataStoreParams paramMap = new DataStoreParams();

        final ZoneOffset offset = dataStore.getTitleTimezone(paramMap);
        assertNotNull(offset);
        assertEquals(ZoneOffset.UTC, offset);
    }

    public void test_getTitleTimezone_customOffset() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("title_timezone_offset", "+09:00");

        final ZoneOffset offset = dataStore.getTitleTimezone(paramMap);
        assertNotNull(offset);
        assertEquals(ZoneOffset.of("+09:00"), offset);
    }

    public void test_getTitleTimezone_negativeOffset() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("title_timezone_offset", "-05:00");

        final ZoneOffset offset = dataStore.getTitleTimezone(paramMap);
        assertNotNull(offset);
        assertEquals(ZoneOffset.of("-05:00"), offset);
    }

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
    public void test_isTargetVisibility_emptyVisibilities() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[0]);

        // Empty visibilities should accept all
        assertTrue(dataStore.isTargetVisibility(configMap, "Public"));
        assertTrue(dataStore.isTargetVisibility(configMap, "Private"));
        assertTrue(dataStore.isTargetVisibility(configMap, "HiddenMembership"));
    }

    public void test_isTargetVisibility_singleVisibility() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[] { "Public" });

        assertTrue(dataStore.isTargetVisibility(configMap, "Public"));
        assertFalse(dataStore.isTargetVisibility(configMap, "Private"));
        assertFalse(dataStore.isTargetVisibility(configMap, "HiddenMembership"));
    }

    public void test_isTargetVisibility_multipleVisibilities() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[] { "Public", "Private" });

        assertTrue(dataStore.isTargetVisibility(configMap, "Public"));
        assertTrue(dataStore.isTargetVisibility(configMap, "Private"));
        assertFalse(dataStore.isTargetVisibility(configMap, "HiddenMembership"));
    }

    public void test_isTargetVisibility_caseInsensitive() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[] { "Public" });

        assertTrue(dataStore.isTargetVisibility(configMap, "public"));
        assertTrue(dataStore.isTargetVisibility(configMap, "PUBLIC"));
        assertTrue(dataStore.isTargetVisibility(configMap, "PuBlIc"));
    }

    public void test_isTargetVisibility_nullVisibility() {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put("include_visibility", new String[] { "Public" });

        assertFalse(dataStore.isTargetVisibility(configMap, null));
    }

    // Test numberOfThreads parameter
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
    public void test_defaultPermissions_parameter() {
        final DataStoreParams paramMap = new DataStoreParams();
        paramMap.put("default_permissions", "{role}admin,{role}user");

        assertEquals("{role}admin,{role}user", paramMap.getAsString("default_permissions"));
    }

    public void test_defaultPermissions_notSet() {
        final DataStoreParams paramMap = new DataStoreParams();

        assertNull(paramMap.getAsString("default_permissions"));
    }

    // Test stripHtmlTags method
    public void test_stripHtmlTags_nullInput() {
        assertEquals("", dataStore.stripHtmlTags(null));
    }

    public void test_stripHtmlTags_emptyInput() {
        assertEquals("", dataStore.stripHtmlTags(""));
    }

    public void test_stripHtmlTags_plainText() {
        assertEquals("plain text", dataStore.stripHtmlTags("plain text"));
        assertEquals("no html here", dataStore.stripHtmlTags("no html here"));
    }

    public void test_stripHtmlTags_simpleHtml() {
        assertEquals("bold text", dataStore.stripHtmlTags("<strong>bold text</strong>").trim());
        assertEquals("paragraph", dataStore.stripHtmlTags("<p>paragraph</p>").trim());
        assertEquals("link text", dataStore.stripHtmlTags("<a href=\"url\">link text</a>").trim());
    }

    public void test_stripHtmlTags_complexHtml() {
        final String html = "<div><p>This is <strong>bold</strong> and <em>italic</em> text.</p></div>";
        final String expected = "This is bold and italic text.";
        assertEquals(expected, dataStore.stripHtmlTags(html).trim());
    }

    public void test_stripHtmlTags_withLineBreaks() {
        // HTMLStripCharFilter converts <br/> and <br> to newlines, not spaces
        final String result1 = dataStore.stripHtmlTags("line1<br/>line2");
        assertTrue("Result should contain line1", result1.contains("line1"));
        assertTrue("Result should contain line2", result1.contains("line2"));

        final String result2 = dataStore.stripHtmlTags("line1<br>line2");
        assertTrue("Result should contain line1", result2.contains("line1"));
        assertTrue("Result should contain line2", result2.contains("line2"));
    }

    public void test_stripHtmlTags_noHtmlBrackets() {
        // If no HTML brackets, should return as-is
        assertEquals("text without html", dataStore.stripHtmlTags("text without html"));
        assertEquals("some text", dataStore.stripHtmlTags("some text"));
    }

    public void test_stripHtmlTags_withEntities() {
        // HTML entities might be processed depending on HTMLStripCharFilter implementation
        final String result = dataStore.stripHtmlTags("&lt;test&gt;");
        assertNotNull(result);
    }
}

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

import java.io.IOException;
import java.io.StringReader;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client.UserType;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.exception.FessSystemException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.AadUserConversationMember;
import com.microsoft.graph.models.BodyType;
import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.ChatMessage;
import com.microsoft.graph.models.ChatMessageFromIdentitySet;
import com.microsoft.graph.models.ConversationMember;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.ItemBody;

/**
 * This class is a data store for crawling and indexing content from Microsoft Teams.
 * It supports crawling messages from teams, channels, and chats.
 * It extracts message content, metadata, attachments, and permissions for indexing.
 */
public class TeamsDataStore extends Microsoft365DataStore {

    /**
     * Default constructor.
     */
    public TeamsDataStore() {
        super();
    }

    /** Key for the message title. */
    private static final String MESSAGE_TITLE = "title";

    /** Key for the message content. */
    private static final String MESSAGE_CONTENT = "content";

    private static final Logger logger = LogManager.getLogger(TeamsDataStore.class);

    // parameters
    /** Parameter name for the team ID. */
    private static final String TEAM_ID = "team_id";
    /** Parameter name for the exclude team IDs. */
    private static final String EXCLUDE_TEAM_ID = "exclude_team_ids";
    /** Parameter name for the include visibility. */
    private static final String INCLUDE_VISIBILITY = "include_visibility";
    /** Parameter name for the channel ID. */
    private static final String CHANNEL_ID = "channel_id";
    /** Parameter name for the chat ID. */
    private static final String CHAT_ID = "chat_id";
    /** Parameter name for the number of threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** Parameter name for default permissions. */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** Parameter name for ignoring replies. */
    private static final String IGNORE_REPLIES = "ignore_replies";
    /** Parameter name for appending attachments. */
    private static final String APPEND_ATTACHMENT = "append_attachment";
    /** Parameter name for ignoring system events. */
    private static final String IGNORE_SYSTEM_EVENTS = "ignore_system_events";
    /** Parameter name for the title date format. */
    private static final String TITLE_DATEFORMAT = "title_dateformat";
    /** Parameter name for the title timezone offset. */
    private static final String TITLE_TIMEZONE = "title_timezone_offset";

    // scripts
    /** Key for the message object in the script map. */
    private static final String MESSAGE = "message";
    /** Key for the message attachments in the script map (internal use only). */
    private static final String MESSAGE_ATTACHMENTS = "attachments"; // internal user only
    /** Key for the message body in the script map. */
    private static final String MESSAGE_BODY = "body";
    /** Key for the message channel identity in the script map. */
    private static final String MESSAGE_CHANNEL_IDENTITY = "channel_identity";
    /** Key for the message chat ID in the script map. */
    private static final String MESSAGE_CHAT_ID = "chat_id";
    /** Key for the message created date time in the script map. */
    private static final String MESSAGE_CREATED_DATE_TIME = "created_date_time";
    /** Key for the message deleted date time in the script map. */
    private static final String MESSAGE_DELETED_DATE_TIME = "deleted_date_time";
    /** Key for the message eTag in the script map. */
    private static final String MESSAGE_ETAG = "etag";
    /** Key for the message from in the script map. */
    private static final String MESSAGE_FROM = "from";
    /** Key for the message hosted contents in the script map (internal use only). */
    private static final String MESSAGE_HOSTED_CONTENTS = "hosted_contents"; // internal user only
    /** Key for the message ID in the script map. */
    private static final String MESSAGE_ID = "id";
    /** Key for the message importance in the script map. */
    private static final String MESSAGE_IMPORTANCE = "importance";
    /** Key for the message last edited date time in the script map. */
    private static final String MESSAGE_LAST_EDITED_DATE_TIME = "last_edited_date_time";
    /** Key for the message last modified date time in the script map. */
    private static final String MESSAGE_LAST_MODIFIED_DATE_TIME = "last_modified_date_time";
    /** Key for the message locale in the script map. */
    private static final String MESSAGE_LOCALE = "locale";
    /** Key for the message mentions in the script map. */
    private static final String MESSAGE_MENTIONS = "mentions";
    /** Key for the message replies in the script map (internal use only). */
    private static final String MESSAGE_REPLIES = "replies"; // internal user only
    /** Key for the message reply to ID in the script map. */
    private static final String MESSAGE_REPLY_TO_ID = "reply_to_id";
    /** Key for the message subject in the script map. */
    private static final String MESSAGE_SUBJECT = "subject";
    /** Key for the message summary in the script map. */
    private static final String MESSAGE_SUMMARY = "summary";
    /** Key for the message web URL in the script map. */
    private static final String MESSAGE_WEB_URL = "web_url";
    /** Key for the message roles in the script map. */
    private static final String MESSAGE_ROLES = "roles";
    /** Key for the parent object in the script map. */
    private static final String PARENT = "parent";
    /** Key for the team object in the script map. */
    private static final String TEAM = "team";
    /** Key for the channel object in the script map. */
    private static final String CHANNEL = "channel";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(TEAM_ID, getTeamId(paramMap));
        configMap.put(EXCLUDE_TEAM_ID, getExcludeTeamIds(paramMap));
        configMap.put(INCLUDE_VISIBILITY, getIncludeVisibilities(paramMap));
        configMap.put(CHANNEL_ID, getChannelId(paramMap));
        configMap.put(CHAT_ID, getChatId(paramMap));
        configMap.put(IGNORE_REPLIES, isIgnoreReplies(paramMap));
        configMap.put(APPEND_ATTACHMENT, isAppendAttachment(paramMap));
        configMap.put(TITLE_DATEFORMAT, getTitleDateformat(paramMap));
        configMap.put(TITLE_TIMEZONE, getTitleTimezone(paramMap));
        configMap.put(IGNORE_SYSTEM_EVENTS, isIgnoreSystemEvents(paramMap));

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Teams crawling started - Configuration: TeamID={}, ChannelID={}, ChatID={}, IgnoreReplies={}, AppendAttachment={}, Threads={}",
                    configMap.get(TEAM_ID), configMap.get(CHANNEL_ID), configMap.get(CHAT_ID), configMap.get(IGNORE_REPLIES),
                    configMap.get(APPEND_ATTACHMENT), paramMap.getAsString(NUMBER_OF_THREADS, "1"));
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Starting Teams messages processing");
            }
            processTeamMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, client);

            if (logger.isDebugEnabled()) {
                logger.debug("Starting Chat messages processing");
            }
            processChatMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, client);

            if (logger.isDebugEnabled()) {
                logger.debug("Teams crawling completed - shutting down thread executor");
            }
            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Processes chat messages.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param configMap The configuration map.
     * @param client The Microsoft365Client.
     */
    protected void processChatMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final Microsoft365Client client) {
        final String chatId = (String) configMap.get(CHAT_ID);

        if (StringUtil.isNotBlank(chatId)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing messages for specific chat: {}", chatId);
            }

            final List<ChatMessage> msgList = new ArrayList<>();

            client.getChatMessages(Collections.emptyList(), m -> {
                msgList.add(m);
                if (logger.isDebugEnabled()) {
                    logger.debug("Retrieved chat: {}", chatId);
                }
            }, chatId);

            if (!msgList.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Creating consolidated chat message from {} individual messages for chat: {}", msgList.size(), chatId);
                }

                final ChatMessage m = createChatMessage(msgList, client);
                processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, getGroupRoles(client, chatId), m,
                        map -> map.put("messages", msgList), client);

                if (logger.isDebugEnabled()) {
                    logger.debug("Successfully processed consolidated chat message for chat: {} with {} individual messages", chatId,
                            msgList.size());
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("No messages found for chat: {}", chatId);
                }
            }
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("No specific chat ID configured - skipping chat message processing");
            }
        }
    }

    /**
     * Creates a chat message from a list of messages.
     *
     * @param msgList The list of chat messages.
     * @param client The Microsoft365Client.
     * @return A new chat message.
     */
    protected ChatMessage createChatMessage(final List<ChatMessage> msgList, final Microsoft365Client client) {
        final ChatMessage msg = new ChatMessage();
        final ChatMessage defaultMsg = msgList.get(0);
        msg.setAttachments(new ArrayList<>());
        msgList.stream().forEach(m -> msg.getAttachments().addAll(m.getAttachments()));
        final ItemBody body = new ItemBody();
        body.setContentType(BodyType.Text);
        msg.setBody(body);
        final StringBuilder bodyBuf = new StringBuilder(1000);
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(APPEND_ATTACHMENT, false);
        msgList.stream().forEach(m -> bodyBuf.append(getContent(configMap, m, client)));
        body.setContent(bodyBuf.toString());
        msg.setChannelIdentity(defaultMsg.getChannelIdentity());
        msg.setCreatedDateTime(defaultMsg.getCreatedDateTime());
        msg.setDeletedDateTime(defaultMsg.getDeletedDateTime());
        msg.setEtag(defaultMsg.getEtag());
        msg.setFrom(defaultMsg.getFrom());
        msg.setImportance(defaultMsg.getImportance());
        msg.setLastEditedDateTime(defaultMsg.getLastEditedDateTime());
        msg.setLastModifiedDateTime(defaultMsg.getLastModifiedDateTime());
        msg.setLocale(defaultMsg.getLocale());
        msg.setMentions(new ArrayList<>());
        msgList.stream().forEach(m -> msg.getMentions().addAll(m.getMentions()));
        msg.setMessageType(defaultMsg.getMessageType());
        msg.setPolicyViolation(defaultMsg.getPolicyViolation());
        msg.setReactions(new ArrayList<>());
        msgList.stream().forEach(m -> msg.getReactions().addAll(m.getReactions()));
        msg.setReplyToId(defaultMsg.getReplyToId());
        msg.setSubject(defaultMsg.getSubject());
        msg.setSummary(defaultMsg.getSummary());
        msg.setWebUrl("https://teams.microsoft.com/_#/conversations/" + defaultMsg.getChatId() + "?ctx=chat");
        msg.setHostedContents(defaultMsg.getHostedContents());
        msg.setReplies(defaultMsg.getReplies());
        return msg;
    }

    /**
     * Processes team messages.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param configMap The configuration map.
     * @param client The Microsoft365Client.
     */
    protected void processTeamMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final Microsoft365Client client) {
        final String teamId = (String) configMap.get(TEAM_ID);

        if (StringUtil.isNotBlank(teamId)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing messages for specific team: {}", teamId);
            }

            final Group g = client.getGroupById(teamId);
            if (g == null) {
                throw new DataStoreException("Could not find a team: " + teamId);
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Found team: {} (Display Name: {})", g.getId(), g.getDisplayName());
            }

            final String channelId = (String) configMap.get(CHANNEL_ID);
            if (StringUtil.isNotBlank(channelId)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Processing messages for specific channel: {} in team: {}", channelId, teamId);
                }

                final Channel c = client.getChannelById(teamId, channelId);
                if (c == null) {
                    throw new DataStoreException("Could not find a channel: " + channelId);
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Found channel: {} (Display Name: {}) in team: {}", c.getId(), c.getDisplayName(), g.getDisplayName());
                }

                client.getTeamMessages(Collections.emptyList(), m -> {
                    final Map<String, Object> message = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                            defaultDataMap, getGroupRoles(client, g.getId(), c.getId()), m, map -> {
                                map.put(TEAM, g);
                                map.put(CHANNEL, c);
                            }, client);
                    if (message != null && !((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
                        client.getTeamReplyMessages(Collections.emptyList(), r -> {
                            processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                    getGroupRoles(client, g.getId(), c.getId()), r, map -> {
                                        map.put(TEAM, g);
                                        map.put(CHANNEL, c);
                                        map.put(PARENT, message);
                                    }, client);
                        }, teamId, channelId, (String) message.get(MESSAGE_ID));
                    }
                }, teamId, channelId);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Processing messages for all channels in team: {}", teamId);
                }

                client.getChannels(Collections.emptyList(), c -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing channel: {} (Display Name: {}) in team: {}", c.getId(), c.getDisplayName(),
                                g.getDisplayName());
                    }
                    client.getTeamMessages(Collections.emptyList(), m -> {
                        final Map<String, Object> message = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                                defaultDataMap, getGroupRoles(client, g.getId(), c.getId()), m, map -> {
                                    map.put(TEAM, g);
                                    map.put(CHANNEL, c);
                                }, client);
                        if (message != null && !((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
                            client.getTeamReplyMessages(Collections.emptyList(), r -> {
                                processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                        getGroupRoles(client, g.getId(), c.getId()), r, map -> {
                                            map.put(TEAM, g);
                                            map.put(CHANNEL, c);
                                            map.put(PARENT, message);
                                        }, client);
                            }, teamId, c.getId(), (String) message.get(MESSAGE_ID));
                        }
                    }, teamId, c.getId());
                }, teamId);
            }
        } else if (teamId == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing messages for all teams with visibility and exclusion filters");
            }

            final Set<String> excludeGroupIdSet = getExcludeGroupIdSet(configMap, client);
            if (logger.isDebugEnabled()) {
                logger.debug("Exclude Group IDs: {}", excludeGroupIdSet);
            }

            client.geTeams(Collections.emptyList(), g -> {

                if (logger.isDebugEnabled()) {
                    logger.debug("Evaluating team: {} (Display Name: {}, Visibility: {})", g.getId(), g.getDisplayName(),
                            g.getVisibility());
                }

                if (excludeGroupIdSet.contains(g.getId())) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Skipping excluded team: {} (Display Name: {})", g.getId(), g.getDisplayName());
                    }
                    return;
                }
                if (!isTargetVisibility(configMap, g.getVisibility())) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Skipping team due to visibility filter: {} (Display Name: {}, Visibility: {})", g.getId(),
                                g.getDisplayName(), g.getVisibility());
                    }
                    return;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Processing team: {} (Display Name: {})", g.getId(), g.getDisplayName());
                }

                client.getChannels(Collections.emptyList(), c -> {

                    if (logger.isDebugEnabled()) {
                        logger.debug("Processing channel: {} (Display Name: {}) in team: {}", c.getId(), c.getDisplayName(),
                                g.getDisplayName());
                    }

                    client.getTeamMessages(Collections.emptyList(), m -> {
                        final Map<String, Object> message = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                                defaultDataMap, getGroupRoles(client, g.getId(), c.getId()), m, map -> {
                                    map.put(TEAM, g);
                                    map.put(CHANNEL, c);
                                }, client);
                        if (message != null && !((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
                            client.getTeamReplyMessages(Collections.emptyList(), r -> {
                                processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                        getGroupRoles(client, g.getId(), c.getId()), r, map -> {
                                            map.put(TEAM, g);
                                            map.put(CHANNEL, c);
                                            map.put(PARENT, message);
                                        }, client);
                            }, g.getId(), c.getId(), (String) message.get(MESSAGE_ID));
                        }
                    }, g.getId(), c.getId());
                }, g.getId());
            });
        }
    }

    /**
     * Gets the set of excluded group IDs based on configured exclude team IDs.
     *
     * @param configMap The configuration map containing exclude team ID settings.
     * @param client The Microsoft365Client for group lookups.
     * @return A set of group IDs to exclude from processing.
     */
    protected Set<String> getExcludeGroupIdSet(final Map<String, Object> configMap, final Microsoft365Client client) {
        final String[] teamIds = (String[]) configMap.get(EXCLUDE_TEAM_ID);
        return StreamUtil.stream(teamIds).get(stream -> stream.map(teamId -> {
            final Group g = client.getGroupById(teamId);
            if (g == null) {
                throw new DataStoreException("Could not find a team: " + teamId);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Team -> Group: {} -> {}", teamId, g.getId());
            }
            return g.getId();
        }).collect(Collectors.toSet()));
    }

    /**
     * Determines if a team visibility level is included in the target visibility settings.
     *
     * @param configMap The configuration map containing visibility settings.
     * @param visibility The visibility level to check.
     * @return true if the visibility should be processed, false otherwise.
     */
    protected boolean isTargetVisibility(final Map<String, Object> configMap, final String visibility) {
        final String[] visibilities = (String[]) configMap.get(INCLUDE_VISIBILITY);
        if (visibilities.length == 0) {
            return true;
        }
        for (final String value : visibilities) {
            if (value.equalsIgnoreCase(visibility)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the date formatter for message titles.
     *
     * @param paramMap The data store parameters containing date format settings.
     * @return The configured DateTimeFormatter for titles.
     */
    protected DateTimeFormatter getTitleDateformat(final DataStoreParams paramMap) {
        return DateTimeFormatter.ofPattern(paramMap.getAsString(TITLE_DATEFORMAT, "yyyy/MM/dd'T'HH:mm:ss"));
    }

    /**
     * Gets the timezone offset for message titles.
     *
     * @param paramMap The data store parameters containing timezone settings.
     * @return The configured ZoneOffset for titles.
     */
    protected ZoneOffset getTitleTimezone(final DataStoreParams paramMap) {
        return ZoneOffset.of(paramMap.getAsString(TITLE_TIMEZONE, "Z"));
    }

    /**
     * Determines if system events should be ignored during processing.
     *
     * @param paramMap The data store parameters containing system event settings.
     * @return true if system events should be ignored, false otherwise.
     */
    protected Object isIgnoreSystemEvents(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_SYSTEM_EVENTS, Constants.TRUE));
    }

    /**
     * Determines if attachments should be appended to message content.
     *
     * @param paramMap The data store parameters containing attachment settings.
     * @return true if attachments should be appended, false otherwise.
     */
    protected Object isAppendAttachment(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(APPEND_ATTACHMENT, Constants.TRUE));
    }

    /**
     * Determines if reply messages should be ignored during processing.
     *
     * @param paramMap The data store parameters containing reply settings.
     * @return true if replies should be ignored, false otherwise.
     */
    protected boolean isIgnoreReplies(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_REPLIES, Constants.FALSE));
    }

    /**
     * Gets the configured team ID for processing a specific team.
     *
     * @param paramMap The data store parameters containing team ID setting.
     * @return The team ID to process, or null if not specified.
     */
    protected String getTeamId(final DataStoreParams paramMap) {
        return paramMap.getAsString(TEAM_ID);
    }

    /**
     * Gets the array of team IDs to exclude from processing.
     *
     * @param paramMap The data store parameters containing exclude team ID settings.
     * @return An array of team IDs to exclude.
     */
    protected String[] getExcludeTeamIds(final DataStoreParams paramMap) {
        final String idStr = paramMap.getAsString(EXCLUDE_TEAM_ID);
        if (StringUtil.isBlank(idStr)) {
            return new String[0];
        }
        return StreamUtil.split(idStr, ",")
                .get(stream -> stream.map(s -> s.trim()).filter(StringUtil::isNotBlank).toArray(n -> new String[n]));
    }

    /**
     * Gets the array of team visibility levels to include in processing.
     *
     * @param paramMap The data store parameters containing visibility settings.
     * @return An array of visibility levels to include.
     */
    protected String[] getIncludeVisibilities(final DataStoreParams paramMap) {
        final String idStr = paramMap.getAsString(INCLUDE_VISIBILITY);
        if (StringUtil.isBlank(idStr)) {
            return new String[0];
        }
        return StreamUtil.split(idStr, ",")
                .get(stream -> stream.map(s -> s.trim()).filter(StringUtil::isNotBlank).toArray(n -> new String[n]));
    }

    /**
     * Gets the configured channel ID for processing a specific channel.
     *
     * @param paramMap The data store parameters containing channel ID setting.
     * @return The channel ID to process, or null if not specified.
     */
    protected String getChannelId(final DataStoreParams paramMap) {
        return paramMap.getAsString(CHANNEL_ID);
    }

    /**
     * Gets the configured chat ID for processing a specific chat.
     *
     * @param paramMap The data store parameters containing chat ID setting.
     * @return The chat ID to process, or null if not specified.
     */
    protected String getChatId(final DataStoreParams paramMap) {
        return paramMap.getAsString(CHAT_ID);
    }

    /**
     * Gets the group roles for members of a specific team channel.
     *
     * @param client The Microsoft365Client for API communication.
     * @param teamId The team ID.
     * @param channelId The channel ID.
     * @return A list of group role permissions.
     */
    protected List<String> getGroupRoles(final Microsoft365Client client, final String teamId, final String channelId) {
        final List<String> permissions = new ArrayList<>();
        client.getChannelMembers(Collections.emptyList(), m -> getGroupRoles(client, permissions, m), teamId, channelId);
        return permissions;
    }

    /**
     * Gets the group roles for members of a specific chat.
     *
     * @param client The Microsoft365Client for API communication.
     * @param chatId The chat ID.
     * @return A list of group role permissions.
     */
    protected List<String> getGroupRoles(final Microsoft365Client client, final String chatId) {
        final List<String> permissions = new ArrayList<>();
        client.getChatMembers(Collections.emptyList(), m -> getGroupRoles(client, permissions, m), chatId);
        return permissions;
    }

    /**
     * Extracts and adds group roles from a conversation member to the permissions list.
     *
     * @param client The Microsoft365Client for API communication.
     * @param permissions The list to add permissions to.
     * @param m The conversation member to process.
     */
    protected void getGroupRoles(final Microsoft365Client client, final List<String> permissions, final ConversationMember m) {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        if (logger.isDebugEnabled()) {
            logger.debug("Member: {} : {}", m.getId(), ToStringBuilder.reflectionToString(m));
        } else {
            logger.info("Member: {} : {}", m.getId(), m.getDisplayName());
        }
        if (m instanceof AadUserConversationMember member) {
            final String id = member.getUserId();
            final String email = member.getEmail();
            if (StringUtil.isNotBlank(email)) {
                final List<String> idList = new ArrayList<>();
                if (StringUtil.isBlank(id)) {
                    Collections.addAll(idList, client.getGroupIdsByEmail(email));
                } else {
                    idList.add(id);
                }
                if (idList.isEmpty()) {
                    permissions.add(systemHelper.getSearchRoleByUser(email));
                    permissions.add(systemHelper.getSearchRoleByGroup(email));
                } else {
                    idList.stream().forEach(i -> {
                        final UserType userType = client.getUserType(i);
                        switch (userType) {
                        case USER:
                            permissions.add(systemHelper.getSearchRoleByUser(email));
                            permissions.add(systemHelper.getSearchRoleByUser(i));
                            break;
                        case GROUP:
                            permissions.add(systemHelper.getSearchRoleByGroup(email));
                            permissions.add(systemHelper.getSearchRoleByGroup(i));
                            break;
                        default:
                            permissions.add(systemHelper.getSearchRoleByUser(email));
                            permissions.add(systemHelper.getSearchRoleByGroup(email));
                            permissions.add(systemHelper.getSearchRoleByUser(i));
                            permissions.add(systemHelper.getSearchRoleByGroup(i));
                            break;
                        }
                    });
                }
            } else if (StringUtil.isNotBlank(id)) {
                final UserType userType = client.getUserType(id);
                switch (userType) {
                case USER:
                    permissions.add(systemHelper.getSearchRoleByUser(id));
                    break;
                case GROUP:
                    permissions.add(systemHelper.getSearchRoleByGroup(id));
                    break;
                default:
                    permissions.add(systemHelper.getSearchRoleByUser(id));
                    permissions.add(systemHelper.getSearchRoleByGroup(id));
                    break;
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("No identity for permission.");
            }
        }
    }

    /**
     * Determines if a chat message is a system event that should be filtered.
     *
     * @param configMap The configuration map containing system event settings.
     * @param message The chat message to check.
     * @return true if the message is a system event and should be ignored, false otherwise.
     */
    protected boolean isSystemEvent(final Map<String, Object> configMap, final ChatMessage message) {
        if (((Boolean) configMap.get(IGNORE_SYSTEM_EVENTS)).booleanValue()) {
            if (message.getBody() != null && "<systemEventMessage/>".equals(message.getBody().getContent())) {
                return true;
            }

            return false;
        }
        return false;
    }

    /**
     * Processes a chat message for indexing, extracting content and metadata.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map for field mappings.
     * @param defaultDataMap The default data map.
     * @param permissions The list of permissions for the message.
     * @param message The chat message to process.
     * @param resultAppender Consumer to append additional result data.
     * @param client The Microsoft365Client for API communication.
     * @return A map containing the processed message data, or null if the message was filtered.
     */
    protected Map<String, Object> processChatMessage(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final List<String> permissions, final ChatMessage message,
            final Consumer<Map<String, Object>> resultAppender, final Microsoft365Client client) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();

        if (logger.isDebugEnabled()) {
            logger.debug("Processing chat message - ID: {}, WebUrl: {}, From: {}, Created: {}", message.getId(), message.getWebUrl(),
                    message.getFrom() != null ? message.getFrom().getUser() : "unknown", message.getCreatedDateTime());
        }

        if (isSystemEvent(configMap, message)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping system event message: {} (ID: {})", message.getWebUrl(), message.getId());
            }
            return null;
        }

        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
        final Map<String, Object> messageMap = new HashMap<>();
        final StatsKeyObject statsKey = new StatsKeyObject(message.getWebUrl());
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);

        try {
            crawlerStatsHelper.begin(statsKey);

            if (logger.isDebugEnabled()) {
                logger.debug("Extracting content for message: {} (ID: {})", message.getWebUrl(), message.getId());
            }

            final String content = getContent(configMap, message, client);
            final String title = getTitle(configMap, message);

            messageMap.put(MESSAGE_CONTENT, content);
            messageMap.put(MESSAGE_TITLE, title);

            messageMap.put(MESSAGE_ATTACHMENTS, message.getAttachments());
            messageMap.put(MESSAGE_BODY, message.getBody());
            messageMap.put(MESSAGE_CHANNEL_IDENTITY, message.getChannelIdentity());
            messageMap.put(MESSAGE_CHAT_ID, message.getChatId());
            messageMap.put(MESSAGE_CREATED_DATE_TIME, message.getCreatedDateTime());
            messageMap.put(MESSAGE_DELETED_DATE_TIME, message.getDeletedDateTime());
            messageMap.put(MESSAGE_ETAG, message.getEtag());
            messageMap.put(MESSAGE_FROM, message.getFrom());
            messageMap.put(MESSAGE_HOSTED_CONTENTS, message.getHostedContents());
            messageMap.put(MESSAGE_ID, message.getId());
            messageMap.put(MESSAGE_IMPORTANCE, message.getImportance());
            messageMap.put(MESSAGE_LAST_EDITED_DATE_TIME, message.getLastEditedDateTime());
            messageMap.put(MESSAGE_LAST_MODIFIED_DATE_TIME, message.getLastModifiedDateTime());
            messageMap.put(MESSAGE_LOCALE, message.getLocale());
            messageMap.put(MESSAGE_MENTIONS, message.getMentions());
            messageMap.put(MESSAGE_REPLIES, message.getReplies());
            messageMap.put(MESSAGE_REPLY_TO_ID, message.getReplyToId());
            messageMap.put(MESSAGE_SUBJECT, message.getSubject());
            messageMap.put(MESSAGE_SUMMARY, message.getSummary());
            messageMap.put(MESSAGE_WEB_URL, message.getWebUrl());

            resultMap.put(MESSAGE, messageMap);
            resultAppender.accept(resultMap);

            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            messageMap.put(MESSAGE_ROLES, permissions.stream().distinct().collect(Collectors.toList()));

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("Prepared message data - Title: {}, Content size: {}, Permissions: {}, Attachments: {}", title,
                        content != null ? content.length() : 0, permissions.size(),
                        message.getAttachments() != null ? message.getAttachments().size() : 0);
            }

            final String scriptType = getScriptType(paramMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

            if (logger.isDebugEnabled()) {
                logger.debug("Final data map prepared for indexing - Fields: {}, URL: {}", dataMap.size(), dataMap.get("url"));
            }

            if (dataMap.get("url") instanceof final String statsUrl) {
                statsKey.setUrl(statsUrl);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);

            if (logger.isDebugEnabled()) {
                logger.debug("Successfully indexed chat message: {} (ID: {})", message.getWebUrl(), message.getId());
            }
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception for message: {} (ID: {}) - Data: {}", message.getWebUrl(), message.getId(), dataMap, e);

            Throwable target = e;
            if (target instanceof final MultipleCrawlingAccessException ex) {
                final Throwable[] causes = ex.getCauses();
                if (causes.length > 0) {
                    target = causes[causes.length - 1];
                }
            }

            String errorName;
            final Throwable cause = target.getCause();
            if (cause != null) {
                errorName = cause.getClass().getCanonicalName();
            } else {
                errorName = target.getClass().getCanonicalName();
            }

            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, message.getWebUrl(), target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Processing exception for message: {} (ID: {}) - Data: {}", message.getWebUrl(), message.getId(), dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), message.getWebUrl(), t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }

        return messageMap;
    }

    /**
     * Generates a title for the chat message based on sender and timestamp.
     *
     * @param configMap The configuration map containing title formatting settings.
     * @param message The chat message.
     * @return The generated title string.
     */
    protected String getTitle(final Map<String, Object> configMap, final ChatMessage message) {
        final StringBuilder titleBuf = new StringBuilder(100);
        if (message.getFrom() != null) {
            final ChatMessageFromIdentitySet identity = message.getFrom();
            if (identity.getUser() != null) {
                titleBuf.append(identity.getUser().getDisplayName());
            } else if (identity.getApplication() != null) {
                titleBuf.append(identity.getApplication().getDisplayName());
            } else if (identity.getDevice() != null) {
                titleBuf.append(identity.getDevice().getDisplayName());
            }
        } else {
            titleBuf.append("unknown");
        }

        if (message.getCreatedDateTime() != null) {
            titleBuf.append(' ');
            final DateTimeFormatter fmt = (DateTimeFormatter) configMap.get(TITLE_DATEFORMAT);
            final ZoneOffset zone = (ZoneOffset) configMap.get(TITLE_TIMEZONE);
            titleBuf.append(fmt.format(message.getCreatedDateTime().withOffsetSameInstant(zone)));
        }

        return titleBuf.toString();
    }

    /**
     * Extracts and formats the content from a chat message, including attachments if configured.
     *
     * @param configMap The configuration map containing content extraction settings.
     * @param message The chat message.
     * @param client The Microsoft365Client for API communication.
     * @return The formatted message content.
     */
    protected String getContent(final Map<String, Object> configMap, final ChatMessage message, final Microsoft365Client client) {
        final StringBuilder bodyBuf = new StringBuilder(1000);
        if (message.getBody() != null) {
            switch (message.getBody().getContentType()) {
            case Html:
                bodyBuf.append(stripHtmlTags(message.getBody().getContent()));
                break;
            case Text:
                bodyBuf.append(normalizeTextContent(message.getBody().getContent()));
                break;
            default:
                bodyBuf.append(message.getBody().getContent());
                break;
            }
        }
        if (((Boolean) configMap.get(APPEND_ATTACHMENT)).booleanValue() && message.getAttachments() != null) {
            message.getAttachments().forEach(a -> {
                if (StringUtil.isNotBlank(a.getName())) {
                    bodyBuf.append('\n').append(a.getName());
                }
                if (a.getContent() != null) {
                    bodyBuf.append('\n').append(a.getContent());
                } else {
                    bodyBuf.append('\n').append(client.getAttachmentContent(a));
                }
            });
        }
        return bodyBuf.toString();
    }

    /**
     * Normalizes text content by removing attachment tags and extra whitespace.
     *
     * @param content The raw text content.
     * @return The normalized text content.
     */
    protected String normalizeTextContent(final String content) {
        if (StringUtil.isBlank(content)) {
            return StringUtil.EMPTY;
        }
        return content.replaceAll("<attachment[^>]*></attachment>", StringUtil.EMPTY).trim();
    }

    /**
     * Strips HTML tags from the given value using Lucene's HTML strip filter.
     *
     * @param value The HTML content to strip tags from.
     * @return The text content with HTML tags removed.
     */
    protected String stripHtmlTags(final String value) {
        if (value == null) {
            return "";
        }

        if (!value.contains("<") || !value.contains(">")) {
            return value;
        }

        final StringBuilder builder = new StringBuilder();
        try (HTMLStripCharFilter filter = new HTMLStripCharFilter(new StringReader(value))) {
            int ch;
            while ((ch = filter.read()) != -1) {
                builder.append((char) ch);
            }
        } catch (final IOException e) {
            throw new FessSystemException(e);
        }

        return builder.toString();
    }
}

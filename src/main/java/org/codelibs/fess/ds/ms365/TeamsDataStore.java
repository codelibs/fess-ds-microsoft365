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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client;
import org.codelibs.fess.ds.ms365.client.Microsoft365Client.UserType;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.exception.FessSystemException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
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
    /** Parameter name for the inclusive lower bound on a message's timestamp. */
    private static final String START_DATE = "start_date";
    /** Parameter name for the inclusive upper bound on a message's timestamp. */
    private static final String END_DATE = "end_date";

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
        configMap.put(IGNORE_ERROR, isIgnoreError(paramMap));
        // Parsed exactly here, once per crawl, so a malformed bound produces exactly one warning
        // rather than one per message.
        configMap.put(START_DATE, getStartDate(paramMap));
        configMap.put(END_DATE, getEndDate(paramMap));

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Teams crawling started - Configuration: TeamID={}, ChannelID={}, ChatID={}, IgnoreReplies={}, AppendAttachment={}, IgnoreError={}, StartDate={}, EndDate={}, Threads={}",
                    configMap.get(TEAM_ID), configMap.get(CHANNEL_ID), configMap.get(CHAT_ID), configMap.get(IGNORE_REPLIES),
                    configMap.get(APPEND_ATTACHMENT), configMap.get(IGNORE_ERROR), configMap.get(START_DATE), configMap.get(END_DATE),
                    paramMap.getAsString(NUMBER_OF_THREADS, "1"));
        }

        final ReportingExecutor executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Microsoft365Client client = createClient(paramMap)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Starting Teams messages processing");
            }
            processTeamMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, executorService, client);

            if (logger.isDebugEnabled()) {
                logger.debug("Starting Chat messages processing");
            }
            processChatMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, executorService, client);

            if (logger.isDebugEnabled()) {
                logger.debug("Teams crawling completed - shutting down thread executor");
            }
            shutdownExecutor(executorService, paramMap);
        } finally {
            executorService.shutdownNow();
        }
    }

    /**
     * Processes chat messages.
     *
     * <p>A chat is consolidated into a single document, so {@code start_date}/{@code end_date} are
     * evaluated once for the whole conversation by {@link #isTargetChatDate}: the chat is kept when
     * <em>any</em> of its messages falls inside the range. The consolidated document's own
     * timestamp cannot be used for that decision -- {@link #createChatMessage} inherits it from
     * {@code msgList.get(0)}, and {@link Microsoft365Client#getChatMessages} sets no
     * {@code $orderby} and does not sort, so which message that is depends entirely on Graph's
     * default ordering. Deciding across the whole list needs no assumption about that ordering and
     * cannot drop a conversation the operator asked for.</p>
     *
     * <p>The consolidated document is therefore processed with the bounds removed from its
     * configuration: the range decision has already been made, with the whole conversation in view,
     * and re-applying it to a single synthetic timestamp would only undo it.</p>
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param configMap The configuration map.
     * @param executorService The executor service handling concurrent tasks.
     * @param client The Microsoft365Client.
     */
    protected void processChatMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final ExecutorService executorService, final Microsoft365Client client) {
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
                final List<ChatMessage> messagesSnapshot = Collections.unmodifiableList(new ArrayList<>(msgList));

                if (!isTargetChatDate(configMap, messagesSnapshot)) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Skipping chat outside the configured date range: {} ({} messages, none in range)", chatId,
                                messagesSnapshot.size());
                    }
                    return;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Creating consolidated chat message from {} individual messages for chat: {}", msgList.size(), chatId);
                }

                final ChatMessage consolidatedMessage = createChatMessage(messagesSnapshot, client);
                final List<String> chatRoles = getGroupRoles(client, chatId);
                final Map<String, Object> chatConfigMap = withoutDateRange(configMap);
                executorService.execute(() -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Submitting consolidated chat processing task for chat: {}", chatId);
                    }
                    processChatMessage(dataConfig, callback, chatConfigMap, paramMap, scriptMap, defaultDataMap, chatRoles,
                            consolidatedMessage, map -> map.put("messages", messagesSnapshot), client);
                });

                if (logger.isDebugEnabled()) {
                    logger.debug("Submitted consolidated chat processing task for chat: {} with {} individual messages", chatId,
                            msgList.size());
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("No messages found for chat: {}", chatId);
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("No specific chat ID configured - skipping chat message processing");
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
     * <p>{@code ignore_error} is honoured only on the paths that abort the crawl today: an
     * unresolvable {@code team_id}, an unresolvable {@code channel_id}, and a failure listing an
     * explicitly configured team's channels. The all-teams branch below already logs and continues,
     * so wiring the flag into it would have turned a tolerated failure into a crawl abort for every
     * configuration that leaves {@code ignore_error} at its default of {@code false}.</p>
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param configMap The configuration map.
     * @param executorService The executor service handling concurrent tasks.
     * @param client The Microsoft365Client.
     */
    protected void processTeamMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final ExecutorService executorService, final Microsoft365Client client) {
        final String teamId = (String) configMap.get(TEAM_ID);

        if (StringUtil.isNotBlank(teamId)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing messages for specific team: {}", teamId);
            }

            final Group g = client.getGroupById(teamId);
            if (g == null) {
                if (!Boolean.TRUE.equals(configMap.get(IGNORE_ERROR))) {
                    throw new DataStoreException("Could not find a team: " + teamId);
                }
                logger.warn("Could not find a team: {}. Skipping it because {} is enabled.", teamId, IGNORE_ERROR);
                return;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Found team: {} (Display Name: {})", g.getId(), g.getDisplayName());
            }

            if (!isTargetVisibility(configMap, g.getVisibility())) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Skipping team due to visibility filter: {} (Display Name: {}, Visibility: {})", g.getId(),
                            g.getDisplayName(), g.getVisibility());
                }
                return;
            }

            final String channelId = (String) configMap.get(CHANNEL_ID);
            if (StringUtil.isNotBlank(channelId)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Processing messages for specific channel: {} in team: {}", channelId, teamId);
                }

                final Channel c = client.getChannelById(teamId, channelId);
                if (c == null) {
                    if (!Boolean.TRUE.equals(configMap.get(IGNORE_ERROR))) {
                        throw new DataStoreException("Could not find a channel: " + channelId);
                    }
                    logger.warn("Could not find a channel: {} in team: {}. Skipping it because {} is enabled.", channelId, teamId,
                            IGNORE_ERROR);
                    return;
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Found channel: {} (Display Name: {}) in team: {}", c.getId(), c.getDisplayName(), g.getDisplayName());
                }

                submitChannelMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, executorService, client, g, c);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Processing messages for all channels in team: {}", teamId);
                }

                try {
                    client.getChannels(Collections.emptyList(), c -> submitChannelMessages(dataConfig, callback, paramMap, scriptMap,
                            defaultDataMap, configMap, executorService, client, g, c), teamId);
                } catch (final Exception e) {
                    if (!Boolean.TRUE.equals(configMap.get(IGNORE_ERROR))) {
                        throw new DataStoreException("Failed to access channels for team: " + teamId + " (Display Name: "
                                + g.getDisplayName() + "). Team may be archived or inaccessible.", e);
                    }
                    logger.warn("Failed to access channels for team: {} (Display Name: {}). Skipping it because {} is enabled.", teamId,
                            g.getDisplayName(), IGNORE_ERROR, e);
                }
            }
        } else if (teamId == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Processing messages for all teams with visibility and exclusion filters");
            }

            final Set<String> excludeGroupIdSet = getExcludeGroupIdSet(configMap, client);
            if (logger.isDebugEnabled()) {
                logger.debug("Exclude Group IDs: {}", excludeGroupIdSet);
            }

            client.getTeams(Collections.emptyList(), g -> {

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

                try {
                    client.getChannels(Collections.emptyList(), c -> submitChannelMessages(dataConfig, callback, paramMap, scriptMap,
                            defaultDataMap, configMap, executorService, client, g, c), g.getId());
                } catch (final Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to access channels for team: {} (Display Name: {}). Team may be archived or inaccessible.",
                                g.getId(), g.getDisplayName(), e);
                    } else {
                        logger.warn("Failed to access channels for team: {} (Display Name: {}). Skipping this team. {}", g.getId(),
                                g.getDisplayName(), e.getMessage());
                    }
                }
            });
        }
    }

    /**
     * Submits channel processing to the executor for asynchronous execution.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param configMap The configuration map.
     * @param executorService The executor service.
     * @param client The Microsoft365Client.
     * @param group The Microsoft 365 group (team).
     * @param channel The Teams channel.
     */
    protected void submitChannelMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final ExecutorService executorService, final Microsoft365Client client, final Group group, final Channel channel) {
        executorService.execute(
                () -> processChannelMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, client, group, channel));
    }

    /**
     * Processes all messages for a given channel including replies when enabled.
     *
     * <p>Replies are fetched only for a root message that {@code processChatMessage} actually
     * indexed. With {@code start_date}/{@code end_date} set, a root outside the window therefore
     * also excludes its replies. That is deliberate: it keeps a reply from ever being indexed with
     * a {@code parent} that was never processed, and it is the one place the range saves Graph
     * traffic -- the reply listing for an out-of-range root is never issued. A reply is always at
     * or after its root, so an {@code end_date} that excludes a root correctly excludes its
     * replies; only {@code start_date} can drop an in-window reply, and only of a conversation
     * whose opening message the operator asked not to index.</p>
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param configMap The configuration map.
     * @param client The Microsoft365Client.
     * @param group The Microsoft 365 group (team).
     * @param channel The Teams channel to process.
     */
    protected void processChannelMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final Microsoft365Client client, final Group group, final Channel channel) {
        final boolean ignoreReplies = ((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue();

        if (logger.isDebugEnabled()) {
            logger.debug("Submitting processing for channel: {} (Display Name: {}) in team: {}", channel.getId(), channel.getDisplayName(),
                    group.getDisplayName());
        }

        // One channel has one membership. Resolving it inside the per-message lambda issued the
        // same paged GET /teams/{id}/channels/{id}/members once per message and once per reply;
        // resolving it before the listing instead made a channel that yields no messages pay for --
        // and newly able to fail on -- a membership it would never read. The holder keeps it at once
        // per channel while deferring it to the first message that actually needs it.
        final AtomicReference<List<String>> channelRolesHolder = new AtomicReference<>();
        try {
            client.getTeamMessages(Collections.emptyList(), message -> {
                final List<String> channelRoles = resolveChannelRoles(channelRolesHolder, client, group, channel);
                final Map<String, Object> processedMessage = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                        defaultDataMap, channelRoles, message, map -> {
                            map.put(TEAM, group);
                            map.put(CHANNEL, channel);
                        }, client);
                if (processedMessage != null && !ignoreReplies) {
                    client.getTeamReplyMessages(Collections.emptyList(), reply -> {
                        processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, channelRoles, reply,
                                map -> {
                                    map.put(TEAM, group);
                                    map.put(CHANNEL, channel);
                                    map.put(PARENT, processedMessage);
                                }, client);
                    }, group.getId(), channel.getId(), (String) processedMessage.get(MESSAGE_ID));
                }
            }, group.getId(), channel.getId());
        } catch (final Exception e) {
            logger.warn("Failed to process channel: {} (Display Name: {}) in team: {}", channel.getId(), channel.getDisplayName(),
                    group.getDisplayName(), e);
            if (!Boolean.TRUE.equals(configMap.get(IGNORE_ERROR))) {
                throw new DataStoreException("Failed to process channel: " + channel.getId(), e);
            }
        }
    }

    /**
     * Returns the channel's search roles, resolving them the first time a message needs them and
     * reusing that result for every later message and reply in the same channel.
     *
     * <p>No synchronisation: {@code holder} is confined to one thread. {@code processChannelMessages}
     * <em>is</em> the pool task body -- {@code submitChannelMessages} passes a call to it straight to
     * {@code executorService.execute} -- and both Graph consumers below run inline on that same
     * thread, so the slot is never touched concurrently.
     *
     * @param holder The single-slot cache for this channel, confined to the calling thread.
     * @param client The Microsoft365Client.
     * @param group The Microsoft 365 group (team).
     * @param channel The Teams channel.
     * @return The channel's roles.
     */
    protected List<String> resolveChannelRoles(final AtomicReference<List<String>> holder, final Microsoft365Client client,
            final Group group, final Channel channel) {
        final List<String> cached = holder.get();
        if (cached != null) {
            return cached;
        }
        final List<String> resolved = getGroupRoles(client, group.getId(), channel.getId());
        holder.set(resolved);
        return resolved;
    }

    /**
     * Gets the set of excluded group IDs based on configured exclude team IDs.
     *
     * <p>Deliberately not gated by {@code ignore_error}: this lookup resolves
     * {@code exclude_team_ids}, so skipping a failure here would leave a team the operator asked to
     * exclude out of the exclusion set and crawl it. {@code ignore_error} may make the crawl more
     * forgiving, never wider.</p>
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
     * Gets the inclusive lower bound on a message's timestamp.
     *
     * @param paramMap The data store parameters.
     * @return the lower bound, or null when unset or unparseable (i.e. no lower bound).
     */
    protected OffsetDateTime getStartDate(final DataStoreParams paramMap) {
        return parseDateBound(paramMap.getAsString(START_DATE), START_DATE, false);
    }

    /**
     * Gets the inclusive upper bound on a message's timestamp.
     *
     * @param paramMap The data store parameters.
     * @return the upper bound, or null when unset or unparseable (i.e. no upper bound).
     */
    protected OffsetDateTime getEndDate(final DataStoreParams paramMap) {
        return parseDateBound(paramMap.getAsString(END_DATE), END_DATE, true);
    }

    /**
     * Parses a date-range bound.
     *
     * <p>Accepts a full ISO-8601 offset date-time (used verbatim) or a date-only ISO-8601 value,
     * which is interpreted in UTC: the start of that day for a lower bound, the last instant of
     * that day for an upper bound. An unparseable value is logged once and treated as absent, so
     * a typo never aborts a crawl -- and, just as importantly, never narrows one: the operator
     * gets the unfiltered crawl they had before the parameter existed, not an empty index. This is
     * the same warn-and-fall-back treatment a malformed {@code include_pattern} and a malformed
     * {@code max_content_length} already get in this plugin.</p>
     *
     * @param value The raw parameter value, may be null or blank.
     * @param key The parameter name, for the warning message.
     * @param endOfDay Whether a date-only value means the end of that day rather than its start.
     * @return the parsed bound, or null.
     */
    protected OffsetDateTime parseDateBound(final String value, final String key, final boolean endOfDay) {
        if (StringUtil.isBlank(value)) {
            return null;
        }
        final String trimmed = value.trim();
        try {
            return OffsetDateTime.parse(trimmed, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (final DateTimeParseException e) {
            // Not an offset date-time; try the date-only form below.
        }
        try {
            final LocalDate date = LocalDate.parse(trimmed, DateTimeFormatter.ISO_LOCAL_DATE);
            return (endOfDay ? date.atTime(LocalTime.MAX) : date.atStartOfDay()).atOffset(ZoneOffset.UTC);
        } catch (final DateTimeParseException e) {
            logger.warn("Ignoring {}={}: expected an ISO-8601 date (yyyy-MM-dd) or offset date-time (yyyy-MM-dd'T'HH:mm:ssXXX).", key,
                    trimmed);
            return null;
        }
    }

    /**
     * Checks whether a message's timestamp falls inside the configured date range.
     *
     * <p>Both bounds are inclusive. {@code createdDateTime} is compared, falling back to
     * {@code lastModifiedDateTime} when it is null; a message with neither is kept, so a missing
     * timestamp never silently removes content from the index. A timestamp the Graph SDK cannot
     * parse never reaches this method at all -- it throws inside the client's deserializer, where
     * {@code ignore_error} decides whether the channel is skipped or the crawl aborts.</p>
     *
     * @param configMap The configuration map holding the parsed bounds.
     * @param message The chat message.
     * @return true if the message should be indexed, false otherwise.
     */
    protected boolean isTargetMessageDate(final Map<String, Object> configMap, final ChatMessage message) {
        final OffsetDateTime startDate = (OffsetDateTime) configMap.get(START_DATE);
        final OffsetDateTime endDate = (OffsetDateTime) configMap.get(END_DATE);
        if (startDate == null && endDate == null) {
            return true;
        }
        OffsetDateTime timestamp = message.getCreatedDateTime();
        if (timestamp == null) {
            timestamp = message.getLastModifiedDateTime();
        }
        if (timestamp == null) {
            return true;
        }
        if (startDate != null && timestamp.isBefore(startDate)) {
            return false;
        }
        return endDate == null || !timestamp.isAfter(endDate);
    }

    /**
     * Checks whether a chat, which is indexed as one consolidated document, falls inside the
     * configured date range.
     *
     * <p>The chat is kept when <em>any</em> of its messages is in range. Judging the chat by the
     * consolidated document's own timestamp would judge it by whichever message
     * {@link Microsoft365Client#getChatMessages} happened to yield first -- that call sets no
     * {@code $orderby} and does no client-side sort, so the order is Graph's default and not
     * something this code controls. A chat spanning years would then be dropped whole on the
     * strength of one message's timestamp, taking every in-range message with it.</p>
     *
     * <p>The consequence of the all-or-nothing shape is that the indexed body is still the whole
     * conversation, including its out-of-range messages: consolidating a chat into one document
     * leaves no way to index part of it.</p>
     *
     * @param configMap The configuration map holding the parsed bounds.
     * @param messages The chat's messages.
     * @return true if the chat should be indexed, false otherwise.
     */
    protected boolean isTargetChatDate(final Map<String, Object> configMap, final List<ChatMessage> messages) {
        if (configMap.get(START_DATE) == null && configMap.get(END_DATE) == null) {
            return true;
        }
        return messages.stream().anyMatch(message -> isTargetMessageDate(configMap, message));
    }

    /**
     * Returns a copy of the configuration with both date bounds cleared.
     *
     * <p>Used for the consolidated chat document, whose range decision {@link #isTargetChatDate}
     * has already made across the whole conversation. Leaving the bounds in place would let the
     * per-message guard in {@link #processChatMessage} re-decide it from the single synthetic
     * timestamp the consolidated document carries, and overturn it.</p>
     *
     * @param configMap The configuration map.
     * @return a new map with the same entries except the two date bounds.
     */
    protected Map<String, Object> withoutDateRange(final Map<String, Object> configMap) {
        final Map<String, Object> copy = new HashMap<>(configMap);
        copy.remove(START_DATE);
        copy.remove(END_DATE);
        return copy;
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
                .get(stream -> stream.map(String::trim).filter(StringUtil::isNotBlank).toArray(n -> new String[n]));
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
                .get(stream -> stream.map(String::trim).filter(StringUtil::isNotBlank).toArray(n -> new String[n]));
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
        if (m instanceof final AadUserConversationMember member) {
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
        if ((Boolean) configMap.get(IGNORE_SYSTEM_EVENTS) && message.getBody() != null
                && "<systemEventMessage/>".equals(message.getBody().getContent())) {
            return true;
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

        if (!isTargetMessageDate(configMap, message)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping message outside the configured date range: {} (ID: {}, Created: {})", message.getWebUrl(),
                        message.getId(), message.getCreatedDateTime());
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

            messageMap.put(MESSAGE_ROLES, buildMessageRoles(paramMap, defaultDataMap, permissions));

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
            handleCrawlingException(dataConfig, crawlerStatsHelper, statsKey, message.getWebUrl(), e);
        } catch (final Throwable t) {
            logger.warn("Processing exception for message: {} (ID: {}) - Data: {}", message.getWebUrl(), message.getId(), dataMap, t);
            handleCrawlingThrowable(dataConfig, crawlerStatsHelper, statsKey, message.getWebUrl(), t);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }

        return messageMap;
    }

    /**
     * Builds the role list for one indexed message: the roles its container contributed, plus the
     * configured {@code default_permissions}, plus the data config's own Permissions field carried
     * in {@code defaultDataMap}, de-duplicated.
     *
     * <p>The returned list is new. The caller's list is never modified -- a channel's membership is
     * resolved once and shared across every message in that channel, so appending to it here would
     * accumulate one copy of {@code default_permissions} per message.
     *
     * @param paramMap The data store parameters.
     * @param defaultDataMap The data store's default data map, holding the data config's Permissions field.
     * @param permissions The roles contributed by the message's channel or chat.
     * @return a new list of roles for this document.
     */
    protected List<String> buildMessageRoles(final DataStoreParams paramMap, final Map<String, Object> defaultDataMap,
            final List<String> permissions) {
        final List<String> roles = new ArrayList<>(permissions);
        roles.addAll(getDefaultPermissions(paramMap));
        return mergeDefaultRoles(roles, defaultDataMap).stream().distinct().collect(Collectors.toList());
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

/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2;

import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.bean.Message;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.utils.event.transport.EventUtilsKt;
import com.exactpro.th2.conn.dirty.fix.FixField;
import com.exactpro.th2.conn.dirty.fix.MessageLoader;
import com.exactpro.th2.conn.dirty.fix.MessageTransformer;
import com.exactpro.th2.conn.dirty.fix.PasswordManager;
import com.exactpro.th2.conn.dirty.fix.UtilKt;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BatchSendConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ChangeSequenceConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RecoveryConfig;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ResendRequestConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.SendSequenceResetConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.SplitSendConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.TransformMessageConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.TransformationConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.DefaultStrategyHolder;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.IncomingMessagesStrategy;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.OutgoingMessagesStrategy;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.ReceiveStrategy;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.RuleType;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.SchedulerType;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.SendStrategy;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.StatefulStrategy;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.StrategyScheduler;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.StrategyState;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api.CleanupHandler;
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api.OnCloseHandler;
import com.exactpro.th2.conn.dirty.tcp.core.SendingTimeoutHandler;
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel;
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.SendMode;
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandler;
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerContext;
import com.exactpro.th2.conn.dirty.tcp.core.util.CommonUtil;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import kotlin.Unit;
import kotlin.jvm.functions.Function1;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.exactpro.th2.common.event.EventUtils.createMessageBean;
import static com.exactpro.th2.conn.dirty.fix.FixByteBufUtilKt.findField;
import static com.exactpro.th2.conn.dirty.fix.FixByteBufUtilKt.findLastField;
import static com.exactpro.th2.conn.dirty.fix.FixByteBufUtilKt.firstField;
import static com.exactpro.th2.conn.dirty.fix.FixByteBufUtilKt.lastField;
import static com.exactpro.th2.conn.dirty.fix.FixByteBufUtilKt.updateChecksum;
import static com.exactpro.th2.conn.dirty.fix.FixByteBufUtilKt.updateLength;
import static com.exactpro.th2.conn.dirty.fix.KeyFileType.Companion.OperationMode.ENCRYPT_MODE;
import static com.exactpro.th2.conn.dirty.tcp.core.util.CommonUtil.getEventId;
import static com.exactpro.th2.conn.dirty.tcp.core.util.CommonUtil.toByteBuf;
import static com.exactpro.th2.conn.dirty.tcp.core.util.CommonUtil.toErrorEvent;
import static com.exactpro.th2.constants.Constants.ADMIN_MESSAGES;
import static com.exactpro.th2.constants.Constants.BEGIN_SEQ_NO;
import static com.exactpro.th2.constants.Constants.BEGIN_SEQ_NO_TAG;
import static com.exactpro.th2.constants.Constants.BEGIN_STRING_TAG;
import static com.exactpro.th2.constants.Constants.BODY_LENGTH;
import static com.exactpro.th2.constants.Constants.BODY_LENGTH_TAG;
import static com.exactpro.th2.constants.Constants.CHECKSUM;
import static com.exactpro.th2.constants.Constants.CHECKSUM_TAG;
import static com.exactpro.th2.constants.Constants.DEFAULT_APPL_VER_ID;
import static com.exactpro.th2.constants.Constants.DEFAULT_APPL_VER_ID_TAG;
import static com.exactpro.th2.constants.Constants.ENCRYPTED_PASSWORD;
import static com.exactpro.th2.constants.Constants.ENCRYPTED_PASSWORD_TAG;
import static com.exactpro.th2.constants.Constants.ENCRYPT_METHOD;
import static com.exactpro.th2.constants.Constants.END_SEQ_NO;
import static com.exactpro.th2.constants.Constants.END_SEQ_NO_TAG;
import static com.exactpro.th2.constants.Constants.GAP_FILL_FLAG;
import static com.exactpro.th2.constants.Constants.GAP_FILL_FLAG_TAG;
import static com.exactpro.th2.constants.Constants.HEART_BT_INT;
import static com.exactpro.th2.constants.Constants.IS_POSS_DUP;
import static com.exactpro.th2.constants.Constants.IS_SEQUENCE_RESET_FLAG;
import static com.exactpro.th2.constants.Constants.MSG_SEQ_NUM;
import static com.exactpro.th2.constants.Constants.MSG_SEQ_NUM_TAG;
import static com.exactpro.th2.constants.Constants.MSG_TYPE;
import static com.exactpro.th2.constants.Constants.MSG_TYPE_HEARTBEAT;
import static com.exactpro.th2.constants.Constants.MSG_TYPE_LOGON;
import static com.exactpro.th2.constants.Constants.MSG_TYPE_LOGOUT;
import static com.exactpro.th2.constants.Constants.MSG_TYPE_RESEND_REQUEST;
import static com.exactpro.th2.constants.Constants.MSG_TYPE_SEQUENCE_RESET;
import static com.exactpro.th2.constants.Constants.MSG_TYPE_TAG;
import static com.exactpro.th2.constants.Constants.MSG_TYPE_TEST_REQUEST;
import static com.exactpro.th2.constants.Constants.NEW_ENCRYPTED_PASSWORD;
import static com.exactpro.th2.constants.Constants.NEW_ENCRYPTED_PASSWORD_TAG;
import static com.exactpro.th2.constants.Constants.NEW_PASSWORD;
import static com.exactpro.th2.constants.Constants.NEW_PASSWORD_TAG;
import static com.exactpro.th2.constants.Constants.NEW_SEQ_NO;
import static com.exactpro.th2.constants.Constants.NEW_SEQ_NO_TAG;
import static com.exactpro.th2.constants.Constants.NEXT_EXPECTED_SEQ_NUM;
import static com.exactpro.th2.constants.Constants.NEXT_EXPECTED_SEQ_NUMBER_TAG;
import static com.exactpro.th2.constants.Constants.ORIG_SENDING_TIME;
import static com.exactpro.th2.constants.Constants.ORIG_SENDING_TIME_TAG;
import static com.exactpro.th2.constants.Constants.PASSWORD;
import static com.exactpro.th2.constants.Constants.PASSWORD_TAG;
import static com.exactpro.th2.constants.Constants.POSS_DUP;
import static com.exactpro.th2.constants.Constants.POSS_DUP_TAG;
import static com.exactpro.th2.constants.Constants.POSS_RESEND_TAG;
import static com.exactpro.th2.constants.Constants.RESET_SEQ_NUM;
import static com.exactpro.th2.constants.Constants.RESET_SEQ_NUM_TAG;
import static com.exactpro.th2.constants.Constants.SENDER_COMP_ID;
import static com.exactpro.th2.constants.Constants.SENDER_COMP_ID_TAG;
import static com.exactpro.th2.constants.Constants.SENDER_SUB_ID;
import static com.exactpro.th2.constants.Constants.SENDER_SUB_ID_TAG;
import static com.exactpro.th2.constants.Constants.SENDING_TIME;
import static com.exactpro.th2.constants.Constants.SENDING_TIME_TAG;
import static com.exactpro.th2.constants.Constants.SESSION_STATUS_TAG;
import static com.exactpro.th2.constants.Constants.SUCCESSFUL_LOGOUT_CODE;
import static com.exactpro.th2.constants.Constants.TARGET_COMP_ID;
import static com.exactpro.th2.constants.Constants.TARGET_COMP_ID_TAG;
import static com.exactpro.th2.constants.Constants.TEST_REQ_ID;
import static com.exactpro.th2.constants.Constants.TEST_REQ_ID_TAG;
import static com.exactpro.th2.constants.Constants.TEXT;
import static com.exactpro.th2.constants.Constants.TEXT_TAG;
import static com.exactpro.th2.constants.Constants.USERNAME;
import static com.exactpro.th2.netty.bytebuf.util.ByteBufUtil.asExpandable;
import static com.exactpro.th2.netty.bytebuf.util.ByteBufUtil.indexOf;
import static com.exactpro.th2.netty.bytebuf.util.ByteBufUtil.isEmpty;
import static com.exactpro.th2.netty.bytebuf.util.ByteBufUtil.startsWith;
import static com.exactpro.th2.util.MessageUtil.findByte;
import static com.exactpro.th2.util.MessageUtil.getBodyLength;
import static com.exactpro.th2.util.MessageUtil.getChecksum;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.Objects.requireNonNull;

//todo add events

public class FixHandler implements AutoCloseable, IHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FixHandler.class);

    private static final int DAY_SECONDS = 24 * 60 * 60;
    private static final String SOH = "\001";
    private static final byte BYTE_SOH = 1;
    private static final String STRING_MSG_TYPE = "MsgType";
    private static final String REJECT_REASON = "Reject reason";
    private static final String UNGRACEFUL_DISCONNECT_PROPERTY = "ungracefulDisconnect";
    private static final String ENABLE_STRATEGIES_PROPERTY = "enableStrategy";
    private static final String DISABLE_STRATEGIES_PROPERTY = "disableStrategy";
    private static final String STUBBING_VALUE = "XXX";
    private static final String SPLIT_SEND_TIMESTAMPS_PROPERTY = "BufferSlicesSendingTimes";
    private static final String STRATEGY_EVENT_TYPE = "StrategyState";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ISO_INSTANT;
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Random random = new Random();
    private final AtomicBoolean activeLogonExchange = new AtomicBoolean(false);
    private final AtomicInteger msgSeqNum = new AtomicInteger(0);
    private final AtomicInteger serverMsgSeqNum = new AtomicInteger(0);
    private final AtomicInteger testReqID = new AtomicInteger(0);
    private final AtomicBoolean activeRecovery = new AtomicBoolean(false);
    private final AtomicBoolean sessionActive = new AtomicBoolean(true);
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean connStarted = new AtomicBoolean(false);
    private final AtomicBoolean strategiesEnabled = new AtomicBoolean(true);
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private final IHandlerContext context;
    private final InetSocketAddress address;

    private final StatefulStrategy strategy = defaultStrategyHolder();
    private final StrategyScheduler scheduler;
    private final EventID strategyRootEvent;

    private final MessageLoader messageLoader;
    private final ReentrantLock recoveryLock = new ReentrantLock();

    private final AtomicReference<Future<?>> heartbeatTimer = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final AtomicReference<Future<?>> testRequestTimer = new AtomicReference<>(CompletableFuture.completedFuture(null));

    private final SendingTimeoutHandler sendingTimeoutHandler;
    private Future<?> reconnectRequestTimer = CompletableFuture.completedFuture(null);
    private volatile IChannel channel;
    protected FixHandlerSettings settings;
    private final MessageTransformer messageTransformer = MessageTransformer.INSTANCE;

    private final PasswordManager passwordManager;

    public FixHandler(IHandlerContext context) {
        this.context = context;
        strategyRootEvent = context.send(CommonUtil.toEvent("Strategy root event"), null);
        this.settings = (FixHandlerSettings) context.getSettings();
        if(settings.isLoadSequencesFromCradle() || settings.isLoadMissedMessagesFromCradle()) {
            this.messageLoader = new MessageLoader(
                context.getGrpcService(DataProviderService.class),
                settings.getSessionStartTime(),
                context.getBookName()
            );
        } else {
            this.messageLoader = null;
        }

        if(settings.getSessionStartTime() != null) {
            Objects.requireNonNull(settings.getSessionEndTime(), "Session end is required when session start is presented");
            LocalTime resetTime = settings.getSessionStartTime();
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            ZonedDateTime scheduleTime = now.with(resetTime);

            if(scheduleTime.isBefore(now)) {
                scheduleTime = now.plusDays(1).with(resetTime);
            }
            long time = now.until(scheduleTime, ChronoUnit.SECONDS);
            executorService.scheduleAtFixedRate(this::reset, time, DAY_SECONDS, TimeUnit.SECONDS);
        }

        if(settings.getSessionEndTime() != null) {
            LocalTime resetTime = settings.getSessionEndTime();
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            ZonedDateTime scheduleTime = now.with(resetTime);

            if(scheduleTime.isBefore(now)) {
                scheduleTime = now.plusDays(1).with(resetTime);
            }

            long time = now.until(scheduleTime, ChronoUnit.SECONDS);
            executorService.scheduleAtFixedRate(() -> {
                sendLogout();
                waitLogoutResponse();
                channel.close();
                sessionActive.set(false);
            }, time, DAY_SECONDS, TimeUnit.SECONDS);

            LocalDate today = LocalDate.now(ZoneOffset.UTC);

            LocalDateTime start = settings.getSessionStartTime().atDate(today);
            LocalDateTime end = settings.getSessionEndTime().atDate(today);

            LocalDateTime nowDateTime = LocalDateTime.now(ZoneOffset.UTC);
            if(nowDateTime.isAfter(end) && nowDateTime.isBefore(start)) {
                sessionActive.set(false);
            }
        }

        String host = settings.getHost();
        if (host == null || host.isBlank()) throw new IllegalArgumentException("host cannot be blank");
        int port = settings.getPort();
        if (port < 1 || port > 65535) throw new IllegalArgumentException("port must be in 1..65535 range");
        address = new InetSocketAddress(host, port);
        Objects.requireNonNull(settings.getSecurity(), "security cannot be null");
        Objects.requireNonNull(settings.getBeginString(), "BeginString can not be null");
        Objects.requireNonNull(settings.getResetSeqNumFlag(), "ResetSeqNumFlag can not be null");
        Objects.requireNonNull(settings.getResetOnLogon(), "ResetOnLogon can not be null");
        if (settings.getHeartBtInt() <= 0) throw new IllegalArgumentException("HeartBtInt cannot be negative or zero");
        if (settings.getTestRequestDelay() <= 0) throw new IllegalArgumentException("TestRequestDelay cannot be negative or zero");
        if (settings.getDisconnectRequestDelay() <= 0) throw new IllegalArgumentException("DisconnectRequestDelay cannot be negative or zero");

        passwordManager = new PasswordManager(settings.getInfraBackupUrl(), settings.getPassword(), settings.getNewPassword(), settings.getSenderCompID());

        this.sendingTimeoutHandler = SendingTimeoutHandler.create(
            settings.getMinConnectionTimeoutOnSend(),
            settings.getConnectionTimeoutOnSend(),
            context::send
        );

        if (settings.getBrokenConnConfiguration() == null) {
            scheduler = new StrategyScheduler(SchedulerType.CONSECUTIVE, Collections.emptyList());
            return;
        }

        var brokenConnConfig = settings.getBrokenConnConfiguration();
        scheduler = new StrategyScheduler(brokenConnConfig.getSchedulerType(), brokenConnConfig.getRules());
        executorService.schedule(this::applyNextStrategy, 0, TimeUnit.MILLISECONDS);
        if (settings.getConnectionTimeoutOnSend() <= 0) {
            throw new IllegalArgumentException("connectionTimeoutOnSend must be greater than zero");
        }
    }

    @Override
    public void onStart() {
        channel = context.createChannel(address, settings.getSecurity(), Map.of(), true, settings.getReconnectDelay() * 1000L, settings.getRateLimit());
        if(settings.isLoadSequencesFromCradle()) {
            SequenceHolder sequences = messageLoader.loadInitialSequences(channel.getSessionGroup(), channel.getSessionAlias());
            LOGGER.info("Loaded sequences are: client - {}, server - {}", sequences.getClientSeq(), sequences.getServerSeq());
            msgSeqNum.set(sequences.getClientSeq());
            serverMsgSeqNum.set(sequences.getServerSeq());
        }
        if(!channel.isOpen()) channel.open();
    }

    @NotNull
    public CompletableFuture<MessageID> send(@NotNull ByteBuf body, @NotNull Map<String, String> properties, @Nullable EventID eventID) {
        strategy.getSendStrategy(SendStrategy::getSendPreprocessor).process(body, properties);
        if (!sessionActive.get()) {
            throw new IllegalStateException("Session is not active. It is not possible to send messages.");
        }

        FixField msgType = findField(body, MSG_TYPE_TAG);
        boolean isLogout = msgType != null && Objects.equals(msgType.getValue(), MSG_TYPE_LOGOUT);
        if(isLogout && !channel.isOpen()) {
            String message = String.format("%s - %s: Logout ignored as channel is already closed.", channel.getSessionGroup(), channel.getSessionAlias());
            LOGGER.warn(message);
            context.send(CommonUtil.toEvent(message));
            return CompletableFuture.completedFuture(null);
        }

        if(properties.containsKey(ENABLE_STRATEGIES_PROPERTY)) {
            strategiesEnabled.set(true);
            context.send(CommonUtil.toEvent("Enabled strategies using message property."));
            return CompletableFuture.completedFuture(null);
        }

        if(properties.containsKey(DISABLE_STRATEGIES_PROPERTY)) {
            strategiesEnabled.set(false);
            context.send(CommonUtil.toEvent("Disabled strategies using message property."));
            return CompletableFuture.completedFuture(null);
        }

        boolean isUngracefulDisconnect = Boolean.getBoolean(properties.get(UNGRACEFUL_DISCONNECT_PROPERTY));
        if(isLogout) {
            context.send(CommonUtil.toEvent(String.format("Closing session %s. Is graceful disconnect: %b", channel.getSessionAlias(), !isUngracefulDisconnect)));
            try {
                disconnect(!isUngracefulDisconnect);
                enabled.set(false);
                activeLogonExchange.set(false);
                sendingTimeoutHandler.getWithTimeout(channel.open());
            } catch (Exception e) {
                context.send(CommonUtil.toErrorEvent(String.format("Error while ending session %s by user logout. Is graceful disconnect: %b", channel.getSessionAlias(), !isUngracefulDisconnect), e));
            }
            return CompletableFuture.completedFuture(null);
        }

        // TODO: probably, this should be moved to the core part
        // But those changes will break API
        // So, let's keep it here for now
        long deadline = sendingTimeoutHandler.getDeadline();
        long currentTimeout = sendingTimeoutHandler.getCurrentTimeout();

        if (!channel.isOpen()) {
            try {
                sendingTimeoutHandler.getWithTimeout(channel.open());
            } catch (TimeoutException e) {
                ExceptionUtils.rethrow(new TimeoutException(
                        String.format("could not open connection before timeout %d mls elapsed",
                                currentTimeout)));
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
        }

        if(strategy.getAllowMessagesBeforeLogon()) {
            while (!channel.isOpen()) {
                if (LOGGER.isWarnEnabled()) LOGGER.warn("Session is not yet logged in: {}", channel.getSessionAlias());
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.error("Error while sleeping.");
                }
                if (System.currentTimeMillis() > deadline) {
                    // The method should have checked exception in signature...
                    ExceptionUtils.rethrow(new TimeoutException(String.format("session was not established within %d mls",
                        settings.getConnectionTimeoutOnSend())));
                }
            }
        } else {
            while (!enabled.get()) {
                if (LOGGER.isWarnEnabled()) LOGGER.warn("Session is not yet logged in: {}", channel.getSessionAlias());
                try {
                    //noinspection BusyWait
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOGGER.error("Error while sleeping.");
                }
                if (System.currentTimeMillis() > deadline) {
                    // The method should have checked exception in signature...
                    ExceptionUtils.rethrow(new TimeoutException(String.format("session was not established within %d mls",
                        settings.getConnectionTimeoutOnSend())));
                }
            }
        }

        if(strategy.getAllowMessagesBeforeRetransmissionFinishes()) {
            return strategy.getSendStrategy(SendStrategy::getSendHandler).send(channel, body, properties, eventID);
        } else {
            try {
                recoveryLock.lock();
                return strategy.getSendStrategy(SendStrategy::getSendHandler).send(channel, body, properties, eventID);
            } finally {
                recoveryLock.unlock();
            }
        }
    }

    @NotNull
    @Override
    public CompletableFuture<MessageID> send(@NotNull RawMessage rawMessage) {
        return send(toByteBuf(rawMessage.getBody()), new HashMap<>(rawMessage.getMetadata().getPropertiesMap()), getEventId(rawMessage));
    }

    @NotNull
    @Override
    public CompletableFuture<MessageID> send(@NotNull com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage message) {
        final var id = message.getEventId();
        return send(message.getBody(), new HashMap<>(message.getMetadata()), id != null ? EventUtilsKt.toProto(id) : null);
    }

    @Override
    public ByteBuf onReceive(@NotNull IChannel channel, @NotNull ByteBuf buffer) {
        strategy.getReceiveMessageStrategy(ReceiveStrategy::getReceivePreprocessor).process(buffer, Collections.emptyMap());
        int offset = buffer.readerIndex();
        if (offset == buffer.writerIndex()) return null;

        int beginStringIdx = indexOf(buffer, "8=FIX");
        if (beginStringIdx < 0) {
            return null;
        }

        if (beginStringIdx > offset) {
            buffer.readerIndex(beginStringIdx);
            return buffer.retainedSlice(offset, beginStringIdx - offset);
        }

        int nextBeginString = indexOf(buffer, SOH + "8=FIX") + 1;
        int checksum = indexOf(buffer, CHECKSUM);
        int endOfMessageIdx = findByte(buffer, checksum + 1, BYTE_SOH);

        try {
            if (checksum == -1 || endOfMessageIdx == -1 || endOfMessageIdx - checksum != 7) {
                throw new IllegalStateException("Failed to parse message: " + buffer.toString(US_ASCII) + ". No Checksum or no tag separator at the end of the message with index: " + beginStringIdx);
            }
        } catch (Exception e) {
            if (nextBeginString > 0) {
                buffer.readerIndex(nextBeginString);
            } else {
                buffer.readerIndex(beginStringIdx);
            }
            return null;
        }

        buffer.readerIndex(endOfMessageIdx + 1);
        return buffer.retainedSlice(beginStringIdx, endOfMessageIdx + 1 - beginStringIdx);
    }

    @NotNull
    @Override
    public Map<String, String> onIncoming(@NotNull IChannel channel, @NotNull ByteBuf message, MessageID messageId) {
        Map<String, String> metadata = new HashMap<>();

        StrategyState state = strategy.getState();
        state.enrichProperties(metadata);
        if(strategy.getIncomingMessageStrategy(IncomingMessagesStrategy::getIncomingMessagesPreprocessor).process(message, metadata) != null) {
            state.addMessageID(messageId);
            return metadata;
        }

        int beginString = indexOf(message, "8=FIX");

        if (beginString == -1) {
            metadata.put(REJECT_REASON, "Not a FIX message");
            return metadata;
        }

        FixField msgSeqNumValue = findField(message, MSG_SEQ_NUM_TAG);
        if (msgSeqNumValue == null) {
            metadata.put(REJECT_REASON, "No msgSeqNum Field");
            if (LOGGER.isErrorEnabled()) LOGGER.error("Invalid message. No MsgSeqNum in message: {}", message.toString(US_ASCII));
            return metadata;
        }

        FixField msgType = findField(message, MSG_TYPE_TAG);
        if (msgType == null) {
            metadata.put(REJECT_REASON, "No msgType Field");
            if (LOGGER.isErrorEnabled()) LOGGER.error("Invalid message. No MsgType in message: {}", message.toString(US_ASCII));
            return metadata;
        }

        FixField possDup = findField(message, POSS_DUP_TAG);
        boolean isDup = false;
        if(possDup != null) {
            isDup = Objects.equals(possDup.getValue(), IS_POSS_DUP);
        }

        String msgTypeValue = requireNonNull(msgType.getValue());
        if(msgTypeValue.equals(MSG_TYPE_LOGOUT)) {
            serverMsgSeqNum.incrementAndGet();
            state.addMessageID(messageId);
            strategy.getIncomingMessageStrategy(x -> x.getLogoutStrategy()).process(message, metadata);
            return metadata;
        }

        int receivedMsgSeqNum = Integer.parseInt(requireNonNull(msgSeqNumValue.getValue()));

        if(msgTypeValue.equals(MSG_TYPE_LOGON) && receivedMsgSeqNum < serverMsgSeqNum.get()) {
            FixField resetSeqNumFlagField = findField(message, RESET_SEQ_NUM_TAG);
            if(resetSeqNumFlagField != null && Objects.equals(resetSeqNumFlagField.getValue(), IS_SEQUENCE_RESET_FLAG)) {
                serverMsgSeqNum.set(0);
            }
        }

        if(receivedMsgSeqNum < serverMsgSeqNum.get() && !isDup) {
            if(settings.isLogoutOnIncorrectServerSequence()) {
                context.send(CommonUtil.toEvent(String.format("Received server sequence %d but expected %d. Sending logout with text: MsgSeqNum is too low...", receivedMsgSeqNum, serverMsgSeqNum.get())));
                sendLogout(String.format("MsgSeqNum too low, expecting %d but received %d", serverMsgSeqNum.get() + 1, receivedMsgSeqNum));
                reconnectRequestTimer = executorService.schedule(this::sendLogon, settings.getReconnectDelay(), TimeUnit.SECONDS);
                if (LOGGER.isErrorEnabled()) LOGGER.error("Invalid message. SeqNum is less than expected {}: {}", serverMsgSeqNum.get(), message.toString(US_ASCII));
            } else {
                context.send(CommonUtil.toEvent(String.format("Received server sequence %d but expected %d. Correcting server sequence.", receivedMsgSeqNum, serverMsgSeqNum.get() + 1)));
                serverMsgSeqNum.set(receivedMsgSeqNum - 1);
            }
            metadata.put(REJECT_REASON, "SeqNum is less than expected.");
            return metadata;
        }

        if(!isDup) {
            serverMsgSeqNum.incrementAndGet();
        }

        if (serverMsgSeqNum.get() < receivedMsgSeqNum && !isDup && enabled.get()) {
            sendResendRequest(serverMsgSeqNum.get(), receivedMsgSeqNum - 1);
            serverMsgSeqNum.set(receivedMsgSeqNum);
        }

        switch (msgTypeValue) {
            case MSG_TYPE_HEARTBEAT:
                if (LOGGER.isInfoEnabled()) LOGGER.info("Heartbeat received - {}", message.toString(US_ASCII));
                handleHeartbeat(message);
                break;
            case MSG_TYPE_LOGON:
                state.addMessageID(messageId);
                Map<String, String> logonMetadata = strategy.getIncomingMessageStrategy(IncomingMessagesStrategy::getLogonStrategy).process(message, metadata);
                if (logonMetadata != null) return logonMetadata;
                if(serverMsgSeqNum.get() < receivedMsgSeqNum && !isDup && !enabled.get()) {
                    if(strategy.getSendResendRequestOnLogonGap() && serverMsgSeqNum.get() > 5 ) {
                        sendResendRequest(serverMsgSeqNum.get() - 5, 0);
                    }
                }
                break;
            case MSG_TYPE_RESEND_REQUEST:
                state.addMessageID(messageId);
                if (LOGGER.isInfoEnabled()) LOGGER.info("Resend request received - {}", message.toString(US_ASCII));
                handleResendRequest(message);
                break;
            case MSG_TYPE_SEQUENCE_RESET: //gap fill
                state.addMessageID(messageId);
                if (LOGGER.isInfoEnabled()) LOGGER.info("Sequence reset received - {}", message.toString(US_ASCII));
                resetSequence(message);
                break;
            case MSG_TYPE_TEST_REQUEST:
                state.addMessageID(messageId);
                if(LOGGER.isInfoEnabled()) LOGGER.info("Test request received - {}", message.toString(US_ASCII));
                if(strategy.getIncomingMessageStrategy(IncomingMessagesStrategy::getTestRequestProcessor).process(message, metadata) != null) {
                    return metadata;
                }
                break;
            default:
                if(isDup) {
                    state.addMessageID(messageId);
                }
                if(LOGGER.isInfoEnabled()) LOGGER.info("Received message - {}", message.toString(US_ASCII));
        }

        resetTestRequestTask();

        metadata.put(STRING_MSG_TYPE, msgTypeValue);

        return metadata;
    }

    private Map<String, String> handleTestRequest(ByteBuf message, Map<String, String> metadata) {
        FixField testReqId = findField(message, TEST_REQ_ID_TAG);
        if(testReqId == null || testReqId.getValue() == null) {
            metadata.put(REJECT_REASON, "Test Request message hasn't got TestReqId field.");
            return metadata;
        }

        sendHeartbeatWithTestRequest(testReqId.getValue());

        return null;
    }

    @Nullable
    private Map<String, String> handleLogon(@NotNull ByteBuf message, Map<String, String> metadata) {
        if (LOGGER.isInfoEnabled()) LOGGER.info("Logon received - {}", message.toString(US_ASCII));
        boolean connectionSuccessful = checkLogon(message);
        if (connectionSuccessful) {
            if(settings.useNextExpectedSeqNum()) {
                FixField nextExpectedSeqField = findField(message, NEXT_EXPECTED_SEQ_NUMBER_TAG);
                if(nextExpectedSeqField == null) {
                    metadata.put(REJECT_REASON, "No NextExpectedSeqNum field");
                    if (LOGGER.isErrorEnabled()) LOGGER.error("Invalid message. No NextExpectedSeqNum in message: {}", message.toString(US_ASCII));
                    return metadata;
                }

                int nextExpectedSeqNumber = Integer.parseInt(requireNonNull(nextExpectedSeqField.getValue()));
                int seqNum = msgSeqNum.incrementAndGet() + 1;
                if(nextExpectedSeqNumber < seqNum) {
                    try {
                        recoveryLock.lock();
                        activeRecovery.set(true);
                        Thread.sleep(settings.getCradleSaveTimeoutMs());
                        if(!channel.isOpen()) {
                            LOGGER.warn("Recovery is interrupted.");
                        } else {
                            strategy.getRecoveryHandler().recovery(nextExpectedSeqNumber, seqNum);
                        }
                    } catch (InterruptedException e) {
                        LOGGER.error("Error while waiting for cradle save timeout.", e);
                    } finally {
                        recoveryLock.unlock();
                        activeRecovery.set(false);
                    }
                } else if (nextExpectedSeqNumber > seqNum) {
                    context.send(
                            Event.start()
                                    .name(String.format("Corrected next client seq num from %s to %s", seqNum, nextExpectedSeqNumber))
                                    .type("Logon"),
                        null
                    );
                    msgSeqNum.set(nextExpectedSeqNumber - 1);
                }
            } else {
                msgSeqNum.incrementAndGet();
            }

            enabled.set(true);
            activeLogonExchange.set(false);

            if (!connStarted.get()){
                connStarted.set(true);
            }

            resetHeartbeatTask();

            resetTestRequestTask();
        } else {
            enabled.set(false);
            activeLogonExchange.set(false);
            reconnectRequestTimer = executorService.schedule(this::sendLogon, settings.getReconnectDelay(), TimeUnit.SECONDS);
        }
        return null;
    }

    private void handleHeartbeat(@NotNull ByteBuf message) {
        checkHeartbeat(message);
    }

    private Map<String, String> handleLogout(@NotNull ByteBuf message, Map<String, String> metadata) {
        if (LOGGER.isInfoEnabled()) LOGGER.info("Logout received - {}", message.toString(US_ASCII));
        if(strategy.getSendResendRequestOnLogoutReply()) {
            sendResendRequest(serverMsgSeqNum.get() - 5, 0);
        }
        FixField sessionStatus = findField(message, SESSION_STATUS_TAG);
        boolean isSequenceChanged = false;
        if(sessionStatus != null) {
            int statusCode = Integer.parseInt(Objects.requireNonNull(sessionStatus.getValue()));
            if(statusCode != SUCCESSFUL_LOGOUT_CODE) {
                FixField text = findField(message, TEXT_TAG);
                if (text != null) {
                    LOGGER.warn("Received Logout has text (58) tag: {}", text.getValue());
                    String wrongClientSequence = StringUtils.substringBetween(text.getValue(), "expecting ", " but received");
                    if (wrongClientSequence != null) {
                        msgSeqNum.set(Integer.parseInt(wrongClientSequence) - 1);
                        isSequenceChanged = true;
                    }
                    String wrongClientNextExpectedSequence = StringUtils.substringBetween(text.getValue(), "MSN to be sent is ", " but received");
                    if(wrongClientNextExpectedSequence != null && settings.getResetStateOnServerReset()) {
                        int wrong = Integer.parseInt(wrongClientNextExpectedSequence);
                        serverMsgSeqNum.set(wrong);
                        if(wrong == 1) {
                            msgSeqNum.set(1);
                        }
                    }
                }
            }
        }

        if(!enabled.get() && !isSequenceChanged) {
            msgSeqNum.incrementAndGet();
        }

        cancelFuture(heartbeatTimer);
        cancelFuture(testRequestTimer);
        enabled.set(false);
        activeLogonExchange.set(false);
        context.send(CommonUtil.toEvent("logout for sender - " + settings.getSenderCompID()), null);//make more useful
        try {
            disconnect(false);
            channel.open();
        } catch (Exception e) {
            LOGGER.error("Error while disconnecting in handle logout.");
        }
        return metadata;
    }

    private void resetSequence(ByteBuf message) {
        FixField seqNumValue = findField(message, NEW_SEQ_NO_TAG);
        FixField gapFillMode = findField(message, GAP_FILL_FLAG_TAG);

        if(seqNumValue == null) {
            LOGGER.warn("Failed to reset servers MsgSeqNum. No such tag in message: {}", message.toString(US_ASCII));
            return;
        }

        if(gapFillMode == null || gapFillMode.getValue() == null || gapFillMode.getValue().equals("N")) {
            serverMsgSeqNum.set(Integer.parseInt(requireNonNull(seqNumValue.getValue())));
        } else {
            int newSeqNo = Integer.parseInt(requireNonNull(seqNumValue.getValue()));
            serverMsgSeqNum.updateAndGet(sequence -> {
                if(sequence < newSeqNo - 1) {
                    return newSeqNo - 1;
                } else {
                    return sequence;
                }
            });
        }
    }

    private void reset() {
        msgSeqNum.set(0);
        serverMsgSeqNum.set(0);
        sessionActive.set(true);
        if(messageLoader != null) {
            messageLoader.updateTime();
        }
        if(!channel.isOpen()) channel.open();
    }

    public void sendResendRequest(int beginSeqNo, int endSeqNo) {
        sendResendRequest(beginSeqNo, endSeqNo, false);
    }

    public void sendResendRequest(int beginSeqNo, int endSeqNo, boolean isPossDup) { //do private
        LOGGER.info("Sending resend request: {} - {}", beginSeqNo, endSeqNo);
        StringBuilder resendRequest = new StringBuilder();
        setHeader(resendRequest, MSG_TYPE_RESEND_REQUEST, msgSeqNum.incrementAndGet(), null, isPossDup);
        resendRequest.append(BEGIN_SEQ_NO).append(beginSeqNo);
        resendRequest.append(END_SEQ_NO).append(endSeqNo);
        setChecksumAndBodyLength(resendRequest);
        channel.send(Unpooled.wrappedBuffer(resendRequest.toString().getBytes(StandardCharsets.UTF_8)),
                        strategy.getState().enrichProperties(),
                        null,
                        SendMode.HANDLE_AND_MANGLE)
            .thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
        resetHeartbeatTask();
    }

    void sendResendRequest(int beginSeqNo) { //do private
        StringBuilder resendRequest = new StringBuilder();
        setHeader(resendRequest, MSG_TYPE_RESEND_REQUEST, msgSeqNum.incrementAndGet(), null);
        resendRequest.append(BEGIN_SEQ_NO).append(beginSeqNo);
        resendRequest.append(END_SEQ_NO).append(0);
        setChecksumAndBodyLength(resendRequest);

        if (enabled.get()) {
            channel.send(Unpooled.wrappedBuffer(resendRequest.toString().getBytes(StandardCharsets.UTF_8)),
                            strategy.getState().enrichProperties(),
                            null,
                            SendMode.HANDLE_AND_MANGLE)
                .thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
            resetHeartbeatTask();
        }
    }

    private void handleResendRequest(ByteBuf message) {

        FixField strBeginSeqNo = findField(message, BEGIN_SEQ_NO_TAG);
        FixField strEndSeqNo = findField(message, END_SEQ_NO_TAG);

        if (strBeginSeqNo != null && strEndSeqNo != null) {
            int beginSeqNo = Integer.parseInt(requireNonNull(strBeginSeqNo.getValue()));
            int endSeqNo = Integer.parseInt(requireNonNull(strEndSeqNo.getValue()));

            try {
                recoveryLock.lock();
                activeRecovery.set(true);
                Thread.sleep(settings.getCradleSaveTimeoutMs());
                if(!channel.isOpen()) {
                    LOGGER.warn("Recovery is interrupted.");
                } else {
                    strategy.getRecoveryHandler().recovery(beginSeqNo, endSeqNo);
                }
            } catch (InterruptedException e) {
                LOGGER.error("Error while waiting for cradle save timeout.", e);
            } finally {
                recoveryLock.unlock();
                activeRecovery.set(false);
            }
        }
    }

    private void recovery(int beginSeqNo, int endSeqNo, RecoveryConfig recoveryConfig) {
        AtomicInteger lastProcessedSequence = new AtomicInteger(beginSeqNo - 1);
        try {

            if (endSeqNo == 0) {
                endSeqNo = msgSeqNum.get() + 1;
            }

            AtomicBoolean skip = new AtomicBoolean(recoveryConfig.getOutOfOrder());
            AtomicReference<ByteBuf> skipped = new AtomicReference(null);

            int endSeq = endSeqNo;
            LOGGER.info("Loading messages from {} to {}", beginSeqNo, endSeqNo);
            if(settings.isLoadMissedMessagesFromCradle()) {
                Function1<ByteBuf, Boolean> processMessage = (buf) -> {
                    FixField seqNum = findField(buf, MSG_SEQ_NUM_TAG);
                    FixField msgTypeField = findField(buf, MSG_TYPE_TAG);
                    if(seqNum == null || seqNum.getValue() == null
                            || msgTypeField == null || msgTypeField.getValue() == null) {
                        return true;
                    }
                    int sequence = Integer.parseInt(seqNum.getValue());
                    String msgType = msgTypeField.getValue();

                    if(sequence < beginSeqNo) return true;
                    if(sequence > endSeq) return false;

                    if(recoveryConfig.getSequenceResetForAdmin() && ADMIN_MESSAGES.contains(msgType)) return true;
                    FixField possDup = findField(buf, POSS_DUP_TAG);
                    if(possDup != null && Objects.equals(possDup.getValue(), IS_POSS_DUP)) return true;

                    if(sequence - 1 != lastProcessedSequence.get() ) {
                        StringBuilder sequenceReset =
                                createSequenceReset(Math.max(beginSeqNo, lastProcessedSequence.get() + 1), sequence);
                        channel.send(Unpooled.wrappedBuffer(sequenceReset.toString().getBytes(StandardCharsets.UTF_8)),
                                strategy.getState().enrichProperties(),
                                null,
                                SendMode.MANGLE);
                        resetHeartbeatTask();
                    }

                    setTime(buf);
                    setPossDup(buf);
                    updateLength(buf);
                    updateChecksum(buf);
                    if(!skip.get()) {
                        channel.send(buf, strategy.getState().enrichProperties(), null, SendMode.MANGLE)
                            .thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
                        try {
                            Thread.sleep(settings.getRecoverySendIntervalMs());
                        } catch (InterruptedException e) {
                            LOGGER.error("Error while waiting send interval during recovery", e);
                        }
                    }

                    if(skip.get() && recoveryConfig.getOutOfOrder()) {
                        skipped.set(buf);
                        skip.set(false);
                    }

                    if(!skip.get() && recoveryConfig.getOutOfOrder()) {
                        skip.set(true);
                        channel.send(skipped.get(), strategy.getState().enrichProperties(), null, SendMode.MANGLE)
                            .thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
                        try {
                            Thread.sleep(settings.getRecoverySendIntervalMs());
                        } catch (InterruptedException e) {
                            LOGGER.error("Error while waiting send interval during recovery", e);
                        }
                    }

                    resetHeartbeatTask();

                    lastProcessedSequence.set(sequence);
                    return true;
                };

                // waiting for messages to be writen in cradle
                messageLoader.processMessagesInRange(
                        channel.getSessionGroup(), channel.getSessionAlias(), Direction.SECOND,
                        beginSeqNo,
                    processMessage
                );

                if(lastProcessedSequence.get() < endSeq && msgSeqNum.get() + 1 != lastProcessedSequence.get() + 1) {
                    String seqReset = createSequenceReset(Math.max(lastProcessedSequence.get() + 1, beginSeqNo), msgSeqNum.get() + 1).toString();
                    channel.send(
                        Unpooled.wrappedBuffer(seqReset.getBytes(StandardCharsets.UTF_8)),
                            strategy.getState().enrichProperties(), null, SendMode.MANGLE
                    ).thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
                }
            } else {
                String seqReset =
                    createSequenceReset(beginSeqNo, msgSeqNum.get() + 1).toString();
                channel.send(
                    Unpooled.wrappedBuffer(seqReset.getBytes(StandardCharsets.UTF_8)),
                        strategy.getState().enrichProperties(), null, SendMode.MANGLE
                );
            }
            resetHeartbeatTask();

        } catch (Exception e) {
            LOGGER.error("Error while loading messages for recovery", e);
            String seqReset =
                createSequenceReset(Math.max(beginSeqNo, lastProcessedSequence.get() + 1), msgSeqNum.get() + 1).toString();
            channel.send(
                    Unpooled.buffer().writeBytes(seqReset.getBytes(StandardCharsets.UTF_8)),
                    strategy.getState().enrichProperties(),
                    null,
                    SendMode.MANGLE
            );
        }
    }

    private void checkHeartbeat(ByteBuf message) {

        FixField receivedTestReqID = findField(message, TEST_REQ_ID_TAG);

        if (receivedTestReqID != null) {
            if (Objects.equals(receivedTestReqID.getValue(), Integer.toString(testReqID.get()))) {
                reconnectRequestTimer.cancel(false);
            }
        }
    }

    private boolean checkLogon(ByteBuf message) {
        FixField sessionStatusField = findField(message, SESSION_STATUS_TAG); //check another options

        String sessionStatusValue = "0";
        if(sessionStatusField != null) {
            sessionStatusValue = sessionStatusField.getValue();
        }

        if(!Objects.equals(sessionStatusValue, "0") && !Objects.equals(sessionStatusValue, "1")) {
            return false;
        }

        FixField msgSeqNumValue = findField(message, MSG_SEQ_NUM_TAG);
        if (msgSeqNumValue == null) {
            return false;
        }
        serverMsgSeqNum.set(Integer.parseInt(requireNonNull(msgSeqNumValue.getValue())));
        context.send(CommonUtil.toEvent("successful login"), null);
        return true;
    }

    @Override
    public void onOutgoing(@NotNull IChannel channel, @NotNull ByteBuf message, @NotNull Map<String, String> metadata) {
        strategy.getState().enrichProperties(metadata);
        strategy.getOutgoingMessageStrategy(OutgoingMessagesStrategy::getOutgoingMessageProcessor).process(message, metadata);

        if (LOGGER.isInfoEnabled()) LOGGER.info("Outgoing message: {}", message.toString(US_ASCII));
        if(enabled.get()) resetHeartbeatTask();
    }

    public void onOutgoingUpdateTag(@NotNull ByteBuf message, @NotNull Map<String, String> metadata) {
        FixField msgType = findField(message, MSG_TYPE_TAG, US_ASCII);

        if(msgType != null && ADMIN_MESSAGES.contains(msgType.getValue())) {
            return;
        }

        if (isEmpty(message)) {
            return;
        }

        FixField beginString = findField(message, BEGIN_STRING_TAG);

        if (beginString == null) {
            beginString = requireNonNull(firstField(message), () -> "First filed isn't found in message: " + message.toString(US_ASCII))
                    .insertPrevious(BEGIN_STRING_TAG, settings.getBeginString());
        }

        FixField bodyLength = findField(message, BODY_LENGTH_TAG, US_ASCII, beginString);

        if (bodyLength == null) { // Length is updated at the of the current method
            bodyLength = beginString.insertNext(BODY_LENGTH_TAG, STUBBING_VALUE);
        }

        if (msgType == null) {                                                        //should we interrupt sending message?
            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("No msgType in message {}", message.toString(US_ASCII));
            }

            if (metadata.get("MsgType") != null) {
                msgType = bodyLength.insertNext(MSG_TYPE_TAG, metadata.get("MsgType"));
            }
        }

        FixField checksum = findLastField(message, CHECKSUM_TAG);

        if (checksum == null) { // Length is updated at the of the current method
            checksum = lastField(message).insertNext(CHECKSUM_TAG, STUBBING_VALUE); //stubbing until finish checking message
        }

        FixField msgSeqNum = findField(message, MSG_SEQ_NUM_TAG, US_ASCII, bodyLength);
        int msgSeqNumValue = this.msgSeqNum.incrementAndGet();

        if (msgSeqNum == null) {

            if (msgType != null) {
                msgSeqNum = msgType.insertNext(MSG_SEQ_NUM_TAG, Integer.toString(msgSeqNumValue));
            } else {
                msgSeqNum = bodyLength.insertNext(MSG_SEQ_NUM_TAG, Integer.toString(msgSeqNumValue));
            }
        } else {
            msgSeqNum.setValue(Integer.toString(msgSeqNumValue));
        }

        FixField senderCompID = findField(message, SENDER_COMP_ID_TAG, US_ASCII, bodyLength);

        if (senderCompID == null) {
            senderCompID = msgSeqNum.insertNext(SENDER_COMP_ID_TAG, settings.getSenderCompID());
        } else {
            String value = senderCompID.getValue();

            if (value == null || value.isEmpty() || value.equals("null")) {
                senderCompID.setValue(settings.getSenderCompID());
            }
        }

        FixField targetCompID = findField(message, TARGET_COMP_ID_TAG, US_ASCII, bodyLength);

        if (targetCompID == null) {
            targetCompID = senderCompID.insertNext(TARGET_COMP_ID_TAG, settings.getTargetCompID());
        } else {
            String value = targetCompID.getValue();

            if (value == null || value.isEmpty() || value.equals("null")) {
                targetCompID.setValue(settings.getTargetCompID());
            }
        }

        if (settings.getSenderSubID() != null) {
            FixField senderSubID = findField(message, SENDER_SUB_ID_TAG, US_ASCII, bodyLength);

            if (senderSubID == null) {
                senderSubID = targetCompID.insertNext(SENDER_SUB_ID_TAG, settings.getSenderSubID());
            } else {
                String value = senderSubID.getValue();

                if (value == null || value.isEmpty() || value.equals("null")) {
                    senderSubID.setValue(settings.getSenderSubID());
                }
            }
        }
        FixField sendingTime = findField(message, SENDING_TIME_TAG, US_ASCII);

        if (sendingTime == null) {
            targetCompID.insertNext(SENDING_TIME_TAG, getTime());
        } else {
            String value = sendingTime.getValue();

            if (value == null || value.isEmpty() || value.equals("null")) {
                sendingTime.setValue(getTime());
            }
        }

        updateLength(message);
        updateChecksum(message);
    }

    @Override
    public void onOpen(@NotNull IChannel channel) {
        this.channel = channel;
        sendLogon();
    }

    public void sendHeartbeatWithPossDup(boolean isPossDup) {
        sendHeartbeatWithTestRequest(null, isPossDup);
    }

    private void sendHeartbeatWithTestRequest(String testRequestID) {
        sendHeartbeatWithTestRequest(testRequestID, false);
    }

    public void sendHeartbeat() {
        sendHeartbeatWithTestRequest(null, false);
    }

    private void sendHeartbeatWithTestRequest(String testRequestId, boolean possDup) {
        StringBuilder heartbeat = new StringBuilder();
        int seqNum = msgSeqNum.incrementAndGet();

        setHeader(heartbeat, MSG_TYPE_HEARTBEAT, seqNum, null, possDup);

        if(testRequestId != null) {
            heartbeat.append(TEST_REQ_ID).append(testRequestId);
        }

        setChecksumAndBodyLength(heartbeat);

        if (enabled.get()) {
            LOGGER.info("Send Heartbeat to server - {}", heartbeat);
            channel.send(Unpooled.wrappedBuffer(heartbeat.toString().getBytes(StandardCharsets.UTF_8)),
                    strategy.getState().enrichProperties(),
                    null,
                    SendMode.HANDLE_AND_MANGLE);
            resetHeartbeatTask();

        } else {
            sendLogon();
        }
    }

    public void sendTestRequest() { //do private
        sendTestRequestWithPossDup(false);
    }

    public void sendTestRequestWithPossDup(boolean isPossDup) { //do private
        StringBuilder testRequest = new StringBuilder();
        setHeader(testRequest, MSG_TYPE_TEST_REQUEST, msgSeqNum.incrementAndGet(), null, isPossDup);
        testRequest.append(TEST_REQ_ID).append(testReqID.incrementAndGet());
        setChecksumAndBodyLength(testRequest);
        if (enabled.get()) {
            channel.send(Unpooled.wrappedBuffer(testRequest.toString().getBytes(StandardCharsets.UTF_8)),
                            strategy.getState().enrichProperties(),
                            null,
                            SendMode.HANDLE_AND_MANGLE)
                .thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
            LOGGER.info("Send TestRequest to server - {}", testRequest);
            resetTestRequestTask();
            resetHeartbeatTask();
        } else {
            sendLogon();
        }
        reconnectRequestTimer = executorService.schedule(this::sendLogon, settings.getReconnectDelay(), TimeUnit.SECONDS);
    }

    public void sendLogon() {
        Map<String, String> props = new HashMap<>();
        if(!sessionActive.get() || !channel.isOpen()) {
            LOGGER.info("Logon is not sent to server because session is not active.");
            return;
        }

        if(activeLogonExchange.get()) {
            LOGGER.info("Active logon exchange already going on.");
            return;
        }

        activeLogonExchange.set(true);

        if(enabled.get()) {
            String message = String.format("Logon attempt while already logged in: %s - %s", channel.getSessionGroup(), channel.getSessionAlias());
            LOGGER.warn(message);
            context.send(CommonUtil.toEvent(message));
            return;
        }

        StringBuilder logon = buildLogon(props);

        LOGGER.info("Send logon - {}", logon);
        channel.send(Unpooled.wrappedBuffer(logon.toString().getBytes(StandardCharsets.UTF_8)),
                        strategy.getState().enrichProperties(props),
                        null,
                        SendMode.HANDLE_AND_MANGLE)
            .thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
    }

    private StringBuilder buildLogon(Map<String, String> props) {
        StringBuilder logon = new StringBuilder();
        Boolean reset;
        if (!connStarted.get()) {
            reset = settings.getResetSeqNumFlag();
        } else {
            reset = settings.getResetOnLogon();
        }
        if (reset) msgSeqNum.getAndSet(0);

        setHeader(logon, MSG_TYPE_LOGON, msgSeqNum.get() + 1, null);
        if (settings.useNextExpectedSeqNum()) logon.append(NEXT_EXPECTED_SEQ_NUM).append(serverMsgSeqNum.get() + 1);
        if (settings.getEncryptMethod() != null) logon.append(ENCRYPT_METHOD).append(settings.getEncryptMethod());
        logon.append(HEART_BT_INT).append(settings.getHeartBtInt());
        if (reset) logon.append(RESET_SEQ_NUM).append("Y");
        if (settings.getDefaultApplVerID() != null) logon.append(DEFAULT_APPL_VER_ID).append(settings.getDefaultApplVerID());
        if (settings.getUsername() != null) logon.append(USERNAME).append(settings.getUsername());
        passwordManager.use((x) -> {
            if (x.getPassword() != null) {
                if (settings.getPasswordEncryptKey() != null) {
                    props.put("PASSWORD", x.getPassword());
                    logon.append(ENCRYPTED_PASSWORD).append(encrypt(x.getPassword(), settings.getPasswordEncryptKey(), settings.getPasswordEncryptAlgorithm(), settings.getPasswordKeyEncryptAlgorithm()));
                } else {
                    logon.append(PASSWORD).append(x.getPassword());
                }
            }

            if (x.getNewPassword() != null) {
                props.put("NEW_PASSWORD", x.getNewPassword());
                if (settings.getPasswordEncryptKey() != null) {
                    logon.append(NEW_ENCRYPTED_PASSWORD).append(encrypt(x.getNewPassword(), settings.getPasswordEncryptKey(), settings.getPasswordEncryptAlgorithm(), settings.getPasswordKeyEncryptAlgorithm()));
                } else {
                    logon.append(NEW_PASSWORD).append(x.getNewPassword());
                }
            }

            return Unit.INSTANCE;
        });

        setChecksumAndBodyLength(logon);

        return logon;
    }

    private void sendLogout(boolean isPossDup) {
        sendLogout(null, isPossDup);
    }

    private void sendLogout(String text) {
        sendLogout(text, false);
    }

    private void sendLogout() {
        sendLogout(null, false);
    }

    private void sendLogout(String text, boolean isPossDup) {
        if (enabled.get()) {
            StringBuilder logout = new StringBuilder();
            setHeader(logout, MSG_TYPE_LOGOUT, msgSeqNum.incrementAndGet(), null, isPossDup);
            if(text != null) {
               logout.append(TEXT).append(text);
            }
            setChecksumAndBodyLength(logout);

            LOGGER.debug("Sending logout - {}", logout);

            try {
                MessageID messageID = channel.send(
                        Unpooled.wrappedBuffer(logout.toString().getBytes(StandardCharsets.UTF_8)),
                        strategy.getState().enrichProperties(),
                        null,
                        SendMode.HANDLE_AND_MANGLE
                ).get();
                strategy.getState().addMessageID(messageID);

                LOGGER.info("Sent logout - {}", logout);
            } catch (Exception e) {
                LOGGER.error("Failed to send logout - {}", logout, e);
            }
        }
    }

    private String encrypt(String password, String encryptKey, String encryptAlgo, String encryptKeyAlgo) {
        return settings.getPasswordEncryptKeyFileType()
                .encrypt(encryptKey,
                        password,
                        encryptKeyAlgo,
                        encryptAlgo,
                        ENCRYPT_MODE);
    }

    @Override
    public void onClose(@NotNull IChannel channel) {
        enabled.set(false);
        activeLogonExchange.set(false);
        if(passwordManager != null && !strategiesEnabled.get()) passwordManager.poll();
        strategy.getOnCloseHandler().close();
        cancelFuture(heartbeatTimer);
        cancelFuture(testRequestTimer);
    }

    @Override
    public void close() {
        sendLogout();
        waitLogoutResponse();
        executorService.shutdown();

        try {
            if(!executorService.awaitTermination(settings.getDisconnectRequestDelay(), TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Failed to shutdown executor.");
                executorService.shutdownNow();
            }
        } catch (Exception e) {
            LOGGER.error("Error while closing handler executor service.", e);
        }
    }

    // <editor-fold desc="strategies definitions goes here.">

    // <editor-fold desc="send strategies definitions goes here."

    private CompletableFuture<MessageID> defaultSend(IChannel channel,
                                                     ByteBuf message,
                                                     Map<String, String> properties,
                                                     EventID eventID) {
        return channel.send(message, strategy.getState().enrichProperties(properties), eventID, SendMode.HANDLE_AND_MANGLE);
    }

    private CompletableFuture<MessageID> bulkSend(IChannel channel, ByteBuf message, Map<String, String> properties, EventID eventID) {
        resetHeartbeatTask();
        BatchSendConfiguration config = strategy.getBatchSendConfiguration();
        onOutgoingUpdateTag(message, properties);
        StrategyState strategyState = strategy.getState();

        strategyState.updateCacheAndRunOnCondition(message, x -> x >= config.getBatchSize(), buffer -> {
            try {
                LOGGER.info("Sending batch of size: {}", config.getBatchSize());
                channel.send(asExpandable(buffer),
                                strategy.getState().enrichProperties(properties),
                                eventID,
                                SendMode.DIRECT)
                    .thenAcceptAsync(strategyState::addMessageID, executorService);
            } catch (Exception e) {
                LOGGER.error("Error while sending batch.", e);
            }
            return Unit.INSTANCE;
        });

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<MessageID> splitSend(IChannel channel, ByteBuf message, Map<String, String> metadata, EventID eventID) {
        SplitSendConfiguration config = strategy.getSplitSendConfiguration();
        onOutgoingUpdateTag(message, metadata);
        List<ByteBuf> slices = new ArrayList<>();
        int numberOfSlices = config.getNumberOfParts() % (message.readableBytes() - 1);
        int sliceSize = message.readableBytes() / numberOfSlices;
        int nextSliceStart = 0;
        for (int i = 0; i < numberOfSlices - 1; i++) {
            slices.add(message.retainedSlice(message.readerIndex() + nextSliceStart, sliceSize));
            nextSliceStart += sliceSize;
        }
        int readerIndex = message.readerIndex();
        slices.add(message.retainedSlice(readerIndex + nextSliceStart, message.writerIndex() - nextSliceStart - readerIndex));

        long sleepTime = config.getTimeoutBetweenParts();
        List<Instant> sendingTimes = new ArrayList<>();
        for(ByteBuf slice : slices) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                LOGGER.error("Error while sending messages in different tcp packets.");
            }
            channel.send(asExpandable(slice),
                    strategy.getState().enrichProperties(metadata),
                    eventID,
                    SendMode.DIRECT_SOCKET);
            resetHeartbeatTask();
            sendingTimes.add(Instant.now());
        }


        String slicesTimestamps = sendingTimes.stream().map(formatter::format).collect(Collectors.joining(","));
        metadata.put(SPLIT_SEND_TIMESTAMPS_PROPERTY, slicesTimestamps);
        LOGGER.info("Sent message by slices: {}", slicesTimestamps);
        CompletableFuture<MessageID> messageID = channel.send(asExpandable(message),
                strategy.getState().enrichProperties(metadata),
                eventID,
                SendMode.DIRECT_MQ);
        messageID.thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
        return messageID;
    }

    // TODO: Add simplified configuration
    private void transformProcessor(
        ByteBuf message,
        Map<String, String> metadata
    ) {
        FixField msgTypeField = findField(message, MSG_TYPE_TAG, US_ASCII);
        if(msgTypeField == null || msgTypeField.getValue() == null) {
            return;
        }
        Set<String> disableForMessageTypes = strategy.getDisableForMessageTypes();
        if (disableForMessageTypes.contains(msgTypeField.getValue())) {
            LOGGER.info("Strategy '{}' is disabled for {} message type", strategy.getType(), msgTypeField.getValue());
            return;
        }

        TransformMessageConfiguration config = strategy.getTransformMessageConfiguration();
        TransformationConfiguration transformation = config.getNextTransformation();

        if(!msgTypeField.getValue().equals(transformation.getMessageType())) {
            if(!transformation.getAnyMessageType()) {
                config.decreaseCounter();
                return;
            }
        }

        var strategyState = strategy.getState();

        strategyState.transformIfCondition(
            x -> x <= config.getNumberOfTimesToTransform(),
            () -> {
                messageTransformer.transformWithoutResults(message, transformation.getCombinedActions());
                if(transformation.getNewPassword() != null) {
                    if(transformation.getEncryptKey() != null) {
                        FixField encryptedPassword = findField(message, ENCRYPTED_PASSWORD_TAG);
                        if(encryptedPassword != null) {
                            encryptedPassword.setValue(encrypt(transformation.getNewPassword(), transformation.getEncryptKey(), transformation.getPasswordEncryptAlgorithm(), transformation.getPasswordKeyEncryptAlgorithm()));
                        }
                    } else {
                        FixField encryptedPassword = findField(message, ENCRYPTED_PASSWORD_TAG);
                        if(encryptedPassword != null) {
                            encryptedPassword.clear();
                        }
                        FixField password = findField(message, PASSWORD_TAG);
                        if(password != null) {
                            password.setValue(transformation.getNewPassword());
                        } else {
                            FixField defaultAppl = findField(message, DEFAULT_APPL_VER_ID_TAG);
                            if(defaultAppl != null) {
                                defaultAppl.insertNext(PASSWORD_TAG, transformation.getNewPassword());
                            }
                        }
                    }
                }

                if(transformation.getNewNewPassword() != null) {
                    if(transformation.getEncryptKey() != null) {
                        FixField encryptedPassword = findField(message, NEW_ENCRYPTED_PASSWORD_TAG);
                        if(encryptedPassword != null) {
                            encryptedPassword.setValue(encrypt(transformation.getNewNewPassword(), transformation.getEncryptKey(), transformation.getPasswordEncryptAlgorithm(), transformation.getPasswordKeyEncryptAlgorithm()));
                        } else {
                            FixField defaultAppl = findField(message, DEFAULT_APPL_VER_ID_TAG);
                            if(defaultAppl != null) {
                                defaultAppl.insertNext(NEW_ENCRYPTED_PASSWORD_TAG, encrypt(transformation.getNewNewPassword(), transformation.getEncryptKey(), transformation.getPasswordEncryptAlgorithm(), transformation.getPasswordKeyEncryptAlgorithm()));
                            }
                        }
                    } else {
                        FixField encryptedPassword = findField(message, NEW_ENCRYPTED_PASSWORD_TAG);
                        if(encryptedPassword != null) {
                            encryptedPassword.clear();
                        }
                        FixField password = findField(message, NEW_PASSWORD_TAG);
                        if(password != null) {
                            password.setValue(transformation.getNewPassword());
                        } else {
                            FixField defaultAppl = findField(message, DEFAULT_APPL_VER_ID_TAG);
                            if(defaultAppl != null) {
                                defaultAppl.insertNext(NEW_PASSWORD_TAG, transformation.getNewPassword());
                            }
                        }
                    }
                }

                if(transformation.getUseOldPasswords() && !passwordManager.getPreviouslyUsedPasswords().isEmpty()) {
                    if(transformation.getEncryptKey() != null) {
                        FixField encryptedPassword = findField(message, ENCRYPTED_PASSWORD_TAG);
                        if(encryptedPassword != null) {
                            encryptedPassword.setValue(encrypt(getRandomOldPassword(), transformation.getEncryptKey(), transformation.getPasswordEncryptAlgorithm(), transformation.getPasswordKeyEncryptAlgorithm()));
                        }
                    } else {
                        FixField password = findField(message, PASSWORD_TAG);
                        if(password != null) {
                            password.setValue(getRandomOldPassword());
                        }
                    }
                }

                updateLength(message);
                if(transformation.getUpdateChecksum()) {
                    updateChecksum(message);
                }

                if(transformation.getComment() != null) {
                    metadata.put("transformationComment", transformation.getComment());
                }
                return Unit.INSTANCE;
            }
        );

    }

    private Map<String, String>
    blockSend(
        ByteBuf message,
        Map<String, String> metadata
    ) {
        long timeToBlock = strategy.getConfig().getDuration().toMillis() + 3000;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= timeToBlock) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                LOGGER.error("Error while blocking send.", e);
            }
        }
        return null;
    }
    // </editor-fold>

    // <editor-fold desc="incoming messages strategies definitions goes here."
    private Map<String, String> missIncomingMessages(ByteBuf message, Map<String, String> metadata) {
        int countToMiss = strategy.getMissIncomingMessagesConfig().getCount();
        var strategyState = strategy.getState();

        FixField msgType = findField(message, MSG_TYPE_TAG);

        if(msgType != null && Objects.equals(msgType.getValue(), MSG_TYPE_RESEND_REQUEST)) {
            handleResendRequest(message);
            return metadata;
        }

        if(!strategyState.updateMissedIncomingMessagesCountIfCondition(x -> x <= countToMiss)) {
            return null;
        }
        resetTestRequestTask();
        metadata.put(REJECT_REASON, "Missed incoming message due to `miss incoming messages` strategy");
        return metadata;
    }

    private Map<String, String> missTestRequest(ByteBuf message, Map<String, String> metadata) {
        metadata.put(REJECT_REASON, "Missed incoming test request because due to `miss test request` strategy.");
        return metadata;
    }

    private Map<String, String> logoutOnLogon(ByteBuf message, Map<String, String> metadata) {
        StrategyState state = strategy.getState();
        TransformMessageConfiguration config = state.getConfig().getTransformMessageConfiguration();
        if(state.getTransformedIncomingMessagesCount() < config.getNumberOfTimesToTransform()) {
            handleLogon(message, metadata);
            try {
                disconnect(strategy.getGracefulDisconnect());
                if(!channel.isOpen()) channel.open().get();
            } catch (Exception e) {
                LOGGER.error("Error while reconnecting.", e);
            }
        } else {
            handleLogon(message, metadata);
        }
        return metadata;
    }
    // </editor-fold>

    // <editor-fold desc="outgoing strategies"

    private Map<String, String> defaultOutgoingStrategy(ByteBuf message, Map<String, String> metadata) {
        onOutgoingUpdateTag(message, metadata);
        return null;
    }

    private Map<String, String> transformOutgoingMessageStrategy(ByteBuf message, Map<String, String> metadata) {
        onOutgoingUpdateTag(message, metadata);
        transformProcessor(message, metadata);
        return null;
    }

    private Map<String, String> fakeRetransmissionOutgoingProcessor(ByteBuf message, Map<String, String> metadata) {
        onOutgoingUpdateTag(message, metadata);

        Set<String> disableForMessageTypes = strategy.getDisableForMessageTypes();
        FixField msgTypeField = findField(message, MSG_TYPE_TAG, US_ASCII);
        if(msgTypeField != null && msgTypeField.getValue() != null && disableForMessageTypes.contains(msgTypeField.getValue())) {
            LOGGER.info("Strategy '{}' is disabled for {} message type", strategy.getType(), msgTypeField.getValue());
            return null;
        }

        FixField sendingTime = requireNonNull(findField(message, SENDING_TIME_TAG));
        strategy.getState().addMissedMessageToCacheIfCondition(msgSeqNum.get(), message.copy(), x -> true);

        sendingTime
            .insertNext(ORIG_SENDING_TIME_TAG, sendingTime.getValue())
            .insertNext(POSS_DUP_TAG, IS_POSS_DUP)
            .insertNext(POSS_RESEND_TAG, IS_POSS_DUP);
        updateLength(message);
        updateChecksum(message);

        return null;
    }

    private Map<String, String> gapFillSequenceReset(ByteBuf message, Map<String, String> metadata) {
        ChangeSequenceConfiguration resendRequestConfig = strategy.getConfig().getChangeSequenceConfiguration();
        onOutgoingUpdateTag(message, metadata);
        FixField msgType = findField(message, MSG_TYPE_TAG, US_ASCII);

        if(msgType == null || !msgType.getValue().equals(MSG_TYPE_SEQUENCE_RESET)) return null;

        if(resendRequestConfig.getGapFill()) return null;

        FixField gapFill = findField(message, GAP_FILL_FLAG_TAG, US_ASCII);

        if(gapFill == null) return null;

        gapFill.setValue("N");

        return null;
    }

    private Map<String, String> missOutgoingMessages(ByteBuf message, Map<String, String> metadata) {
        int countToMiss = strategy.getMissOutgoingMessagesConfiguration().getCount();
        var strategyState = strategy.getState();
        onOutgoingUpdateTag(message, metadata);
        if(!strategyState.addMissedMessageToCacheIfCondition(msgSeqNum.get(), message.copy(), x -> x <= countToMiss)) {
            return null;
        }

        message.clear();

        return null;
    }

    private Map<String, String> missHeartbeatsAndTestRequestReplies(ByteBuf message, Map<String, String> metadata) {
        return missHeartbeats(message, metadata, true);
    }

    private Map<String, String> missHeartbeats(ByteBuf message, Map<String, String> metadata) {
        return missHeartbeats(message, metadata, false);
    }

    private Map<String, String> missHeartbeats(ByteBuf message, Map<String, String> metadata, boolean skipTestRequestReplies) {
        onOutgoingUpdateTag(message, metadata);
        FixField msgTypeField = findField(message, MSG_TYPE_TAG, US_ASCII);
        if(msgTypeField == null || msgTypeField.getValue() == null) {
            return null;
        }

        String msgType = msgTypeField.getValue();

        strategy.getState().addMissedMessageToCacheIfCondition(msgSeqNum.get(), message.copy(), x -> true);
        if(msgType.equals(MSG_TYPE_HEARTBEAT)) {
            FixField testReqId = findField(message, TEST_REQ_ID_TAG);
            if(testReqId != null && testReqId.getValue() != null && !skipTestRequestReplies) {
                return null;
            }
            message.clear();
            return null;
        } else {
            message.clear();
        }

        return null;
    }
    // </editor-fold>

    // <editor-fold desc="receive strategies"
    private Map<String, String> blockReceiveQueue(ByteBuf message, Map<String, String> metadata) {
        long timeToBlock = strategy.getConfig().getDuration().toMillis();
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= timeToBlock) {
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                LOGGER.error("Error while blocking receive.", e);
            }
        }
        return null;
    }
    // </editor-fold>

    // <editor-fold desc="strategy setup and cleanup">
    private StatefulStrategy defaultStrategyHolder() {
        var receiveStrategy = new ReceiveStrategy((msg, mtd) -> null);
        var receiveStrategyCopy = new ReceiveStrategy((msg, mtd) -> null);
        var sendStrategy = new SendStrategy(this::defaultMessageProcessor, this::defaultSend);
        var sendStrategyCopy = new SendStrategy(this::defaultMessageProcessor, this::defaultSend);
        var incomingMessagesStrategy = new IncomingMessagesStrategy(
            this::defaultMessageProcessor, this::handleTestRequest, this::handleLogon, this::handleLogout
        );
        var incomingMessagesStrategyCopy = new IncomingMessagesStrategy(
            this::defaultMessageProcessor, this::handleTestRequest, this::handleLogon, this::handleLogout
        );
        var outgoingMessagesStrategy = new OutgoingMessagesStrategy(this::defaultOutgoingStrategy);
        var outgoingMessagesStrategyCopy = new OutgoingMessagesStrategy(this::defaultOutgoingStrategy);
        return new StatefulStrategy(
            sendStrategy,
            incomingMessagesStrategy,
            outgoingMessagesStrategy,
            receiveStrategy,
            this::defaultCleanupHandler,
            this::recoveryFromState,
            this::defaultOnCloseHandler,
            new DefaultStrategyHolder(
                sendStrategyCopy,
                incomingMessagesStrategyCopy,
                outgoingMessagesStrategyCopy,
                receiveStrategyCopy,
                this::defaultCleanupHandler,
                this::recoveryFromState,
                this::defaultOnCloseHandler
            )
        );
    }

    private void setupFakeRetransmissionStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::fakeRetransmissionOutgoingProcessor); return Unit.INSTANCE;});
        strategy.setCleanupHandler(this::cleanupFakeRetransmissionStrategy);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupFakeRetransmissionStrategy() {
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::defaultOutgoingStrategy); return Unit.INSTANCE;});
        strategy.cleanupStrategy();
        ruleEndEvent(strategy.getType(), strategy.getState().getStartTime(), strategy.getState().getMessageIDs());
    }

    private void runLogonAfterLogonStrategy(RuleConfiguration configuration) {
        Instant start = Instant.now();
        strategy.resetStrategyAndState(configuration);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
        if(!enabled.get()) {
            ruleErrorEvent(strategy.getType(), String.format("Session %s isn't logged in.", channel.getSessionAlias()), null);
            return;
        }

        try {
            sendLogon();
            msgSeqNum.incrementAndGet();
        } catch (Exception e) {
            ruleErrorEvent(strategy.getType(), null, e);
        }
        ruleEndEvent(configuration.getRuleType(), start, strategy.getState().getMessageIDs());
    }

    private void runPossDupSessionMessages(RuleConfiguration configuration) {
        Instant start = Instant.now();
        strategy.resetStrategyAndState(configuration);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
        if(!enabled.get()) {
            ruleErrorEvent(strategy.getType(), String.format("Session %s isn't logged in.", channel.getSessionAlias()), null);
            return;
        }

        sendResendRequest(serverMsgSeqNum.get() - 2, serverMsgSeqNum.get(), true);
        sendHeartbeatWithPossDup(true);
        sendTestRequestWithPossDup(true);
        sendLogout(true);
        ruleEndEvent(configuration.getRuleType(), start, strategy.getState().getMessageIDs());
    }

    private OnCloseHandler getRunLogonFromAnotherConnectionOnCloseHandler(AtomicBoolean sessionDisconnected) {
        return () -> sessionDisconnected.set(true);
    }

    private void runLogonFromAnotherConnection(RuleConfiguration configuration) {
        Instant start = Instant.now();
        strategy.resetStrategyAndState(configuration);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
        if(!enabled.get()) {
            ruleErrorEvent(strategy.getType(), String.format("Session %s isn't logged in.", channel.getSessionAlias()), null);
            return;
        }
        AtomicBoolean isMainSessionDisconnected = new AtomicBoolean(false);
        strategy.setOnCloseHandler(getRunLogonFromAnotherConnectionOnCloseHandler(isMainSessionDisconnected));

        Map<String, String> props = new HashMap<>();
        StringBuilder logon = buildLogon(props);
        props.put("sentUsingAnotherSocket", "True");
        ByteBuf logonBuf = Unpooled.wrappedBuffer(logon.toString().getBytes(StandardCharsets.UTF_8));

        channel.send(logonBuf, strategy.getState().enrichProperties(props), null, SendMode.DIRECT_MQ)
            .thenAcceptAsync(x -> {
                strategy.getState().addMessageID(x);
            }, executorService);

        boolean logonSent = false;
        boolean responseReceived = true;
        boolean sessionDisconnected = false;

        try(
            Socket socket = new Socket(address.getAddress(), address.getPort());
            DataOutputStream dOut = new DataOutputStream(socket.getOutputStream());
            DataInputStream dIn = new DataInputStream(socket.getInputStream());
        ){
            socket.setSoTimeout(5000);

            byte[] logonByteArray = new byte[logonBuf.readableBytes()];
            logonBuf.readBytes(logonByteArray);
            dOut.write(logonByteArray);
            logonSent = true;


            try {
                byte[] buffer = new byte[1024];
                int read = dIn.read(buffer);

                if (read == -1) {
                    responseReceived = false;
                    sessionDisconnected = true;
                } else {
                    responseReceived = true;
                    LOGGER.warn("Received response while connecting with the same compId and there is live session for this compId. {}", new String(buffer, StandardCharsets.UTF_8));
                }
            } catch (SocketTimeoutException e) {
                responseReceived = false;
            }

        } catch (IOException e) {
            LOGGER.error("Error while connecting from another socket to the same user.", e);
            responseReceived = false;
        }

        try {
            LOGGER.info("Waiting for 5 seconds to check if main session will be disconnected.");
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        HashMap<String, Object> additionalDetails = new HashMap<>();
        additionalDetails.put("logonFromAnotherSocketSent", logonSent);
        additionalDetails.put("responseForLogonInAnotherSessionReceived", responseReceived);
        additionalDetails.put("anotherSocketSessionDisconnectedAfterLogon", sessionDisconnected);
        additionalDetails.put("isMainSessionDisconnected", isMainSessionDisconnected.get());
        additionalDetails.put("type", "logon_from_another_socket");
        ruleEndEvent(configuration.getRuleType(), start, strategy.getState().getMessageIDs(), additionalDetails);
    }

    private void setupDisconnectStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.updateSendStrategy(x -> {x.setSendPreprocessor(this::blockSend); return Unit.INSTANCE; });
        strategy.setCleanupHandler(this::cleanupDisconnectStrategy);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
        try {
            disconnect(configuration.getGracefulDisconnect());
        } catch (Exception e) {
            String message = String.format("Error while setting up %s", strategy.getType());
            LOGGER.error(message, e);
            context.send(CommonUtil.toErrorEvent(message, e), strategyRootEvent);
        }
    }

    private void cleanupDisconnectStrategy() {
        var state = strategy.getState();
        strategy.updateSendStrategy(x -> {x.setSendPreprocessor(this::defaultMessageProcessor); return Unit.INSTANCE;});
        try {
            openChannelAndWaitForLogon();
            Thread.sleep(strategy.getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s", strategy.getType());
            LOGGER.error(message, e);
            context.send(CommonUtil.toErrorEvent(message, e), strategyRootEvent);
        }
        ruleEndEvent(strategy.getType(), state.getStartTime(), strategy.getState().getMessageIDs());
    }

    private void setupIgnoreIncomingMessagesStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        try {
            disconnect(configuration.getGracefulDisconnect());
            openChannelAndWaitForLogon();
        } catch (Exception e) {
            String message = String.format("Error while setup %s strategy.", configuration.getRuleType());
            LOGGER.error(message, e);
            context.send(toErrorEvent(message, e), strategyRootEvent);
        }
        strategy.updateIncomingMessageStrategy(x -> {x.setIncomingMessagesPreprocessor(this::missIncomingMessages); return Unit.INSTANCE;});
        strategy.setCleanupHandler(this::cleanupIgnoreIncomingMessagesStrategy);

        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupIgnoreIncomingMessagesStrategy() {
        strategy.updateIncomingMessageStrategy(x -> {x.setIncomingMessagesPreprocessor(this::defaultMessageProcessor); return Unit.INSTANCE;});
        try {
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis()); // waiting for new incoming messages to trigger resend request.
            disconnect(strategy.getGracefulDisconnect());
            openChannelAndWaitForLogon();
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
        strategy.cleanupStrategy();
    }

    private void setupTransformStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.updateIncomingMessageStrategy(x -> {x.setLogonStrategy(this::logoutOnLogon); return Unit.INSTANCE;});
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::transformOutgoingMessageStrategy); return Unit.INSTANCE;});
        strategy.setCleanupHandler(this::cleanupTransformStrategy);
        try {
            disconnect(configuration.getGracefulDisconnect());
            if(!channel.isOpen()) channel.open().get();
        } catch (Exception e) {
            String message = String.format("Error while setting up %s", strategy.getType());
            LOGGER.error(message, e);
            context.send(CommonUtil.toErrorEvent(message, e), strategyRootEvent);
        }
        ruleStartEvent(strategy.getType(), strategy.getStartTime());
    }

    private void cleanupTransformStrategy() {
        strategy.updateIncomingMessageStrategy(x -> {x.setLogonStrategy(this::handleLogon); return Unit.INSTANCE;});
        strategy.updateSendStrategy(x -> {x.setSendPreprocessor(this::defaultMessageProcessor); return Unit.INSTANCE;});
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::defaultOutgoingStrategy); return Unit.INSTANCE;});
        strategy.updateIncomingMessageStrategy(x -> {x.setLogoutStrategy(this::handleLogout); return Unit.INSTANCE;});
        try {
            disconnect(strategy.getGracefulDisconnect());
            openChannelAndWaitForLogon();
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message);
        }
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
        strategy.cleanupStrategy();
    }

    private void setupTransformMessageStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::transformOutgoingMessageStrategy); return Unit.INSTANCE;});
        strategy.setCleanupHandler(this::cleanupTransformMessageStrategy);
        ruleStartEvent(strategy.getType(), strategy.getStartTime());
    }

    private void cleanupTransformMessageStrategy() {
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::defaultOutgoingStrategy); return Unit.INSTANCE;});
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
        strategy.cleanupStrategy();
    }

    private void setupBidirectionalResendRequestStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.setCleanupHandler(this::cleanupBidirectionalResendRequestStrategy);
        strategy.setRecoveryHandler(this::recoveryFromState);
        try {
            disconnect(configuration.getGracefulDisconnect());
            openChannelAndWaitForLogon();
        } catch (Exception e) {
            String message = String.format("Error while setup %s strategy.", strategy.getType());
            LOGGER.error(message, e);
            context.send(toErrorEvent(message, e), strategyRootEvent);
        }
        strategy.setOnCloseHandler(this::outageOnCloseHandler);
        strategy.updateIncomingMessageStrategy(x -> {x.setIncomingMessagesPreprocessor(this::missIncomingMessages); return Unit.INSTANCE;});
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::missOutgoingMessages); return Unit.INSTANCE;});
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupBidirectionalResendRequestStrategy() {
        strategy.updateIncomingMessageStrategy(x -> {x.setIncomingMessagesPreprocessor(this::defaultMessageProcessor); return Unit.INSTANCE;});
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::defaultOutgoingStrategy); return Unit.INSTANCE;});
        try {
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis()); // waiting for new incoming/outgoing messages to trigger resend request.
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
    }

    private void setupOutgoingGapStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.setRecoveryHandler(this::recoveryFromState);
        strategy.setCleanupHandler(this::cleanupOutgoingGapStrategy);
        try {
            disconnect(configuration.getGracefulDisconnect());
            openChannelAndWaitForLogon();
        } catch (Exception e) {
            String message = String.format("Error while setup %s strategy.", strategy.getType());
            LOGGER.error(message, e);
            context.send(toErrorEvent(message, e), strategyRootEvent);
        }
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::missOutgoingMessages); return Unit.INSTANCE;});
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupOutgoingGapStrategy() {
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::defaultOutgoingStrategy); return Unit.INSTANCE;});
        try {
            disconnect(strategy.getGracefulDisconnect());
            openChannelAndWaitForLogon();
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
        strategy.cleanupStrategy();
    }

    private void setupClientOutageStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.setCleanupHandler(this::cleanupClientOutageStrategy);
        strategy.setOnCloseHandler(this::outageOnCloseHandler);
        strategy.setRecoveryHandler(this::recoveryFromState);
        strategy.updateIncomingMessageStrategy(x -> {x.setTestRequestProcessor(this::missTestRequest); return Unit.INSTANCE;});
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::missHeartbeatsAndTestRequestReplies); return Unit.INSTANCE;});
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupClientOutageStrategy() {
        strategy.updateOutgoingMessageStrategy(x -> { x.setOutgoingMessageProcessor(this::defaultOutgoingStrategy); return Unit.INSTANCE;});
        strategy.updateIncomingMessageStrategy(x -> { x.setTestRequestProcessor(this::handleTestRequest); return Unit.INSTANCE;});
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
        strategy.cleanupStrategy();
    }

    private void setupPartialClientOutageStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.setOnCloseHandler(this::outageOnCloseHandler);
        strategy.setCleanupHandler(this::cleanupPartialClientOutageStrategy);
        strategy.setRecoveryHandler(this::recoveryFromState);
        strategy.updateOutgoingMessageStrategy(x -> {x.setOutgoingMessageProcessor(this::missHeartbeats); return Unit.INSTANCE;});
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupPartialClientOutageStrategy() {
        strategy.updateOutgoingMessageStrategy(x -> { x.setOutgoingMessageProcessor(this::defaultOutgoingStrategy); return Unit.INSTANCE;});
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
        strategy.cleanupStrategy();
    }

    private void runResendRequestStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        Instant start = Instant.now();
        ruleStartEvent(configuration.getRuleType(), start);
        ResendRequestConfiguration resendRequestConfig = configuration.getResendRequestConfiguration();
        int msgCount = resendRequestConfig.getMessageCount();
        int currentSeq = serverMsgSeqNum.get();
        try {
            if(resendRequestConfig.getSingle()) {
                sendResendRequest(currentSeq - 1, currentSeq - 1);
            }
            if(resendRequestConfig.getRange()) {
                sendResendRequest(currentSeq - msgCount, currentSeq);
            }
            if(resendRequestConfig.getUntilLast()) {
                sendResendRequest(currentSeq - msgCount, 0);
            }
            if(resendRequestConfig.getFutureResendRequest()) {
                sendResendRequest(currentSeq + 1, currentSeq + msgCount);
            }
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(configuration.getRuleType(), start, strategy.getState().getMessageIDs());
    }

    private void setupSlowConsumerStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.updateReceiveMessageStrategy(x -> {x.setReceivePreprocessor(this::blockReceiveQueue); return Unit.INSTANCE;});
        strategy.setCleanupHandler(this::cleanupSlowConsumerStrategy);
        strategy.setOnCloseHandler(this::outageOnCloseHandler);
    }

    private void cleanupSlowConsumerStrategy() {
        strategy.updateReceiveMessageStrategy(x -> {x.setReceivePreprocessor(this::defaultMessageProcessor); return Unit.INSTANCE;});
        try {
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
        strategy.cleanupStrategy();
    }

    private void runReconnectWithSequenceResetStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        Instant start = Instant.now();
        ruleStartEvent(configuration.getRuleType(), start);
        strategy.updateOutgoingMessageStrategy(x -> { x.setOutgoingMessageProcessor(this::gapFillSequenceReset); return Unit.INSTANCE;});

        ChangeSequenceConfiguration resendRequestConfig = configuration.getChangeSequenceConfiguration();

        try {
            disconnect(configuration.getGracefulDisconnect());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }

        if(resendRequestConfig.getChangeIncomingSequence()) {
            if(resendRequestConfig.getChangeUp()) {
                serverMsgSeqNum.set(serverMsgSeqNum.get() + resendRequestConfig.getMessageCount());
            } else {
                serverMsgSeqNum.set(serverMsgSeqNum.get() - resendRequestConfig.getMessageCount());
            }
        } else {
            if(resendRequestConfig.getChangeUp()) {
                msgSeqNum.set(msgSeqNum.get() + resendRequestConfig.getMessageCount());
            } else {
                msgSeqNum.set(msgSeqNum.get() - resendRequestConfig.getMessageCount());
            }
        }

        try {
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
            openChannelAndWaitForLogon();
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        strategy.cleanupStrategy();
        ruleEndEvent(configuration.getRuleType(), start, strategy.getState().getMessageIDs());
    }

    private void sendSequenceReset(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        Instant start = Instant.now();
        SendSequenceResetConfiguration config = configuration.getSendSequenceResetConfiguration();

        StringBuilder sequenceReset = new StringBuilder();
        String time = getTime();
        setHeader(sequenceReset, MSG_TYPE_SEQUENCE_RESET, msgSeqNum.incrementAndGet(), time);
        sequenceReset.append(ORIG_SENDING_TIME).append(time);
        if(config.getChangeUp()) {
            int seqNum = msgSeqNum.get();
            sequenceReset.append(NEW_SEQ_NO).append(seqNum + 5);
            msgSeqNum.set(seqNum + 5);
        } else {
            sequenceReset.append(NEW_SEQ_NO).append(msgSeqNum.get() - 5);
        }
        setChecksumAndBodyLength(sequenceReset);

        channel.send(Unpooled.wrappedBuffer(sequenceReset.toString().getBytes(StandardCharsets.UTF_8)),
                        strategy.getState().enrichProperties(),
                        null,
                        SendMode.HANDLE_AND_MANGLE)
            .thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
        resetHeartbeatTask();
        strategy.cleanupStrategy();
        ruleEndEvent(configuration.getRuleType(), start, strategy.getState().getMessageIDs());
    }

    private void setupBatchSendStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.updateSendStrategy(x -> {x.setSendHandler(this::bulkSend); return Unit.INSTANCE;});
        strategy.setCleanupHandler(this::cleanupBatchSendStrategy);
        ruleStartEvent(strategy.getType(), strategy.getStartTime());
    }

    private void cleanupBatchSendStrategy() {
        var state = strategy.getState();
        strategy.updateSendStrategy(x -> {
            state.executeOnBatchCacheIfCondition(size -> size > 0, message -> {
                try {
                    channel.send(message, strategy.getState().enrichProperties(), null, SendMode.DIRECT)
                        .thenAcceptAsync(messageID -> strategy.getState().addMessageID(messageID), executorService);
                } catch (Exception e) {
                    LOGGER.error("Error while sending batch.", e);
                }
                return Unit.INSTANCE;
            });
            x.setSendHandler(this::defaultSend);
            return Unit.INSTANCE;
        });
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
        strategy.cleanupStrategy();
    }

    private void setupSplitSendStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.updateSendStrategy(x -> {x.setSendHandler(this::splitSend); return Unit.INSTANCE;});
        strategy.setCleanupHandler(this::cleanupSplitSendStrategy);
        ruleStartEvent(strategy.getType(), strategy.getStartTime());
    }

    private void cleanupSplitSendStrategy() {
        strategy.updateSendStrategy(x -> {x.setSendHandler(this::defaultSend); return Unit.INSTANCE;});
        ruleEndEvent(strategy.getType(), strategy.getStartTime(), strategy.getState().getMessageIDs());
        strategy.cleanupStrategy();
    }
    // </editor-fold>

    private Map<String, String> defaultMessageProcessor(ByteBuf message, Map<String, String> metadata) {return null;}
    private void defaultCleanupHandler() {}

    // <editor-fold desc="strategies scheduling and cleanup">
    private void applyNextStrategy() {
        LOGGER.info("Cleaning up current strategy {}", strategy.getState().getType());
        LOGGER.info("Started waiting for recovery finish.");
        while (activeRecovery.get()) {
            LOGGER.debug("Waiting for recovery to finish.");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("Error while waiting for recovery to finish", e);
            }
        }
        LOGGER.info("Stopped waiting for recovery finish.");
        try {
            strategy.getCleanupHandler().cleanup();
        } catch (Exception e) {
            String message = String.format("Error while cleaning up strategy: %s", strategy.getState().getType());
            LOGGER.error(message, e);
            ruleErrorEvent(strategy.getState().getType(), null, e);
        }

        if(!sessionActive.get()) {
            strategy.resetStrategyAndState(RuleConfiguration.Companion.defaultConfiguration());
            executorService.schedule(this::applyNextStrategy, Duration.of(10, ChronoUnit.MINUTES).toMinutes(), TimeUnit.MINUTES);
            return;
        }

        if(!strategiesEnabled.get()) {
            LOGGER.info("Strategies disabled. New strategy will not be applied. Trying again in 30 seconds.");
            strategy.resetStrategyAndState(RuleConfiguration.Companion.defaultConfiguration());
            executorService.schedule(this::applyNextStrategy, Duration.of(30, ChronoUnit.SECONDS).toSeconds(), TimeUnit.SECONDS);
            return;
        }

        RuleConfiguration nextStrategyConfig = scheduler.next();
        Consumer<RuleConfiguration> nextStrategySetupFunction = getSetupFunction(nextStrategyConfig);
        try {
            nextStrategySetupFunction.accept(nextStrategyConfig);
        } catch (Exception e) {
            String message = String.format("Error while setting up strategy: %s", strategy.getState().getType());
            LOGGER.error(message, e);
            ruleErrorEvent(nextStrategyConfig.getRuleType(), null, e);
        }

        LOGGER.info("Next strategy applied: {}", nextStrategyConfig.getRuleType());
        executorService.schedule(this::applyNextStrategy, nextStrategyConfig.getDuration().toMillis(), TimeUnit.MILLISECONDS);
    }

    private Consumer<RuleConfiguration> getSetupFunction(RuleConfiguration config) {
        switch (config.getRuleType()) {
            case BATCH_SEND: return this::setupBatchSendStrategy;
            case BI_DIRECTIONAL_RESEND_REQUEST: return this::setupBidirectionalResendRequestStrategy;
            case SPLIT_SEND: return this::setupSplitSendStrategy;
            case CLIENT_OUTAGE: return this::setupClientOutageStrategy;
            case SLOW_CONSUMER: return this::setupSlowConsumerStrategy;
            case RESEND_REQUEST: return this::runResendRequestStrategy;
            case SEQUENCE_RESET: return this::runReconnectWithSequenceResetStrategy;
            case SEND_SEQUENCE_RESET: return this::sendSequenceReset;
            case TRANSFORM_LOGON: return this::setupTransformStrategy;
            case TRANSFORM_MESSAGE_STRATEGY:
            case INVALID_CHECKSUM:
                return this::setupTransformMessageStrategy;
            case CREATE_OUTGOING_GAP: return this::setupOutgoingGapStrategy;
            case PARTIAL_CLIENT_OUTAGE: return this::setupPartialClientOutageStrategy;
            case IGNORE_INCOMING_MESSAGES: return this::setupIgnoreIncomingMessagesStrategy;
            case DISCONNECT_WITH_RECONNECT: return this::setupDisconnectStrategy;
            case FAKE_RETRANSMISSION: return this::setupFakeRetransmissionStrategy;
            case LOGON_AFTER_LOGON: return this::runLogonAfterLogonStrategy;
            case POSS_DUP_SESSION_MESSAGES: return this::runPossDupSessionMessages;
            case LOGON_FROM_ANOTHER_CONNECTION: return this::runLogonFromAnotherConnection;
            case DEFAULT: return configuration -> strategy.cleanupStrategy();
            default: throw new IllegalStateException(String.format("Unknown strategy type %s.", config.getRuleType()));
        }
    }

    // </editor-fold>

    // <editor-fold desc="recovery">
    private void recoveryFromState(Integer beginSeqNo, Integer endSeqNo) {
        if (endSeqNo == 0) {
            endSeqNo = msgSeqNum.get() + 1;
        }

        StrategyState state = strategy.getState();
        RecoveryConfig recoveryConfig = strategy.getRecoveryConfig();

        LOGGER.info("Making recovery from state: {} - {}.", beginSeqNo, endSeqNo);

        boolean skip = recoveryConfig.getOutOfOrder();
        ByteBuf skipped = null;

        for(int i = beginSeqNo; i <= endSeqNo; i++) {
            var missedMessage = state.getMissedMessage(i);
            if(missedMessage == null) {
                recovery(i, endSeqNo, recoveryConfig);
                break;
            } else {
                FixField msgType = findField(missedMessage, MSG_TYPE_TAG);
                if(recoveryConfig.getSequenceResetForAdmin() && msgType == null || ADMIN_MESSAGES.contains(msgType.getValue())) {
                    int newSeqNo = i == endSeqNo ? msgSeqNum.get() + 1 : i + 1;
                    StringBuilder seqReset = createSequenceReset(i, newSeqNo);

                    channel.send(
                        Unpooled.wrappedBuffer(seqReset.toString().getBytes(StandardCharsets.UTF_8)),
                            strategy.getState().enrichProperties(), null, SendMode.MANGLE
                    ).thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
                } else {
                    FixField possDup = findField(missedMessage, POSS_DUP_TAG);
                    if(possDup == null || !Objects.equals(possDup.getValue(), IS_POSS_DUP)) {
                        setPossDup(missedMessage);
                        setTime(missedMessage);
                    } else {
                        updateSendingTime(missedMessage);
                    }
                    FixField possResend = findField(missedMessage, POSS_RESEND_TAG);
                    if(possResend != null && Objects.equals(possResend.getValue(), IS_POSS_DUP)) {
                        possResend.clear();
                    }
                    updateLength(missedMessage);
                    updateChecksum(missedMessage);

                    LOGGER.info("Sending recovery message from state: {}", missedMessage.toString(US_ASCII));
                    if(!skip) {
                        channel.send(missedMessage, strategy.getState().enrichProperties(), null, SendMode.MANGLE)
                            .thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
                        try {
                            Thread.sleep(settings.getRecoverySendIntervalMs());
                        } catch (InterruptedException e) {
                            LOGGER.error("Error while waiting send interval during recovery", e);
                        }
                    }

                    if(skip && recoveryConfig.getOutOfOrder()) {
                        skip = false;
                        skipped = missedMessage;
                    }

                    if(!skip && recoveryConfig.getOutOfOrder()) {
                        channel.send(skipped, strategy.getState().enrichProperties(), null, SendMode.MANGLE)
                            .thenAcceptAsync(x -> strategy.getState().addMessageID(x), executorService);
                        try {
                            Thread.sleep(settings.getRecoverySendIntervalMs());
                        } catch (InterruptedException e) {
                            LOGGER.error("Error while waiting send interval during recovery", e);
                        }
                        skip = true;
                    }
                }
            }
        }
    }

    // </editor-fold">

    // </editor-fold>

    // <editor-fold desc="utility">

    private void defaultOnCloseHandler() {}

    private void outageOnCloseHandler() {
        CleanupHandler cleanup = strategy.getCleanupHandler();
        strategy.setOnCloseHandler(this::defaultOnCloseHandler);
        strategy.setCleanupHandler(this::defaultCleanupHandler);
        cleanup.cleanup();
    }

    private StringBuilder createSequenceReset(int seqNo, int newSeqNo) {
        StringBuilder sequenceReset = new StringBuilder();
        String time = getTime();
        setHeader(sequenceReset, MSG_TYPE_SEQUENCE_RESET, seqNo, null);
        sequenceReset.append(ORIG_SENDING_TIME).append(time);
        sequenceReset.append(POSS_DUP).append(IS_POSS_DUP);
        sequenceReset.append(GAP_FILL_FLAG).append("Y");
        sequenceReset.append(NEW_SEQ_NO).append(newSeqNo);
        setChecksumAndBodyLength(sequenceReset);
        return sequenceReset;
    }

    private void setPossDup(ByteBuf buf) {
        FixField sendingTime = requireNonNull(findField(buf, SENDING_TIME_TAG));
        sendingTime.insertNext(POSS_DUP_TAG, IS_POSS_DUP);
    }

    private void updateSendingTime(ByteBuf buf) {
        FixField sendingTime = Objects.requireNonNull(findField(buf, SENDING_TIME_TAG));
        sendingTime.setValue(getTime());
    }

    private void setTime(ByteBuf buf) {
        FixField sendingTime = findField(buf, SENDING_TIME_TAG);
        FixField seqNum = requireNonNull(findField(buf, MSG_SEQ_NUM_TAG), "SeqNum field was null.");

        String time = getTime();
        if (sendingTime == null) {
            seqNum.insertNext(SENDING_TIME_TAG, time).insertNext(ORIG_SENDING_TIME_TAG, time);
        } else {
            String value = sendingTime.getValue();

            if (value == null || value.isEmpty() || value.equals("null")) {
                sendingTime.setValue(time);
                sendingTime.insertNext(ORIG_SENDING_TIME_TAG, time);
            } else {
                sendingTime.setValue(time);
                sendingTime.insertNext(ORIG_SENDING_TIME_TAG, value);
            }
        }
    }

    private void disconnect(boolean graceful) throws ExecutionException, InterruptedException {
        LOGGER.info("Started waiting for recovery finish.");
        while (activeRecovery.get()) {
            LOGGER.debug("Waiting for recovery to finish.");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("Error while waiting for recovery to finish", e);
            }
        }
        LOGGER.info("Finished waiting for recovery finish.");
        if(graceful) {
            sendLogout();
            waitLogoutResponse();
        }
        while (activeRecovery.get()) {
            LOGGER.debug("Waiting for recovery to finish.");
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("Error while waiting for recovery to finish", e);
            }
        }
        enabled.set(false);
        activeLogonExchange.set(false);
        resetHeartbeatTask();
        resetTestRequestTask();
        Thread.sleep(settings.getDisconnectCleanUpTimeoutMs());
        channel.close().get();
    }

    private void openChannelAndWaitForLogon() throws ExecutionException, InterruptedException {
        if(!channel.isOpen()) channel.open().get();
        waitUntilLoggedIn();
    }

    private void waitUntilLoggedIn() {
        long start = System.currentTimeMillis();
        while (!enabled.get() && System.currentTimeMillis() - start < 2000) {
            LOGGER.info("Waiting until session will be logged in: {}", channel.getSessionAlias());
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                LOGGER.error("Error while waiting session login.", e);
            }
        }
    }

    private void waitLogoutResponse() {
        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() - start < settings.getDisconnectRequestDelay() && enabled.get()) {
            if (LOGGER.isWarnEnabled()) LOGGER.warn("Waiting session logout: {}", channel.getSessionAlias());
            try {
                //noinspection BusyWait
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("Error while sleeping.");
            }
        }
    }

    private void setHeader(StringBuilder stringBuilder, String msgType, Integer seqNum, String time) {
        setHeader(stringBuilder, msgType, seqNum, time, false);
    }

    private void setHeader(StringBuilder stringBuilder, String msgType, Integer seqNum, String time, boolean isPossDup) {
        stringBuilder.append(BEGIN_STRING_TAG).append("=").append(settings.getBeginString());
        stringBuilder.append(MSG_TYPE).append(msgType);
        stringBuilder.append(MSG_SEQ_NUM).append(seqNum);
        if (settings.getSenderCompID() != null) stringBuilder.append(SENDER_COMP_ID).append(settings.getSenderCompID());
        if (settings.getTargetCompID() != null) stringBuilder.append(TARGET_COMP_ID).append(settings.getTargetCompID());
        if (settings.getSenderSubID() != null) stringBuilder.append(SENDER_SUB_ID).append(settings.getSenderSubID());
        stringBuilder.append(SENDING_TIME);
        String now = getTime();
        if(time != null) {
            stringBuilder.append(time);
            now = time;
        } else {
            stringBuilder.append(now);
        }
        if(isPossDup) {
            stringBuilder.append(ORIG_SENDING_TIME).append(now);
            stringBuilder.append(POSS_DUP).append(IS_POSS_DUP);
        }
    }

    private void setChecksumAndBodyLength(StringBuilder stringBuilder) {
        stringBuilder.append(CHECKSUM).append("000").append(SOH);
        stringBuilder.insert(stringBuilder.indexOf(MSG_TYPE),
            BODY_LENGTH + getBodyLength(stringBuilder));
        stringBuilder.replace(stringBuilder.lastIndexOf("000" + SOH), stringBuilder.lastIndexOf(SOH), getChecksum(stringBuilder));
    }

    public String getTime() {
        DateTimeFormatter formatter = settings.getSendingDateTimeFormat();
        LocalDateTime datetime = LocalDateTime.now(ZoneOffset.UTC);
        return formatter.format(datetime);
    }

    public String getRandomOldPassword() {
        var previouslyUsedPasswords = passwordManager.getPreviouslyUsedPasswords();
        if(previouslyUsedPasswords.isEmpty()) {
            throw new IllegalStateException("There was attempt to get old password while there is no old passwords");
        }
        return previouslyUsedPasswords.get(random.nextInt(previouslyUsedPasswords.size()));
    }

    private <T> T getRandomElementFromList(List<T> elements) {
        if(elements.isEmpty()) return null;
        return elements.get(random.nextInt(elements.size()));
    }

    public AtomicBoolean getEnabled() {
        return enabled;
    }

    private void resetHeartbeatTask() {
        heartbeatTimer.getAndSet(
            executorService.schedule(
                this::sendHeartbeat,
                settings.getHeartBtInt(),
                TimeUnit.SECONDS
            )
        ).cancel(false);
    }

    private void resetTestRequestTask() {
        testRequestTimer.getAndSet(
            executorService.schedule(
                this::sendTestRequest,
                settings.getHeartBtInt() * 3,
                TimeUnit.SECONDS
            )
        ).cancel(false);
    }

    private void cancelFuture(AtomicReference<Future<?>> future) {
        future.get().cancel(false);
    }

    private void ruleStartEvent(RuleType type, Instant start) {
        String message = String.format("%s strategy started: %s", type.name(), start.toString());
        LOGGER.info(message);
        context.send(
            Event
            .start()
            .endTimestamp()
            .type(STRATEGY_EVENT_TYPE)
            .name(message)
            .status(Event.Status.PASSED),
            strategyRootEvent
        );
    }

    private void ruleEndEvent(RuleType type, Instant start, List<MessageID> messageIDS, Map<String, Object> additionalDetails) {
        Instant end = Instant.now();
        String message = String.format("%s strategy finished: %s - %s", type.name(), start.toString(), end.toString());
        LOGGER.info(message);
        try {
            Message jsonBody = createMessageBean(mapper.writeValueAsString(Map.of(
                "StartTime", start.toString(), "EndTime", end.toString(),
                "Type", type.toString(), "AffectedMessages", messageIDS.stream().map(UtilKt::logId).collect(Collectors.toList()),
                "AdditionalDetails", additionalDetails
            )));
            Event event = Event
                .start()
                .endTimestamp()
                .type(STRATEGY_EVENT_TYPE)
                .name(message)
                .bodyData(jsonBody)
                .status(Event.Status.PASSED);
            context.send(
                event,
                strategyRootEvent
            );
        } catch (Exception e) {
            LOGGER.error("Error while publishing strategy event: {}", message, e);
        }
    }

    private void ruleEndEvent(RuleType type, Instant start, List<MessageID> messageIDS) {
        ruleEndEvent(type, start, messageIDS, Collections.emptyMap());
    }

    private void ruleErrorEvent(RuleType type, String message, Throwable error) {
        String errorLog = String.format("Rule %s error event: message - %s, error - %s", type, message, error);
        LOGGER.error(errorLog, error);
        context.send(
            Event
                .start()
                .endTimestamp()
                .type(STRATEGY_EVENT_TYPE)
                .name(errorLog)
                .exception(error, true)
                .status(Event.Status.FAILED),
            strategyRootEvent
        );
    }
    // </editor-fold">
}
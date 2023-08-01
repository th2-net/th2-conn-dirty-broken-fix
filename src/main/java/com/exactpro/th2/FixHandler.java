/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.EventID;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.utils.event.transport.EventUtilsKt;
import com.exactpro.th2.conn.dirty.fix.FixField;
import com.exactpro.th2.conn.dirty.fix.MessageTransformer;
import com.exactpro.th2.conn.dirty.fix.SequenceLoader;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BatchSendConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BlockMessageConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ChangeSequenceConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ResendRequestConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.SplitSendConfiguration;
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.TransformMessageConfiguration;
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
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel;
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.SendMode;
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandler;
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerContext;
import com.exactpro.th2.conn.dirty.tcp.core.util.CommonUtil;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import static com.exactpro.th2.constants.Constants.ENCRYPTED_PASSWORD;
import static com.exactpro.th2.constants.Constants.ENCRYPT_METHOD;
import static com.exactpro.th2.constants.Constants.END_SEQ_NO;
import static com.exactpro.th2.constants.Constants.END_SEQ_NO_TAG;
import static com.exactpro.th2.constants.Constants.GAP_FILL_FLAG;
import static com.exactpro.th2.constants.Constants.GAP_FILL_FLAG_TAG;
import static com.exactpro.th2.constants.Constants.HEART_BT_INT;
import static com.exactpro.th2.constants.Constants.IS_POSS_DUP;
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
import static com.exactpro.th2.constants.Constants.NEW_PASSWORD;
import static com.exactpro.th2.constants.Constants.NEW_SEQ_NO;
import static com.exactpro.th2.constants.Constants.NEW_SEQ_NO_TAG;
import static com.exactpro.th2.constants.Constants.NEXT_EXPECTED_SEQ_NUM;
import static com.exactpro.th2.constants.Constants.NEXT_EXPECTED_SEQ_NUMBER_TAG;
import static com.exactpro.th2.constants.Constants.ORIG_SENDING_TIME;
import static com.exactpro.th2.constants.Constants.ORIG_SENDING_TIME_TAG;
import static com.exactpro.th2.constants.Constants.PASSWORD;
import static com.exactpro.th2.constants.Constants.POSS_DUP;
import static com.exactpro.th2.constants.Constants.POSS_DUP_TAG;
import static com.exactpro.th2.constants.Constants.RESET_SEQ_NUM;
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
import static com.exactpro.th2.constants.Constants.TEXT_TAG;
import static com.exactpro.th2.constants.Constants.USERNAME;
import static com.exactpro.th2.netty.bytebuf.util.ByteBufUtil.asExpandable;
import static com.exactpro.th2.netty.bytebuf.util.ByteBufUtil.indexOf;
import static com.exactpro.th2.netty.bytebuf.util.ByteBufUtil.isEmpty;
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
    private static final String STUBBING_VALUE = "XXX";

    private final AtomicInteger msgSeqNum = new AtomicInteger(0);
    private final AtomicInteger serverMsgSeqNum = new AtomicInteger(0);
    private final AtomicInteger testReqID = new AtomicInteger(0);
    private final AtomicBoolean sessionActive = new AtomicBoolean(true);
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final AtomicBoolean connStarted = new AtomicBoolean(false);
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
    private final IHandlerContext context;
    private final InetSocketAddress address;
    private final DataProviderService dataProvider;

    private final StatefulStrategy strategy = defaultStrategyHolder();
    private final StrategyScheduler scheduler;
    private final EventID strategyRootEvent;

    private final AtomicReference<Future<?>> heartbeatTimer = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private final AtomicReference<Future<?>> testRequestTimer = new AtomicReference<>(CompletableFuture.completedFuture(null));
    private Future<?> reconnectRequestTimer = CompletableFuture.completedFuture(null);
    private volatile IChannel channel;
    protected FixHandlerSettings settings;
    private final MessageTransformer messageTransformer = MessageTransformer.INSTANCE;

    public FixHandler(IHandlerContext context) {
        this.context = context;
        strategyRootEvent = context.send(CommonUtil.toEvent("Strategy root event"), null);
        this.settings = (FixHandlerSettings) context.getSettings();
        if(settings.isLoadSequencesFromCradle()) {
            this.dataProvider = context.getGrpcService(DataProviderService.class);
        } else {
            this.dataProvider = null;
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
            } else if(now.isBefore(now.with(settings.getSessionStartTime()))) {
                sessionActive.set(false);
            }

            long time = now.until(scheduleTime, ChronoUnit.SECONDS);
            executorService.scheduleAtFixedRate(() -> {
                sendLogout();
                waitLogoutResponse();
                channel.close();
                sessionActive.set(false);
            }, time, DAY_SECONDS, TimeUnit.SECONDS);
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
        if (settings.getBrokenConnConfiguration() == null) {
            scheduler = new StrategyScheduler(SchedulerType.CONSECUTIVE, Collections.emptyList());
            return;
        }

        var brokenConnConfig = settings.getBrokenConnConfiguration();
        scheduler = new StrategyScheduler(brokenConnConfig.getSchedulerType(), brokenConnConfig.getRules());
        executorService.schedule(this::applyNextStrategy, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onStart() {
        channel = context.createChannel(address, settings.getSecurity(), Map.of(), true, settings.getReconnectDelay() * 1000L, Integer.MAX_VALUE);
        if(settings.isLoadSequencesFromCradle()) {
            SequenceLoader seqLoader = new SequenceLoader(
                    dataProvider,
                    settings.getSessionStartTime(),
                    channel.getSessionAlias(),
                    context.getBookName()
            );
            SequenceHolder sequences = seqLoader.load();
            LOGGER.info("Loaded sequences are: client - {}, server - {}", sequences.getClientSeq(), sequences.getServerSeq());
            msgSeqNum.set(sequences.getClientSeq());
            serverMsgSeqNum.set(sequences.getServerSeq());
        }
        channel.open();
    }

    @NotNull
    public CompletableFuture<MessageID> send(@NotNull ByteBuf body, @NotNull Map<String, String> properties, @Nullable EventID eventID) {
        strategy.getPresendStrategy().process(body, properties);
        if (!sessionActive.get()) {
            throw new IllegalStateException("Session is not active. It is not possible to send messages.");
        }

        if (!channel.isOpen()) {
            try {
                channel.open().get();
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
        }

        while (channel.isOpen() && !enabled.get()) {
            if (LOGGER.isWarnEnabled()) LOGGER.warn("Session is not yet logged in: {}", channel.getSessionAlias());
            try {
                //noinspection BusyWait
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("Error while sleeping.");
            }
        }
        return strategy.getSendStrategy().getSendHandler().send(channel, body, properties, eventID);
    }

    @NotNull
    @Override
    public CompletableFuture<MessageID> send(@NotNull RawMessage rawMessage) {
        return send(toByteBuf(rawMessage.getBody()), rawMessage.getMetadata().getPropertiesMap(), getEventId(rawMessage));
    }

    @NotNull
    @Override
    public CompletableFuture<MessageID> send(@NotNull com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage message) {
        final var id = message.getEventId();
        return send(message.getBody(), message.getMetadata(), id != null ? EventUtilsKt.toProto(id) : null);
    }

    @Override
    public ByteBuf onReceive(@NotNull IChannel channel, @NotNull ByteBuf buffer) {
        strategy.getReceivePreprocessor().process(buffer, Collections.emptyMap());
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
    public Map<String, String> onIncoming(@NotNull IChannel channel, @NotNull ByteBuf message) {
        Map<String, String> metadata = new HashMap<>();

        if(strategy.getIncomingMessagesPreprocessor().process(message, metadata) != null) {
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
        if(possDup != null && possDup.getValue() != null) {
            isDup = possDup.getValue().equals(IS_POSS_DUP);
        }

        String msgTypeValue = requireNonNull(msgType.getValue());
        if(msgTypeValue.equals(MSG_TYPE_LOGOUT)) {
            serverMsgSeqNum.incrementAndGet();
            handleLogout(message);
            return metadata;
        }

        int receivedMsgSeqNum = Integer.parseInt(requireNonNull(msgSeqNumValue.getValue()));

        if(receivedMsgSeqNum < serverMsgSeqNum.get() && !isDup) {
            metadata.put(REJECT_REASON, "SeqNum is less than expected.");
            if (LOGGER.isErrorEnabled()) LOGGER.error("Invalid message. SeqNum is less than expected {}: {}", serverMsgSeqNum.get(), message.toString(US_ASCII));
            sendLogout();
            reconnectRequestTimer = executorService.schedule(this::sendLogon, settings.getReconnectDelay(), TimeUnit.SECONDS);
            return metadata;
        }

        serverMsgSeqNum.incrementAndGet();

        if (serverMsgSeqNum.get() < receivedMsgSeqNum && !isDup && enabled.get()) {
            sendResendRequest(serverMsgSeqNum.get(), receivedMsgSeqNum - 1);
        }

        switch (msgTypeValue) {
            case MSG_TYPE_HEARTBEAT:
                if (LOGGER.isInfoEnabled()) LOGGER.info("Heartbeat received - {}", message.toString(US_ASCII));
                handleHeartbeat(message);
                break;
            case MSG_TYPE_LOGON:
                Map<String, String> logonMetadata = strategy.getLogonProcessor().process(message, metadata);
                if (logonMetadata != null) return logonMetadata;
                break;
            case MSG_TYPE_RESEND_REQUEST:
                if (LOGGER.isInfoEnabled()) LOGGER.info("Resend request received - {}", message.toString(US_ASCII));
                handleResendRequest(message);
                break;
            case MSG_TYPE_SEQUENCE_RESET: //gap fill
                if (LOGGER.isInfoEnabled()) LOGGER.info("Sequence reset received - {}", message.toString(US_ASCII));
                resetSequence(message);
                break;
            case MSG_TYPE_TEST_REQUEST:
                if(LOGGER.isInfoEnabled()) LOGGER.info("Test request received - {}", message.toString(US_ASCII));
                if(strategy.getTestRequestProcessor().process(message, metadata) != null) {
                    return metadata;
                }
                break;
            default:
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
                    strategy.getRecoveryHandler().recovery(nextExpectedSeqNumber, seqNum);
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

            if (!connStarted.get()){
                connStarted.set(true);
            }

            resetHeartbeatTask();

            resetTestRequestTask();
        } else {
            enabled.set(false);
            reconnectRequestTimer = executorService.schedule(this::sendLogon, settings.getReconnectDelay(), TimeUnit.SECONDS);
        }
        return null;
    }

    private void handleHeartbeat(@NotNull ByteBuf message) {
        checkHeartbeat(message);
    }

    private void handleLogout(@NotNull ByteBuf message) {
        if (LOGGER.isInfoEnabled()) LOGGER.info("Logout received - {}", message.toString(US_ASCII));
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
                        serverMsgSeqNum.set(Integer.parseInt(wrongClientNextExpectedSequence));
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
        context.send(CommonUtil.toEvent("logout for sender - " + settings.getSenderCompID()), null);//make more useful
    }

    private void resetSequence(ByteBuf message) {
        FixField gapFillMode = findField(message, GAP_FILL_FLAG_TAG);
        FixField seqNumValue = findField(message, NEW_SEQ_NO_TAG);

        if(seqNumValue != null) {
            if(gapFillMode == null || gapFillMode.getValue() == null || gapFillMode.getValue().equals("N")) {
                serverMsgSeqNum.set(Integer.parseInt(requireNonNull(seqNumValue.getValue())));
            } else {
                serverMsgSeqNum.set(Integer.parseInt(requireNonNull(seqNumValue.getValue())) - 1);
            }
        } else {
            LOGGER.trace("Failed to reset servers MsgSeqNum. No such tag in message: {}", message.toString(US_ASCII));
        }
    }

    private void reset() {
        msgSeqNum.set(0);
        serverMsgSeqNum.set(0);
        sessionActive.set(true);
        channel.open();
    }

    public void sendResendRequest(int beginSeqNo, int endSeqNo) { //do private
        LOGGER.info("Sending resend request: {} - {}", beginSeqNo, endSeqNo);
        StringBuilder resendRequest = new StringBuilder();
        setHeader(resendRequest, MSG_TYPE_RESEND_REQUEST, msgSeqNum.incrementAndGet());
        resendRequest.append(BEGIN_SEQ_NO).append(beginSeqNo).append(SOH);
        resendRequest.append(END_SEQ_NO).append(endSeqNo).append(SOH);
        setChecksumAndBodyLength(resendRequest);
        channel.send(Unpooled.wrappedBuffer(resendRequest.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), null, SendMode.HANDLE_AND_MANGLE);
        resetHeartbeatTask();
    }

    void sendResendRequest(int beginSeqNo) { //do private
        StringBuilder resendRequest = new StringBuilder();
        setHeader(resendRequest, MSG_TYPE_RESEND_REQUEST, msgSeqNum.incrementAndGet());
        resendRequest.append(BEGIN_SEQ_NO).append(beginSeqNo);
        resendRequest.append(END_SEQ_NO).append(0);
        setChecksumAndBodyLength(resendRequest);

        if (enabled.get()) {
            channel.send(Unpooled.wrappedBuffer(resendRequest.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), null, SendMode.HANDLE_AND_MANGLE);
            resetHeartbeatTask();
        } else {
            sendLogon();
        }
    }

    private void handleResendRequest(ByteBuf message) {

        FixField strBeginSeqNo = findField(message, BEGIN_SEQ_NO_TAG);
        FixField strEndSeqNo = findField(message, END_SEQ_NO_TAG);

        if (strBeginSeqNo != null && strEndSeqNo != null) {
            int beginSeqNo = Integer.parseInt(requireNonNull(strBeginSeqNo.getValue()));
            int endSeqNo = Integer.parseInt(requireNonNull(strEndSeqNo.getValue()));

            try {
                // FIXME: there is not syn on the outgoing sequence. Should make operations with seq more careful
                strategy.getRecoveryHandler().recovery(beginSeqNo, endSeqNo);
            } catch (Exception e) {
                sendSequenceReset();
            }
        }
    }

    private void recovery(int beginSeqNo, int endSeqNo) {
        if (endSeqNo == 0) {
            endSeqNo = msgSeqNum.get() + 1;
        }
        LOGGER.info("Returning messages from {} to {}", beginSeqNo, endSeqNo);

        StringBuilder sequenceReset = new StringBuilder();
        setHeader(sequenceReset, MSG_TYPE_SEQUENCE_RESET, beginSeqNo);
        sequenceReset.append(GAP_FILL_FLAG).append("Y");
        sequenceReset.append(NEW_SEQ_NO).append(endSeqNo);
        setChecksumAndBodyLength(sequenceReset);

        channel.send(Unpooled.wrappedBuffer(sequenceReset.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), null, SendMode.HANDLE_AND_MANGLE);
        resetHeartbeatTask();
    }

    private void sendSequenceReset() {
        StringBuilder sequenceReset = new StringBuilder();
        setHeader(sequenceReset, MSG_TYPE_SEQUENCE_RESET, msgSeqNum.incrementAndGet());
        sequenceReset.append(NEW_SEQ_NO).append(msgSeqNum.get() + 1);
        setChecksumAndBodyLength(sequenceReset);

        if (enabled.get()) {
            channel.send(Unpooled.wrappedBuffer(sequenceReset.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), null, SendMode.HANDLE_AND_MANGLE);
            resetHeartbeatTask();
        } else {
            sendLogon();
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
        if (sessionStatusField == null || requireNonNull(sessionStatusField.getValue()).equals("0")) {
            FixField msgSeqNumValue = findField(message, MSG_SEQ_NUM_TAG);
            if (msgSeqNumValue == null) {
                return false;
            }
            serverMsgSeqNum.set(Integer.parseInt(requireNonNull(msgSeqNumValue.getValue())));
            context.send(CommonUtil.toEvent("successful login"), null);
            return true;
        }
        return false;
    }

    @Override
    public void onOutgoing(@NotNull IChannel channel, @NotNull ByteBuf message, @NotNull Map<String, String> metadata) {
        strategy.getOutgoingMessageProcessor().process(message, metadata);

        if (LOGGER.isDebugEnabled()) LOGGER.debug("Outgoing message: {}", message.toString(US_ASCII));
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
            beginString = firstField(message).insertPrevious(BEGIN_STRING_TAG, settings.getBeginString());
        } else if (!settings.getBeginString().equals(beginString.getValue())) {
            beginString.setValue(settings.getBeginString());
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
            lastField(message).insertNext(CHECKSUM_TAG, STUBBING_VALUE); //stubbing until finish checking message
        }

        FixField msgSeqNum = findField(message, MSG_SEQ_NUM_TAG, US_ASCII, bodyLength);

        String msgSeqNumValue = Integer.toString(this.msgSeqNum.incrementAndGet());
        if (msgSeqNum == null) {
            if (msgType != null) {
                msgSeqNum = msgType.insertNext(MSG_SEQ_NUM_TAG, msgSeqNumValue);
            } else {
                msgSeqNum = bodyLength.insertNext(MSG_SEQ_NUM_TAG, msgSeqNumValue);
            }
        } else {
            msgSeqNum.setValue(msgSeqNumValue);
        }

        FixField senderCompID = findField(message, SENDER_COMP_ID_TAG, US_ASCII, bodyLength);

        if (senderCompID == null) {
            senderCompID = msgSeqNum.insertNext(SENDER_COMP_ID_TAG, settings.getSenderCompID());
        } else if (!settings.getSenderCompID().equals(senderCompID.getValue())) {
            senderCompID.setValue(settings.getSenderCompID());
        }

        FixField targetCompID = findField(message, TARGET_COMP_ID_TAG, US_ASCII, bodyLength);

        if (targetCompID == null) {
            targetCompID = senderCompID.insertNext(TARGET_COMP_ID_TAG, settings.getTargetCompID());
        } else if (!settings.getTargetCompID().equals(targetCompID.getValue())) {
            targetCompID.setValue(settings.getTargetCompID());
        }

        FixField senderSubID = findField(message, SENDER_SUB_ID_TAG, US_ASCII, bodyLength);
        if (senderSubID == null) {
            if (settings.getSenderSubID() != null) {
                targetCompID.insertNext(SENDER_SUB_ID_TAG, settings.getSenderSubID());
            }
        } else {
            if (settings.getSenderSubID() == null) {
                senderSubID.clear();
            } else if (!settings.getSenderSubID().equals(senderSubID.getValue())) {
                senderSubID.setValue(settings.getSenderSubID());
            }
        }

        FixField sendingTime = findField(message, SENDING_TIME_TAG, US_ASCII, bodyLength);

        if (sendingTime == null) {
            targetCompID.insertNext(SENDING_TIME_TAG, getTime());
        } else {
            sendingTime.setValue(getTime());
        }

        updateLength(message);
        updateChecksum(message);
    }

    @Override
    public void onOpen(@NotNull IChannel channel) {
        this.channel = channel;
        sendLogon();
    }

    public void sendHeartbeat() {
        sendHeartbeatWithTestRequest(null);
    }

    private void sendHeartbeatWithTestRequest(String testRequestId) {
        StringBuilder heartbeat = new StringBuilder();
        int seqNum = msgSeqNum.incrementAndGet();

        setHeader(heartbeat, MSG_TYPE_HEARTBEAT, seqNum);

        if(testRequestId != null) {
            heartbeat.append(TEST_REQ_ID).append(testRequestId);
        }

        setChecksumAndBodyLength(heartbeat);

        if (enabled.get()) {
            LOGGER.info("Send Heartbeat to server - {}", heartbeat);
            channel.send(Unpooled.wrappedBuffer(heartbeat.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), null, SendMode.HANDLE_AND_MANGLE);
            resetHeartbeatTask();

        } else {
            sendLogon();
        }
    }

    public void sendTestRequest() { //do private
        StringBuilder testRequest = new StringBuilder();
        setHeader(testRequest, MSG_TYPE_TEST_REQUEST, msgSeqNum.incrementAndGet());
        testRequest.append(TEST_REQ_ID).append(testReqID.incrementAndGet());
        setChecksumAndBodyLength(testRequest);
        if (enabled.get()) {
            channel.send(Unpooled.wrappedBuffer(testRequest.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), null, SendMode.HANDLE_AND_MANGLE);
            LOGGER.info("Send TestRequest to server - {}", testRequest);
            resetTestRequestTask();
            resetHeartbeatTask();
        } else {
            sendLogon();
        }
        reconnectRequestTimer = executorService.schedule(this::sendLogon, settings.getReconnectDelay(), TimeUnit.SECONDS);
    }

    public void sendLogon() {
        if(!sessionActive.get() || !channel.isOpen()) {
            LOGGER.info("Logon is not sent to server because session is not active.");
            return;
        }
        StringBuilder logon = new StringBuilder();
        Boolean reset;
        if (!connStarted.get()) {
            reset = settings.getResetSeqNumFlag();
        } else {
            reset = settings.getResetOnLogon();
        }
        if (reset) msgSeqNum.getAndSet(0);

        setHeader(logon, MSG_TYPE_LOGON, msgSeqNum.get() + 1);
        if (settings.useNextExpectedSeqNum()) logon.append(NEXT_EXPECTED_SEQ_NUM).append(serverMsgSeqNum.get() + 1);
        if (settings.getEncryptMethod() != null) logon.append(ENCRYPT_METHOD).append(settings.getEncryptMethod());
        logon.append(HEART_BT_INT).append(settings.getHeartBtInt());
        if (reset) logon.append(RESET_SEQ_NUM).append("Y");
        if (settings.getDefaultApplVerID() != null) logon.append(DEFAULT_APPL_VER_ID).append(settings.getDefaultApplVerID());
        if (settings.getUsername() != null) logon.append(USERNAME).append(settings.getUsername());
        if (settings.getPassword() != null) {
            if (settings.getPasswordEncryptKeyFilePath() != null) {
                logon.append(ENCRYPTED_PASSWORD).append(encrypt(settings.getPassword()));
            } else {
                logon.append(PASSWORD).append(settings.getPassword());
            }
        }
        if (settings.getNewPassword() != null) {
            if (settings.getPasswordEncryptKeyFilePath() != null) {
                logon.append(NEW_ENCRYPTED_PASSWORD).append(encrypt(settings.getNewPassword()));
            } else {
                logon.append(NEW_PASSWORD).append(settings.getNewPassword());
            }
        }

        setChecksumAndBodyLength(logon);
        LOGGER.info("Send logon - {}", logon);
        channel.send(Unpooled.wrappedBuffer(logon.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), null, SendMode.HANDLE_AND_MANGLE);
    }

    private void sendLogout() {
        if (enabled.get()) {
            StringBuilder logout = new StringBuilder();
            setHeader(logout, MSG_TYPE_LOGOUT, msgSeqNum.incrementAndGet());
            setChecksumAndBodyLength(logout);

            LOGGER.debug("Sending logout - {}", logout);

            try {
                channel.send(
                        Unpooled.wrappedBuffer(logout.toString().getBytes(StandardCharsets.UTF_8)),
                        Collections.emptyMap(),
                        null,
                        SendMode.HANDLE_AND_MANGLE
                ).get();

                LOGGER.info("Sent logout - {}", logout);
            } catch (Exception e) {
                LOGGER.error("Failed to send logout - {}", logout, e);
            }
        }
    }

    private String encrypt(String password) {
        return settings.getPasswordEncryptKeyFileType()
                .encrypt(Paths.get(settings.getPasswordEncryptKeyFilePath()),
                        password,
                        settings.getPasswordKeyEncryptAlgorithm(),
                        settings.getPasswordEncryptAlgorithm(),
                        ENCRYPT_MODE);
    }

    @Override
    public void onClose(@NotNull IChannel channel) {
        enabled.set(false);
        cancelFuture(heartbeatTimer);
        cancelFuture(testRequestTimer);
    }

    @Override
    public void close() {
        sendLogout();
        waitLogoutResponse();
    }

    // <editor-fold desc="strategies definitions goes here.">

    // <editor-fold desc="send strategies definitions goes here."

    private CompletableFuture<MessageID> defaultSend(IChannel channel, ByteBuf message, Map<String, String> properties, EventID eventID) {
        return channel.send(message, properties, eventID, SendMode.HANDLE_AND_MANGLE);
    }

    // FIXME: Should we send batch pieces separately to mstore?
    private CompletableFuture<MessageID> bulkSend(IChannel channel, ByteBuf message, Map<String, String> properties, EventID eventID) {
        BatchSendConfiguration config = strategy.getBatchSendConfiguration();
        onOutgoingUpdateTag(message, properties);
        StrategyState strategyState = strategy.getState();

        strategyState.addMessageToBatchCache(message);

        CompletableFuture<MessageID> messageID;
        if(strategyState.getBatchMessageCacheSize().get() == config.getBatchSize()) {
            messageID = channel.send(strategyState.getBatchMessageCache(), properties, eventID, SendMode.DIRECT);
            strategyState.resetBatchMessageCache();
        } else {
            messageID = CompletableFuture.completedFuture(null);
        }
        return messageID;
    }

    // FIXME: Should we send full message to mstore?
    private CompletableFuture<MessageID> splitSend(IChannel channel, ByteBuf message, Map<String, String> properties, EventID eventID) {
        SplitSendConfiguration config = strategy.getSplitSendConfiguration();
        onOutgoingUpdateTag(message, properties);
        List<ByteBuf> slices = new ArrayList<>();
        int numberOfSlices = config.getNumberOfParts();
        int sliceSize = message.readableBytes() / numberOfSlices;
        int nextSliceStart = 0;
        for (int i = 0; i < numberOfSlices - 1; i++) {
            slices.add(message.retainedSlice(message.readerIndex() + nextSliceStart, sliceSize));
            nextSliceStart += sliceSize;
        }
        int readerIndex = message.readerIndex();
        slices.add(message.retainedSlice(readerIndex + nextSliceStart, message.writerIndex() - nextSliceStart - readerIndex));

        long sleepTime = config.getTimeoutBetweenParts();
        List<CompletableFuture<MessageID>> messageIDS = new ArrayList<>();
        for(ByteBuf slice : slices) {
            messageIDS.add(channel.send(slice, properties, eventID, SendMode.DIRECT));
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                LOGGER.error("Error while sending messages in different tcp packets.");
            }
        }
        return messageIDS.get(0);
    }

    // TODO: Add simplified configuration
    private Map<String, String> transformProcessor(
        ByteBuf message,
        Map<String, String> metadata
    ) {
        FixField msgTypeField = findField(message, MSG_TYPE_TAG, US_ASCII);
        if(msgTypeField == null || msgTypeField.getValue() == null) {
            return null;
        }

        TransformMessageConfiguration config = strategy.getTransformMessageConfiguration();
        if(!msgTypeField.getValue().equals(config.getMessageType())) {
            return null;
        }

        var strategyState = strategy.getState();

        int count = strategyState.getTransformedIncomingMessagesCount().get();
        if(count == config.getNumberOfTimesToTransform()) {
            return null;
        }
        messageTransformer.transformWithoutResults(message, config.getCombinedActions());
        updateLength(message);
        updateChecksum(message);

        strategyState.incrementIncomingTransformedMessages();
        return null;
    }

    private Map<String, String> blockSend(
        ByteBuf message,
        Map<String, String> metadata
    ) {
        BlockMessageConfiguration config = strategy.getBlockOutgoingMessagesConfiguration();
        long timeToBlock = config.getTimeout();
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
        if(strategyState.getMissedIncomingMessagesCount().get() == countToMiss) {
            return null;
        }
        strategyState.incrementMissedIncomingMessages();
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
        if(state.getTransformedIncomingMessagesCount().get() < config.getNumberOfTimesToTransform()) {
            handleLogon(message, metadata);
            sendLogout();
            try {
                channel.close().get();
                channel.open().get();
            } catch (Exception e) {
                LOGGER.error("Error while reconnecting.");
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

    private Map<String, String> missOutgoingMessages(ByteBuf message, Map<String, String> metadata) {
        int countToMiss = strategy.getMissOutgoingMessagesConfiguration().getCount();
        var strategyState = strategy.getState();
        onOutgoingUpdateTag(message, metadata);
        if(strategyState.getMissedOutgoingMessagesCount().get() == countToMiss) {
            return null;
        }
        strategyState.incrementMissedOutgoingMessages();

        strategyState.addMissedMessageToCache(msgSeqNum.get(), message.copy());
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

        if(msgType.equals(MSG_TYPE_HEARTBEAT)) {
            FixField testReqId = findField(message, TEST_REQ_ID_TAG);
            if(testReqId != null && testReqId.getValue() != null && !skipTestRequestReplies) {
                return null;
            }
            message.clear();
            return null;
        } else {
            if(!ADMIN_MESSAGES.contains(msgType)) {
                strategy.getState().addMissedMessageToCache(msgSeqNum.get(), message.copy());
            }
            message.clear();
        }

        return null;
    }
    // </editor-fold>

    // <editor-fold desc="receive strategies"
    private Map<String, String> blockReceiveQueue(ByteBuf message, Map<String, String> metadata) {
        BlockMessageConfiguration config = strategy.getBlockIncomingMessagesConfiguration();
        long timeToBlock = config.getTimeout();
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime <= timeToBlock) {
            try {
                Thread.sleep(1000);
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
        var sendStrategy = new SendStrategy(this::defaultMessageProcessor, this::defaultSend);
        var incomingMessagesStrategy = new IncomingMessagesStrategy(
            this::defaultMessageProcessor, this::handleTestRequest, this::handleLogon
        );
        var outgoingMessagesStrategy = new OutgoingMessagesStrategy(this::defaultOutgoingStrategy);
        return new StatefulStrategy(
            sendStrategy,
            incomingMessagesStrategy,
            outgoingMessagesStrategy,
            receiveStrategy,
            this::defaultCleanupHandler,
            this::recovery,
            new DefaultStrategyHolder(
                sendStrategy,
                incomingMessagesStrategy,
                outgoingMessagesStrategy,
                receiveStrategy,
                this::defaultCleanupHandler,
                this::recovery
            )
        );
    }

    private void setupDisconnectStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.getSendStrategy().setSendPreprocessor(this::blockSend);
        strategy.setCleanupHandler(this::cleanupDisconnectStrategy);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
        try {
            sendLogout();
            channel.close().get();
        } catch (Exception e) {
            String message = String.format("Error while setting up %s", strategy.getType());
            LOGGER.error(message, e);
            context.send(CommonUtil.toErrorEvent(message, e), strategyRootEvent);
        }
    }

    private void cleanupDisconnectStrategy() {
        var state = strategy.getState();
        strategy.getSendStrategy().setSendPreprocessor(this::defaultMessageProcessor);
        try {
            channel.open().get();
            waitUntilLoggedIn();
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s", strategy.getType());
            LOGGER.error(message, e);
            context.send(CommonUtil.toErrorEvent(message, e), strategyRootEvent);
        }
        ruleEndEvent(strategy.getType(), state.getStartTime());
    }

    private void setupIgnoreIncomingMessagesStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        try {
            channel.close().get();
            channel.open().get();
        } catch (Exception e) {
            String message = String.format("Error while setup %s strategy.", configuration.getRuleType());
            LOGGER.error(message, e);
            context.send(toErrorEvent(message, e), strategyRootEvent);
        }
        strategy.getIncomingMessagesStrategy().setIncomingMessagesPreprocessor(this::missIncomingMessages);
        strategy.setCleanupHandler(this::cleanupIgnoreIncomingMessagesStrategy);

        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupIgnoreIncomingMessagesStrategy() {
        strategy.getIncomingMessagesStrategy().setIncomingMessagesPreprocessor(this::defaultMessageProcessor);
        try {
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis()); // waiting for new incoming messages to trigger resend request.
            channel.close().get();
            channel.open().get();
            waitUntilLoggedIn();
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(strategy.getType(), strategy.getStartTime());
        strategy.cleanupStrategy();
    }

    private void setupTransformStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.getIncomingMessagesStrategy().setLogonStrategy(this::logoutOnLogon);
        strategy.getOutgoingMessagesStrategy().setOutgoingMessageProcessor(this::transformOutgoingMessageStrategy);
        strategy.setCleanupHandler(this::cleanupTransformStrategy);
        try {
            sendLogout();
            channel.close().get();
            channel.open().get();
            waitUntilLoggedIn();
        } catch (Exception e) {
            String message = String.format("Error while setting up %s", strategy.getType());
            LOGGER.error(message, e);
            context.send(CommonUtil.toErrorEvent(message, e), strategyRootEvent);
        }
        ruleStartEvent(strategy.getType(), strategy.getStartTime());
    }

    private void cleanupTransformStrategy() {
        strategy.getIncomingMessagesStrategy().setLogonStrategy(this::handleLogon);
        strategy.getSendStrategy().setSendPreprocessor(this::defaultMessageProcessor);
        try {
            channel.close().get();
            channel.open().get();
            waitUntilLoggedIn();
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message);
        }
        ruleEndEvent(strategy.getType(), strategy.getStartTime());
        strategy.cleanupStrategy();
    }

    private void setupBidirectionalResendRequestStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.setCleanupHandler(this::cleanupBidirectionalResendRequestStrategy);
        strategy.setRecoveryHandler(this::recoveryFromState);
        try {
            channel.close().get();
            channel.open().get();
            waitUntilLoggedIn();
        } catch (Exception e) {
            String message = String.format("Error while setup %s strategy.", strategy.getType());
            LOGGER.error(message, e);
            context.send(toErrorEvent(message, e), strategyRootEvent);
        }
        strategy.getIncomingMessagesStrategy().setIncomingMessagesPreprocessor(this::missIncomingMessages);
        strategy.getOutgoingMessagesStrategy().setOutgoingMessageProcessor(this::missOutgoingMessages);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupBidirectionalResendRequestStrategy() {
        strategy.getOutgoingMessagesStrategy().setOutgoingMessageProcessor(this::defaultOutgoingStrategy);
        strategy.getIncomingMessagesStrategy().setIncomingMessagesPreprocessor(this::defaultMessageProcessor);
        try {
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis()); // waiting for new incoming/outgoing messages to trigger resend request.
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(strategy.getType(), strategy.getStartTime());
    }

    private void setupOutgoingGapStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.setRecoveryHandler(this::recoveryFromState);
        strategy.setCleanupHandler(this::cleanupOutgoingGapStrategy);
        try {
            channel.close().get();
            channel.open().get();
            waitUntilLoggedIn();
        } catch (Exception e) {
            String message = String.format("Error while setup %s strategy.", strategy.getType());
            LOGGER.error(message, e);
            context.send(toErrorEvent(message, e), strategyRootEvent);
        }
        strategy.getOutgoingMessagesStrategy().setOutgoingMessageProcessor(this::missOutgoingMessages);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupOutgoingGapStrategy() {
        strategy.getOutgoingMessagesStrategy().setOutgoingMessageProcessor(this::defaultOutgoingStrategy);
        try {
            channel.close().get();
            channel.open().get();
            waitUntilLoggedIn();
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(strategy.getType(), strategy.getStartTime());
        strategy.cleanupStrategy();
    }

    private void setupClientOutageStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.getIncomingMessagesStrategy().setTestRequestProcessor(this::missTestRequest);
        strategy.getOutgoingMessagesStrategy().setOutgoingMessageProcessor(this::missHeartbeatsAndTestRequestReplies);
        strategy.setCleanupHandler(this::cleanupClientOutageStrategy);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupClientOutageStrategy() {
        ruleEndEvent(strategy.getType(), strategy.getStartTime());
        strategy.cleanupStrategy();
    }

    private void setupPartialClientOutageStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.getOutgoingMessagesStrategy().setOutgoingMessageProcessor(this::missHeartbeats);
        strategy.setCleanupHandler(this::cleanupPartialClientOutageStrategy);
        ruleStartEvent(configuration.getRuleType(), strategy.getStartTime());
    }

    private void cleanupPartialClientOutageStrategy() {
        ruleEndEvent(strategy.getType(), strategy.getStartTime());
        strategy.cleanupStrategy();
    }

    private void runResendRequestStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        Instant start = Instant.now();
        ruleStartEvent(configuration.getRuleType(), start);
        ResendRequestConfiguration resendRequestConfig = configuration.getResendRequestConfiguration();
        int msgCount = resendRequestConfig.getMessageCount();
        int currentSeq = serverMsgSeqNum.get();
        sendResendRequest(currentSeq - msgCount, currentSeq);
        try {
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(configuration.getRuleType(), start);
    }

    private void setupSlowConsumerStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.getReceiveStrategy().setReceivePreprocessor(this::blockReceiveQueue);
        strategy.setCleanupHandler(this::cleanupSlowConsumerStrategy);
    }

    private void cleanupSlowConsumerStrategy() {
        ruleEndEvent(strategy.getType(), strategy.getStartTime());
        strategy.cleanupStrategy();
    }

    private void runReconnectWithSequenceResetStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        Instant start = Instant.now();
        ruleStartEvent(configuration.getRuleType(), start);
        ChangeSequenceConfiguration resendRequestConfig = configuration.getChangeSequenceConfiguration();

        try {
            channel.close().get();
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }

        if(resendRequestConfig.getChangeIncomingSequence()) {
            serverMsgSeqNum.set(serverMsgSeqNum.get() - resendRequestConfig.getMessageCount());
        } else {
            msgSeqNum.set(msgSeqNum.get() + resendRequestConfig.getMessageCount());
        }

        try {
            channel.open().get();
            waitUntilLoggedIn();
            Thread.sleep(strategy.getState().getConfig().getCleanUpDuration().toMillis());
        } catch (Exception e) {
            String message = String.format("Error while cleaning up %s strategy", strategy.getType());
            LOGGER.error(message, e);
        }
        ruleEndEvent(configuration.getRuleType(), start);
    }

    private void setupBatchSendStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.getSendStrategy().setSendHandler(this::bulkSend);
        strategy.setCleanupHandler(this::cleanupBatchSendStrategy);
    }

    private void cleanupBatchSendStrategy() {
        var state = strategy.getState();
        if(state.getBatchMessageCacheSize().get() > 0) {
            defaultSend(channel, state.getBatchMessageCache(), Collections.emptyMap(), null);
        }
        strategy.getSendStrategy().setSendHandler(this::defaultSend);
        ruleEndEvent(strategy.getType(), strategy.getStartTime());
        strategy.cleanupStrategy();
    }

    private void setupSplitSendStrategy(RuleConfiguration configuration) {
        strategy.resetStrategyAndState(configuration);
        strategy.getSendStrategy().setSendHandler(this::splitSend);
        strategy.setCleanupHandler(this::cleanupSplitSendStrategy);
    }

    private void cleanupSplitSendStrategy() {
        ruleEndEvent(strategy.getType(), strategy.getStartTime());
        strategy.cleanupStrategy();
    }
    // </editor-fold>

    private Map<String, String> defaultMessageProcessor(ByteBuf message, Map<String, String> metadata) {return null;}
    private void defaultCleanupHandler() {}

    // <editor-fold desc="strategies scheduling and cleanup">
    private void applyNextStrategy() {
        LOGGER.info("Cleaning up current strategy {}", strategy.getState().getType());
        try {
            strategy.getCleanupHandler().cleanup();
        } catch (Exception e) {
            String message = String.format("Error while cleaning up strategy: %s", strategy.getState().getType());
            LOGGER.error(message, e);
            ruleErrorEvent(strategy.getState().getType(), e);
        }

        RuleConfiguration nextStrategyConfig = scheduler.next();
        Consumer<RuleConfiguration> nextStrategySetupFunction = getSetupFunction(nextStrategyConfig);
        try {
            nextStrategySetupFunction.accept(nextStrategyConfig);
        } catch (Exception e) {
            String message = String.format("Error while setting up strategy: %s", strategy.getState().getType());
            LOGGER.error(message, e);
            ruleErrorEvent(nextStrategyConfig.getRuleType(), e);
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
            case TRANSFORM_LOGON: return this::setupTransformStrategy;
            case CREATE_OUTGOING_GAP: return this::setupOutgoingGapStrategy;
            case PARTIAL_CLIENT_OUTAGE: return this::setupPartialClientOutageStrategy;
            case IGNORE_INCOMING_MESSAGES: return this::setupIgnoreIncomingMessagesStrategy;
            case DISCONNECT_WITH_RECONNECT: return this::setupDisconnectStrategy;
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

        for(int i = beginSeqNo; i <= endSeqNo; i++) {
            var missedMessage = state.getMissedMessage(i);
            if(missedMessage == null) {
                int newSeqNo = i == endSeqNo ? msgSeqNum.get() + 1 : i + 1;
                StringBuilder seqReset = createSequenceReset(i, newSeqNo);

                channel.send(
                    Unpooled.wrappedBuffer(seqReset.toString().getBytes(StandardCharsets.UTF_8)),
                    Collections.emptyMap(), null, SendMode.MANGLE
                );
            } else {
                setTime(missedMessage);
                setPossDup(missedMessage);
                updateLength(missedMessage);
                updateChecksum(missedMessage);

                channel.send(missedMessage, Collections.emptyMap(), null, SendMode.MANGLE);
            }
        }
    }

    // </editor-fold">

    // </editor-fold>

    // <editor-fold desc="utility">
    private StringBuilder createSequenceReset(int seqNo, int newSeqNo) {
        StringBuilder sequenceReset = new StringBuilder();
        String time = getTime();
        setHeader(sequenceReset, MSG_TYPE_SEQUENCE_RESET, seqNo);
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

    private void waitUntilLoggedIn() {
        while (!enabled.get()) {
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

    private void setHeader(StringBuilder stringBuilder, String msgType, Integer seqNum) {
        stringBuilder.append(BEGIN_STRING_TAG).append("=").append(settings.getBeginString());
        stringBuilder.append(MSG_TYPE).append(msgType);
        stringBuilder.append(MSG_SEQ_NUM).append(seqNum);
        if (settings.getSenderCompID() != null) stringBuilder.append(SENDER_COMP_ID).append(settings.getSenderCompID());
        if (settings.getTargetCompID() != null) stringBuilder.append(TARGET_COMP_ID).append(settings.getTargetCompID());
        if (settings.getSenderSubID() != null) stringBuilder.append(SENDER_SUB_ID).append(settings.getSenderSubID());
        stringBuilder.append(SENDING_TIME).append(getTime());
    }

    private void setChecksumAndBodyLength(StringBuilder stringBuilder) {
        stringBuilder.append(CHECKSUM).append("000").append(SOH);
        stringBuilder.insert(stringBuilder.indexOf(MSG_TYPE),
            BODY_LENGTH + getBodyLength(stringBuilder));
        stringBuilder.replace(stringBuilder.lastIndexOf("000" + SOH), stringBuilder.lastIndexOf(SOH), getChecksum(stringBuilder));
    }

    public String getTime() {
        DateTimeFormatter formatter = settings.getSendingDateTimeFormat();
        LocalDateTime datetime = LocalDateTime.now();
        return formatter.format(datetime);
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
        context.send(
            Event
                .start()
                .endTimestamp()
                .name(String.format("%s strategy started: %s", type.name(), start.toString()))
                .status(Event.Status.PASSED),
            strategyRootEvent
        );
    }

    private void ruleEndEvent(RuleType type, Instant start) {
        context.send(
            Event
                .start()
                .endTimestamp()
                .name(String.format("%s strategy finished: %s - %s", type.name(), start.toString(), Instant.now().toString()))
                .status(Event.Status.PASSED),
            strategyRootEvent
        );
    }

    private void ruleErrorEvent(RuleType type, Throwable error) {
        context.send(
            Event
                .start()
                .endTimestamp()
                .name(String.format("Rule %s error event: %s", type, error))
                .exception(error, true)
                .status(Event.Status.FAILED),
            strategyRootEvent
        );
    }
    // </editor-fold">
}
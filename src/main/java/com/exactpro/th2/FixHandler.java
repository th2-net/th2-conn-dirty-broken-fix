package com.exactpro.th2;

import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel;
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandler;
import com.exactpro.th2.conn.dirty.tcp.core.api.IProtocolHandlerSettings;
import com.exactpro.th2.constants.Constants;
import com.exactpro.th2.util.MessageUtil;
import com.google.auto.service.AutoService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.exactpro.th2.constants.Constants.*;

//todo add meta-inf
//todo parse logout
//todo gapFillTag
//todo ring buffer as cache
//todo add events

@AutoService(IProtocolHandler.class)
public class FixHandler implements AutoCloseable, IProtocolHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FixHandler.class);
    private static final String SOH = "\001";
    private static final byte BYTE_SOH = 1;
    private static final String STRING_MSG_TYPE = "MsgType";
    private static final String REJECT_REASON = "Reject reason";
    private static final String STUBBING_VALUE = "XXX";
    private final Log outgoingMessages = new Log(10000);
    private final AtomicInteger msgSeqNum = new AtomicInteger(0);
    private final AtomicInteger serverMsgSeqNum = new AtomicInteger(0);
    private final AtomicInteger testReqID = new AtomicInteger(0);
    private final AtomicBoolean enabled = new AtomicBoolean(false);
    private final ScheduledExecutorService executorService;
    private final IChannel client;
    private Future<?> heartbeatTimer;
    private Future<?> testRequestTimer;
    private Future<?> reconnectRequestTimer;
    private Future<?> disconnectRequest;


    protected FixHandlerSettings settings;

    public FixHandler(IChannel client, IProtocolHandlerSettings settings) {
        this.client = client;
        this.settings = (FixHandlerSettings) settings;
        executorService = Executors.newScheduledThreadPool(1);
    }

    @Override
    public ByteBuf onReceive(ByteBuf buffer) {

        int offset = buffer.readerIndex();
        int beginStringIdx = MessageUtil.findTag(buffer, offset, BEGIN_STRING_TAG);

        if (beginStringIdx == -1) {
            if (buffer.writerIndex() > 0) {
                buffer.readerIndex(buffer.writerIndex());
                return buffer.copy(offset, buffer.writerIndex() - offset);
            }
            return null;
        }

        if (beginStringIdx > offset) {
            buffer.readerIndex(beginStringIdx);
            return buffer.copy(offset, beginStringIdx - offset);
        }

        int nextBeginString = MessageUtil.findTag(buffer, beginStringIdx + 1, BEGIN_STRING_TAG);
        int checksum = MessageUtil.findTag(buffer, beginStringIdx, CHECKSUM_TAG);
        int endOfMessageIdx = checksum + 7; //checksum is always 3 digits // or we should search next soh?

        try {
            if (checksum == -1 || buffer.getByte(endOfMessageIdx) != BYTE_SOH || (nextBeginString != -1 && nextBeginString < endOfMessageIdx)) {
                LOGGER.trace("Failed to parse message: {}. No Checksum or no tag separator at the end of the message with index {}", buffer.toString(StandardCharsets.US_ASCII), beginStringIdx);
                throw new Exception();
            }
        } catch (Exception e) {
            if (nextBeginString != -1) {
                buffer.readerIndex(nextBeginString);
                return buffer.copy(beginStringIdx, nextBeginString - beginStringIdx);
            } else {
                buffer.readerIndex(buffer.writerIndex());
                return buffer.copy(beginStringIdx, buffer.writerIndex() - beginStringIdx);
            }
        }

        buffer.readerIndex(endOfMessageIdx + 1);
        return buffer.copy(beginStringIdx, endOfMessageIdx + 1 - beginStringIdx);
    }

    @NotNull
    @Override
    public Map<String, String> onIncoming(@NotNull ByteBuf message) {

        Map<String, String> meta = new HashMap<>();

        int beginString = MessageUtil.findTag(message, BEGIN_STRING_TAG);

        if (beginString == -1) {
            meta.put(REJECT_REASON, "Not a FIX message");
            return meta;
        }

        String msgSeqNumValue = MessageUtil.getTagValue(message, MSG_SEQ_NUM_TAG);
        if (msgSeqNumValue == null) {
            meta.put(REJECT_REASON, "No msgSeqNum Field");
            LOGGER.error("Invalid message. No MsgSeqNum in message: {}", message.toString(StandardCharsets.US_ASCII));
            return meta;
        }

        String msgType = MessageUtil.getTagValue(message, MSG_TYPE_TAG);
        if (msgType == null) {
            meta.put(REJECT_REASON, "No msgType Field");
            LOGGER.error("Invalid message. No MsgType in message: {}", message.toString(StandardCharsets.US_ASCII));
            return meta;
        }

        serverMsgSeqNum.incrementAndGet();
        int receivedMsgSeqNum = Integer.parseInt(msgSeqNumValue);
        if (serverMsgSeqNum.get() < receivedMsgSeqNum) {
            sendResendRequest(serverMsgSeqNum.get(), receivedMsgSeqNum);
        }

        switch (msgType) {
            case MSG_TYPE_HEARTBEAT:
                checkHeartbeat(message);
            case MSG_TYPE_LOGON:
                boolean connectionSuccessful = checkLogon(message);
                enabled.set(connectionSuccessful);
                if (connectionSuccessful) {
                    heartbeatTimer = executorService.scheduleWithFixedDelay(this::sendHeartbeat, settings.getHeartBtInt(), settings.getHeartBtInt(), TimeUnit.SECONDS);
                } else {
                    reconnectRequestTimer = executorService.schedule(this::sendLogon, settings.getReconnectDelay(), TimeUnit.SECONDS);
                }
                break;
            case MSG_TYPE_LOGOUT: //extract logout reason
                if (disconnectRequest != null && !disconnectRequest.isCancelled()) {
                    disconnectRequest.cancel(false);
                }
                enabled.set(false);
                break;
            case MSG_TYPE_RESEND_REQUEST:
                handleResendRequest(message);
                break;
            case MSG_TYPE_SEQUENCE_RESET: //gap fill
                resetSequence(message);
                break;
        }

        if (testRequestTimer != null && !testRequestTimer.isCancelled()) {
            testRequestTimer.cancel(false);
        }

        meta.put(STRING_MSG_TYPE, msgType);

        return meta;
    }

    private void resetSequence(ByteBuf message) {

        String gapFillFlagValue = MessageUtil.getTagValue(message, GAP_FILL_FLAG_TAG);
        String seqNumValue = MessageUtil.getTagValue(message, NEW_SEQ_NO_TAG);

        if (seqNumValue != null && (gapFillFlagValue == null || gapFillFlagValue.equals("N"))) {
            serverMsgSeqNum.set(Integer.parseInt(seqNumValue));
        } else {
            LOGGER.trace("Failed to reset servers MsgSeqNum. No such tag in message: {}", message.toString(StandardCharsets.US_ASCII));
        }
    }

    public void sendResendRequest(int beginSeqNo, int endSeqNo) { //do private

        StringBuilder resendRequest = new StringBuilder();
        setHeader(resendRequest, MSG_TYPE_RESEND_REQUEST);
        resendRequest.append(BEGIN_SEQ_NO).append(beginSeqNo).append(SOH);
        resendRequest.append(END_SEQ_NO).append(endSeqNo).append(SOH);
        setChecksumAndBodyLength(resendRequest);

        if (enabled.get()) {
            client.send(Unpooled.wrappedBuffer(resendRequest.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), IChannel.SendMode.MANGLE);
        } else {
            sendLogon();
        }
    }


    void sendResendRequest(int beginSeqNo) { //do private

        StringBuilder resendRequest = new StringBuilder();
        setHeader(resendRequest, MSG_TYPE_RESEND_REQUEST);
        resendRequest.append(Constants.BEGIN_SEQ_NO).append(beginSeqNo).append(SOH);
        resendRequest.append(Constants.END_SEQ_NO).append(0).append(SOH);
        setChecksumAndBodyLength(resendRequest);

        if (enabled.get()) {
            client.send(Unpooled.wrappedBuffer(resendRequest.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), IChannel.SendMode.MANGLE);
        } else {
            sendLogon();
        }
    }

    private void handleResendRequest(ByteBuf message) {

        if (disconnectRequest != null && !disconnectRequest.isCancelled()) {
            disconnectRequest.cancel(false);
        }

        String strBeginSeqNo = MessageUtil.getTagValue(message, BEGIN_SEQ_NO_TAG);
        String strEndSeqNo = MessageUtil.getTagValue(message, END_SEQ_NO_TAG);

        if (strBeginSeqNo != null && strEndSeqNo != null) {
            int beginSeqNo = Integer.parseInt(strBeginSeqNo);
            int endSeqNo = Integer.parseInt(strBeginSeqNo);

            try {
                for (int i = beginSeqNo; i <= endSeqNo; i++) {
                    client.send(outgoingMessages.get(i), Collections.emptyMap(), IChannel.SendMode.MANGLE);
                }
            } catch (Exception e) {
                sendSequenceReset();
            }
        }
    }

    private void sendSequenceReset() {
        StringBuilder sequenceReset = new StringBuilder();
        setHeader(sequenceReset, MSG_TYPE_SEQUENCE_RESET);
        sequenceReset.append(NEW_SEQ_NO).append(msgSeqNum.get() + 1).append(SOH);
        setChecksumAndBodyLength(sequenceReset);

        if (enabled.get()) {
            client.send(Unpooled.wrappedBuffer(sequenceReset.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), IChannel.SendMode.MANGLE);
        } else {
            sendLogon();
        }
    }

    private void checkHeartbeat(ByteBuf message) {

        String receivedTestReqID = MessageUtil.getTagValue(message, TEST_REQ_ID_TAG);

        if (receivedTestReqID != null) {
            if (receivedTestReqID.equals(Integer.toString(testReqID.get()))) {
                reconnectRequestTimer.cancel(false);
            }
        }
    }

    private boolean checkLogon(ByteBuf message) {

        String sessionStatusField = MessageUtil.getTagValue(message, SESSION_STATUS_TAG); //check another options
        if (sessionStatusField != null && sessionStatusField.equals("0")) {
            String msgSeqNumValue = MessageUtil.getTagValue(message, MSG_SEQ_NUM_TAG);
            if (msgSeqNumValue == null) {
                return false;
            }
            serverMsgSeqNum.set(Integer.parseInt(msgSeqNumValue));
            return true;
        }
        return false;
    }

    @NotNull
    @Override
    public Map<String, String> onOutgoing(@NotNull ByteBuf message, @NotNull Map<String, String> map) {

        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
            heartbeatTimer = executorService.scheduleWithFixedDelay(this::sendHeartbeat, settings.getHeartBtInt(), settings.getHeartBtInt(), TimeUnit.SECONDS);
        }
        Map<String, String> metadata = new HashMap<>();

        message.readerIndex(0);

        int beginString = MessageUtil.findTag(message, BEGIN_STRING_TAG);
        if (beginString == -1) {
            message = MessageUtil.putTag(message, BEGIN_STRING_TAG, settings.getBeginString());
        }

        int bodyLength = MessageUtil.findTag(message, BODY_LENGTH_TAG);
        if (bodyLength == -1) {
            message = MessageUtil.putTag(message, BODY_LENGTH_TAG, STUBBING_VALUE); //stubbing until finish checking message
        }

        int msgType = MessageUtil.findTag(message, MSG_TYPE_TAG);
        if (msgType == -1) {
            LOGGER.error("No msgType in message {}", new String(message.array()));
        } else {
            metadata.put(STRING_MSG_TYPE, MessageUtil.getTagValue(message, MSG_TYPE_TAG));
        }

        int checksum = MessageUtil.findTag(message, CHECKSUM_TAG);
        if (checksum == -1) {
            message = MessageUtil.putTag(message, CHECKSUM_TAG, STUBBING_VALUE); //stubbing until finish checking message
        }

        int senderCompID = MessageUtil.findTag(message, SENDER_COMP_ID);
        if (senderCompID == -1) {
            message = MessageUtil.putTag(message, SENDER_COMP_ID_TAG, settings.getSenderCompID());
        }

        int targetCompID = MessageUtil.findTag(message, TARGET_COMP_ID_TAG);
        if (targetCompID == -1) {
            message = MessageUtil.putTag(message, TARGET_COMP_ID_TAG, settings.getTargetCompID());
        }

        int sendingTime = MessageUtil.findTag(message, SENDING_TIME_TAG);
        if (sendingTime == -1) {
            message = MessageUtil.putTag(message, SENDING_TIME_TAG, getTime().toString());
        }

        int msgSeqNumValue = msgSeqNum.incrementAndGet();
        MessageUtil.putTag(message, MSG_SEQ_NUM_TAG, Integer.toString(msgSeqNumValue));

        message = MessageUtil.updateTag(message, BODY_LENGTH_TAG, Integer.toString(getBodyLength(message)));
        message = MessageUtil.updateTag(message, CHECKSUM_TAG, getChecksum(message));

        outgoingMessages.put(msgSeqNumValue, message);

        return metadata;
    }

    @Override
    public void onOpen() {
        sendLogon();
    }

    public void sendHeartbeat() {

        StringBuilder heartbeat = new StringBuilder();
        setHeader(heartbeat, MSG_TYPE_HEARTBEAT);
        setChecksumAndBodyLength(heartbeat);

        if (enabled.get()) {
            client.send(Unpooled.wrappedBuffer(heartbeat.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), IChannel.SendMode.MANGLE);
        } else {
            sendLogon();
        }
        testRequestTimer = executorService.schedule(this::sendTestRequest, settings.getTestRequestDelay(), TimeUnit.SECONDS);
    }

    public void sendTestRequest() { //do private

        StringBuilder testRequest = new StringBuilder();
        setHeader(testRequest, MSG_TYPE_TEST_REQUEST);
        testRequest.append(Constants.TEST_REQ_ID).append(testReqID.incrementAndGet()).append(SOH);
        setChecksumAndBodyLength(testRequest);
        if (enabled.get()) {
            client.send(Unpooled.wrappedBuffer(testRequest.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), IChannel.SendMode.MANGLE);
        } else {
            sendLogon();
        }
        reconnectRequestTimer = executorService.schedule(this::sendLogon, settings.getReconnectDelay(), TimeUnit.SECONDS);
    }


    public void sendLogon() {

        StringBuilder logon = new StringBuilder();// add defaultApplVerID for fix5+

        setHeader(logon, MSG_TYPE_LOGON);
        logon.append(Constants.ENCRYPT_METHOD).append(settings.getEncryptMethod()).append(SOH);
        logon.append(Constants.HEART_BT_INT).append(settings.getHeartBtInt()).append(SOH);
        logon.append(Constants.USERNAME).append(settings.getUsername()).append(SOH);
        logon.append(Constants.PASSWORD).append(settings.getPassword()).append(SOH);
        setChecksumAndBodyLength(logon);

        client.send(Unpooled.wrappedBuffer(logon.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), IChannel.SendMode.MANGLE);
    }

    @Override
    public void onClose() {

        sendTestRequest();

        StringBuilder logout = new StringBuilder();
        setHeader(logout, MSG_TYPE_LOGOUT);
        setChecksumAndBodyLength(logout);

        if (enabled.get()) {
            client.send(Unpooled.wrappedBuffer(logout.toString().getBytes(StandardCharsets.UTF_8)), Collections.emptyMap(), IChannel.SendMode.MANGLE);
        }
        disconnectRequest = executorService.schedule(() -> enabled.set(false), settings.getDisconnectRequestDelay(), TimeUnit.SECONDS);
    }

    @Override
    public void close() {

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(3000, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }

    private void setHeader(StringBuilder stringBuilder, String msgType) {

        stringBuilder.append(Constants.BEGIN_STRING).append(settings.getBeginString()).append(SOH);
        stringBuilder.append(Constants.MSG_TYPE).append(msgType).append(SOH);
        stringBuilder.append(Constants.SENDER_COMP_ID).append(settings.getSenderCompID()).append(SOH);
        stringBuilder.append(Constants.TARGET_COMP_ID).append(settings.getTargetCompID()).append(SOH);
        stringBuilder.append(Constants.SENDING_TIME).append(getTime()).append(SOH);
    }

    private void setChecksumAndBodyLength(StringBuilder stringBuilder) {
        stringBuilder.append(Constants.CHECKSUM).append("000").append(SOH);
        stringBuilder.insert(stringBuilder.indexOf(SOH + MSG_TYPE) + 1,
                Constants.BODY_LENGTH + getBodyLength(stringBuilder) + SOH);
        stringBuilder.replace(stringBuilder.lastIndexOf("000" + SOH), stringBuilder.lastIndexOf(SOH), getChecksum(stringBuilder));
    }

    public String getChecksum(StringBuilder message) { //do private

        String substring = message.substring(0, message.indexOf(SOH + CHECKSUM) + 1);
        return calculateChecksum(substring.getBytes(StandardCharsets.US_ASCII));
    }

    public String getChecksum(ByteBuf message) {

        int checksumIdx = MessageUtil.findTag(message, CHECKSUM_TAG) + 1;
        if (checksumIdx == 0) {
            checksumIdx = message.capacity();
        }

        ByteBuf data = message.copy(0, checksumIdx);
        return calculateChecksum(data.array());
    }

    private String calculateChecksum(byte[] data) {
        int total = 0;
        for (byte item : data) {
            total += item;
        }
        int checksum = total % 256;
        return String.format("%03d", checksum);
    }

    public int getBodyLength(StringBuilder message) { //do private

        int start = message.indexOf(SOH, message.indexOf(SOH + BODY_LENGTH) + 1);
        int end = message.indexOf(SOH + Constants.CHECKSUM);
        return end - start;
    }

    public int getBodyLength(ByteBuf message) {
        int bodyLengthIdx = MessageUtil.findTag(message, BODY_LENGTH_TAG);
        int start = MessageUtil.findByte(message, bodyLengthIdx + 1, BYTE_SOH);
        int end = MessageUtil.findTag(message, CHECKSUM_TAG);
        return end - start;
    }

    public Instant getTime() {
        return Instant.now();
    }

    public AtomicBoolean getEnabled() {
        return enabled;
    }
}

/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.conn.dirty.fix.TestMessageSearcher;
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse;
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest;
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse;
import com.google.protobuf.ByteString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.exactpro.th2.TestUtils.createHandlerSettings;
import static com.exactpro.th2.conn.dirty.fix.FixByteBufUtilKt.findField;
import static com.exactpro.th2.constants.Constants.MSG_SEQ_NUM_TAG;
import static com.exactpro.th2.constants.Constants.MSG_TYPE_SEQUENCE_RESET;
import static com.exactpro.th2.constants.Constants.MSG_TYPE_TAG;
import static com.exactpro.th2.constants.Constants.NEW_SEQ_NO_TAG;
import static com.exactpro.th2.constants.Constants.POSS_DUP_TAG;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("DataFlowIssue")
public class RecoveryTest {

    private static final ByteBuf logonResponse = Unpooled.wrappedBuffer("8=FIXT.1.1\0019=105\00135=A\00134=1\00149=server\00156=client\00150=system\00152=2014-12-22T10:15:30Z\00198=0\001108=30\0011137=9\0011409=0\00110=203\001".getBytes(StandardCharsets.US_ASCII));
    private Channel channel;
    private FixHandler fixHandler;

    @Test
    void testSequenceResetInRange() {
        FixHandlerSettings settings = createHandlerSettings();
        settings.setLoadMissedMessagesFromCradle(true);
        DataProviderService dataProviderService = Mockito.mock(DataProviderService.class);
        TestMessageSearcher ms = new TestMessageSearcher(
            List.of(
                messageSearchResponse(2),
                messageSearchResponse(3),
                messageSearchResponse(4),
                messageSearchResponse(5)
            )
        );
        Mockito.when(dataProviderService.searchMessageGroups(Mockito.any())).thenAnswer(
            x -> ms.searchMessages(x.getArgument(0, MessageGroupsSearchRequest.class))
        );
        channel = new Channel(settings, dataProviderService);
        fixHandler = channel.getFixHandler();
        fixHandler.onOpen(channel);
        fixHandler.onIncoming(channel, logonResponse, MessageID.getDefaultInstance());
        // requesting resend from 2 to 5
        ByteBuf resendRequest = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=73\u000135=2\u000134=2\u000149=client\u000156=server\u000150=trader\u000152=2014-12-22T10:15:30Z\u00017=2\u000116=5\u000110=226\u0001".getBytes(StandardCharsets.UTF_8));
        fixHandler.onIncoming(channel, resendRequest, MessageID.getDefaultInstance());
        assertEquals(5, channel.getQueue().size());

        for(int i = 1; i <= 4; i++) {
            ByteBuf buf = channel.getQueue().get(i);
            assertEquals(findField(buf, MSG_TYPE_TAG).getValue(), "C");
            assertEquals(Integer.parseInt(findField(buf, MSG_SEQ_NUM_TAG).getValue()), i + 1);
            assertEquals(findField(buf, POSS_DUP_TAG).getValue(), "Y");
        }
    }

    @Test
    void testSequenceResetInsideRange() {
        FixHandlerSettings settings = createHandlerSettings();
        settings.setLoadMissedMessagesFromCradle(true);
        DataProviderService dataProviderService = Mockito.mock(DataProviderService.class);
        TestMessageSearcher ms = new TestMessageSearcher(
            List.of(
                messageSearchResponse(4),
                messageSearchResponse(5)
            )
        );
        Mockito.when(dataProviderService.searchMessageGroups(Mockito.any())).thenAnswer(
            x -> ms.searchMessages(x.getArgument(0, MessageGroupsSearchRequest.class))
        );
        channel = new Channel(settings, dataProviderService);
        fixHandler = channel.getFixHandler();
        fixHandler.onOpen(channel);
        fixHandler.onIncoming(channel, logonResponse, MessageID.getDefaultInstance());
        // handler sequence after loop is 22
        for(int i = 0; i <= 20; i++) {
            fixHandler.onOutgoing(
                channel,
                Unpooled.buffer().writeBytes(messageWithoutSeqNum().getBytes(StandardCharsets.UTF_8)),
                new HashMap<>()
            );
        }
        // requesting resend from 2 to 8
        ByteBuf resendRequest = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=73\u000135=2\u000134=2\u000149=client\u000156=server\u000150=trader\u000152=2014-12-22T10:15:30Z\u00017=2\u000116=8\u000110=226\u0001".getBytes(StandardCharsets.UTF_8));
        fixHandler.onIncoming(channel, resendRequest, MessageID.getDefaultInstance());
        assertEquals(channel.getQueue().size(), 5);

        // for missed messages after beginSeqNo to 4
        ByteBuf firstSequenceReset = channel.getQueue().get(1);
        assertEquals(findField(firstSequenceReset, MSG_TYPE_TAG).getValue(), MSG_TYPE_SEQUENCE_RESET);
        assertEquals(Integer.parseInt(findField(firstSequenceReset, MSG_SEQ_NUM_TAG).getValue()), 2);
        assertEquals(Integer.parseInt(findField(firstSequenceReset, NEW_SEQ_NO_TAG).getValue()), 4);

        ByteBuf message4 = channel.getQueue().get(2);

        assertEquals(findField(message4, MSG_TYPE_TAG).getValue(), "C");
        assertEquals(Integer.parseInt(findField(message4, MSG_SEQ_NUM_TAG).getValue()), 4);
        assertEquals(findField(message4, POSS_DUP_TAG).getValue(), "Y");

        ByteBuf message5 = channel.getQueue().get(3);

        assertEquals(findField(message5, MSG_TYPE_TAG).getValue(), "C");
        assertEquals(Integer.parseInt(findField(message5, MSG_SEQ_NUM_TAG).getValue()), 5);
        assertEquals(findField(message5, POSS_DUP_TAG).getValue(), "Y");

        // For missed messages after 4
        ByteBuf seqReset2 = channel.getQueue().get(4);
        assertEquals(findField(seqReset2, MSG_TYPE_TAG).getValue(), MSG_TYPE_SEQUENCE_RESET);
        assertEquals(Integer.parseInt(findField(seqReset2, MSG_SEQ_NUM_TAG).getValue()), 6);
        assertEquals(Integer.parseInt(findField(seqReset2, NEW_SEQ_NO_TAG).getValue()), 23);
    }

    @Test
    void testSequenceResetOutOfRange() {
        FixHandlerSettings settings = createHandlerSettings();
        settings.setLoadMissedMessagesFromCradle(true);
        DataProviderService dataProviderService = Mockito.mock(DataProviderService.class);
        TestMessageSearcher ms = new TestMessageSearcher(
            List.of(
                messageSearchResponse(1),
                messageSearchResponse(2),
                messageSearchResponse(3),
                messageSearchResponse(4),
                messageSearchResponse(5),
                messageSearchResponse(6)
            )
        );
        Mockito.when(dataProviderService.searchMessageGroups(Mockito.any())).thenAnswer(
            x -> ms.searchMessages(x.getArgument(0, MessageGroupsSearchRequest.class))
        );
        channel = new Channel(settings, dataProviderService);
        fixHandler = channel.getFixHandler();
        fixHandler.onOpen(channel);
        fixHandler.onIncoming(channel, logonResponse, MessageID.getDefaultInstance());
        // requesting resend from 2 to 5
        ByteBuf resendRequest = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=73\u000135=2\u000134=2\u000149=client\u000156=server\u000150=trader\u000152=2014-12-22T10:15:30Z\u00017=2\u000116=5\u000110=226\u0001".getBytes(StandardCharsets.UTF_8));
        fixHandler.onIncoming(channel, resendRequest, MessageID.getDefaultInstance());
        assertEquals(5, channel.getQueue().size());
        for(int i = 1; i <= 4; i++) {
            ByteBuf buf = channel.getQueue().get(i);
            assertEquals(findField(buf, MSG_TYPE_TAG).getValue(), "C");
            assertEquals(Integer.parseInt(findField(buf, MSG_SEQ_NUM_TAG).getValue()), i + 1);
            assertEquals(findField(buf, POSS_DUP_TAG).getValue(), "Y");
        }
    }

    @Test
    void testSequenceResetAdminMessages() {
        FixHandlerSettings settings = createHandlerSettings();
        settings.setLoadMissedMessagesFromCradle(true);
        DataProviderService dataProviderService = Mockito.mock(DataProviderService.class);
        TestMessageSearcher ms = new TestMessageSearcher(
            List.of(
                messageSearchResponseAdmin(2),
                messageSearchResponse(4),
                messageSearchResponseAdmin(5),
                messageSearchResponseAdmin(6)
            )
        );
        Mockito.when(dataProviderService.searchMessageGroups(Mockito.any())).thenAnswer(
            x -> ms.searchMessages(x.getArgument(0, MessageGroupsSearchRequest.class))
        );
        channel = new Channel(settings, dataProviderService);
        fixHandler = channel.getFixHandler();
        fixHandler.onOpen(channel);
        fixHandler.onIncoming(channel, logonResponse, MessageID.getDefaultInstance());
        // handler sequence after loop is 22
        for(int i = 0; i <= 20; i++) {
            fixHandler.onOutgoing(
                channel,
                Unpooled.buffer().writeBytes(messageWithoutSeqNum().getBytes(StandardCharsets.UTF_8)),
                new HashMap<>()
            );
        }
        // requesting resend from 1 to 5
        ByteBuf resendRequest = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=73\u000135=2\u000134=2\u000149=client\u000156=server\u000150=trader\u000152=2014-12-22T10:15:30Z\u00017=1\u000116=5\u000110=226\u0001".getBytes(StandardCharsets.UTF_8));
        fixHandler.onIncoming(channel, resendRequest, MessageID.getDefaultInstance());

        // sequence reset for messages from 1 to 3 ( 1, 2 - missing, 3 - admin )
        ByteBuf seqReset1 = channel.getQueue().get(1);
        assertEquals(findField(seqReset1, MSG_TYPE_TAG).getValue(), MSG_TYPE_SEQUENCE_RESET);
        assertEquals(Integer.parseInt(findField(seqReset1, MSG_SEQ_NUM_TAG).getValue()), 1);
        assertEquals(Integer.parseInt(findField(seqReset1, NEW_SEQ_NO_TAG).getValue()), 4);

        ByteBuf message = channel.getQueue().get(2);
        assertEquals(findField(message, MSG_TYPE_TAG).getValue(), "C");
        assertEquals(Integer.parseInt(findField(message, MSG_SEQ_NUM_TAG).getValue()), 4);
        assertEquals(findField(message, POSS_DUP_TAG).getValue(), "Y");

        // sequence reset for messages from 1 to 3 ( 1, 2 - missing, 3 - admin )
        ByteBuf seqReset2 = channel.getQueue().get(3);
        assertEquals(findField(seqReset2, MSG_TYPE_TAG).getValue(), MSG_TYPE_SEQUENCE_RESET);
        assertEquals(Integer.parseInt(findField(seqReset2, MSG_SEQ_NUM_TAG).getValue()), 5);
        assertEquals(Integer.parseInt(findField(seqReset2, NEW_SEQ_NO_TAG).getValue()), 23);

    }

    @Test
    void allMessagesMissed() {
        FixHandlerSettings settings = createHandlerSettings();
        settings.setLoadMissedMessagesFromCradle(true);
        DataProviderService dataProviderService = Mockito.mock(DataProviderService.class);
        Mockito.when(dataProviderService.searchMessageGroups(Mockito.any())).thenReturn(Collections.emptyIterator());
        channel = new Channel(settings, dataProviderService);
        fixHandler = channel.getFixHandler();
        fixHandler.onOpen(channel);
        fixHandler.onIncoming(channel, logonResponse, MessageID.getDefaultInstance());
        // handler sequence after loop is 22
        for(int i = 0; i <= 20; i++) {
            fixHandler.onOutgoing(
                channel,
                Unpooled.buffer().writeBytes(messageWithoutSeqNum().getBytes(StandardCharsets.UTF_8)),
                new HashMap<>()
            );
        }
        // requesting resend from 1 to 5
        ByteBuf resendRequest = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=73\u000135=2\u000134=2\u000149=client\u000156=server\u000150=trader\u000152=2014-12-22T10:15:30Z\u00017=1\u000116=5\u000110=226\u0001".getBytes(StandardCharsets.UTF_8));
        fixHandler.onIncoming(channel, resendRequest, MessageID.getDefaultInstance());

        // sequence reset for messages from 1 to 3 ( 1, 2 - missing, 3 - admin )
        ByteBuf seqReset = channel.getQueue().get(1);
        assertEquals(findField(seqReset, MSG_TYPE_TAG).getValue(), MSG_TYPE_SEQUENCE_RESET);
        assertEquals(Integer.parseInt(findField(seqReset, MSG_SEQ_NUM_TAG).getValue()), 1);
        assertEquals(Integer.parseInt(findField(seqReset, NEW_SEQ_NO_TAG).getValue()), 23);
    }

    private MessageSearchResponse messageSearchResponse(Integer sequence) {
        return MessageSearchResponse.newBuilder()
            .setMessage(
                MessageGroupResponse.newBuilder()
                    .setBodyRaw(ByteString.copyFromUtf8(message(sequence)))
            ).build();
    }

    private MessageSearchResponse messageSearchResponseAdmin(Integer sequence) {
        return MessageSearchResponse.newBuilder()
            .setMessage(
                MessageGroupResponse.newBuilder()
                    .setBodyRaw(ByteString.copyFromUtf8(adminMessage(sequence)))
            ).build();
    }

    private String message(Integer sequence) {
        return String.format("8=FIXT.1.1\u00019=70\u000135=C\u0001552=1\u000149=client\u000134=%d\u000156=server\u000152=2014-12-22T10:15:30Z\u000150=trader\u000110=132\u0001", sequence);
    }

    private String messageWithoutSeqNum() {
        return "8=FIXT.1.1\u00019=70\u000135=C\u0001552=1\u000149=client\u000156=server\u000152=2014-12-22T10:15:30Z\u000150=trader\u000110=132\u0001";
    }

    private String adminMessage(Integer sequence) {
        return String.format("8=FIXT.1.1\u00019=70\u000135=4\u0001552=1\u000149=client\u000134=%d\u000156=server\u000152=2014-12-22T10:15:30Z\u000150=trader\u000110=132\u0001", sequence);
    }
}

/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage;
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel;
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerContext;
import kotlin.Unit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyVararg;

class FixHandlerSendTimeoutTest {
    @Test
    void sendTimeoutOnConnectionOpen() {
        var contextMock = Mockito.mock(IHandlerContext.class);
        var channelMock = Mockito.mock(IChannel.class);
        Mockito.when(contextMock.createChannel(
                        any(),
                        any(),
                        any(),
                        anyBoolean(),
                        anyLong(),
                        anyInt(),
                        anyVararg()
                ))
                .thenReturn(channelMock);
        Mockito.when(channelMock.open())
                .thenReturn(new CompletableFuture<>()); // future never completes
        var settings = new FixHandlerSettings();
        settings.setPort(42);
        settings.setHost("localhost");
        settings.setConnectionTimeoutOnSend(300); // 300 millis
        settings.setMinConnectionTimeoutOnSend(100);
        Mockito.when(contextMock.getSettings())
                .thenReturn(settings);
        var fixHandler = new FixHandler(contextMock);
        fixHandler.onStart();
        var exception = Assertions.assertThrows(TimeoutException.class, () ->
                fixHandler.send(RawMessage.builder()
                        .setId(MessageId.builder()
                                .setDirection(Direction.OUTGOING)
                                .setSessionAlias("test")
                                .setSequence(1)
                                .setTimestamp(Instant.now())
                                .build())
                        .build()));
        Assertions.assertEquals(
                "could not open connection before timeout 300 mls elapsed",
                exception.getMessage(),
                "unexpected message"
        );
    }

    @Test
    void sendTimeoutOnSessionEnabled() {
        var contextMock = Mockito.mock(IHandlerContext.class);
        var channelMock = Mockito.mock(IChannel.class);
        Mockito.when(contextMock.createChannel(
                        any(),
                        any(),
                        any(),
                        anyBoolean(),
                        anyLong(),
                        anyInt(),
                        anyVararg()
                ))
                .thenReturn(channelMock);
        Mockito.when(channelMock.open())
                .thenReturn(CompletableFuture.completedFuture(Unit.INSTANCE)); // completed immediately
        Mockito.when(channelMock.isOpen()).thenReturn(true);
        var settings = new FixHandlerSettings();
        settings.setPort(42);
        settings.setHost("localhost");
        settings.setConnectionTimeoutOnSend(300); // 300 millis
        settings.setMinConnectionTimeoutOnSend(100);
        LocalTime currentTime = LocalTime.now(ZoneOffset.UTC);
        int deltaMinutes = currentTime.isAfter(LocalTime.NOON)
                ? -1
                : 1;
        if (deltaMinutes > 0) {
            settings.setSessionStartTime(currentTime.plusMinutes(deltaMinutes));
            settings.setSessionEndTime(currentTime.plusMinutes(deltaMinutes * 2));
        } else {
            settings.setSessionStartTime(currentTime.plusMinutes(deltaMinutes * 2));
            settings.setSessionEndTime(currentTime.plusMinutes(deltaMinutes));
        }
        Mockito.when(contextMock.getSettings())
                .thenReturn(settings);
        var fixHandler = new FixHandler(contextMock);
        fixHandler.onStart();
        var exception = Assertions.assertThrows(TimeoutException.class, () ->
                fixHandler.send(RawMessage.builder()
                        .setId(MessageId.builder()
                                .setDirection(Direction.OUTGOING)
                                .setSessionAlias("test")
                                .setSequence(1)
                                .setTimestamp(Instant.now())
                                .build())
                        .build()));
        Assertions.assertEquals(
                "session was not established within 300 mls",
                exception.getMessage(),
                "unexpected message"
        );
    }
}

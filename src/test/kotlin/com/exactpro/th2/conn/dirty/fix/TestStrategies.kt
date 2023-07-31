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
package com.exactpro.th2.conn.dirty.fix

import com.exactpro.th2.FixHandler
import com.exactpro.th2.FixHandlerSettings
import com.exactpro.th2.TestUtils.createHandlerSettings
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BlockMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BrokenConnConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ChangeSequenceConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.MissMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ResendRequestConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.TransformMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.RuleType
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.SchedulerType
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerContext
import com.exactpro.th2.netty.bytebuf.util.asExpandable
import com.exactpro.th2.netty.bytebuf.util.contains
import com.exactpro.th2.netty.bytebuf.util.isEmpty
import com.exactpro.th2.netty.bytebuf.util.isNotEmpty
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyArray
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever


class TestStrategies {

    private class TestContext(
        val channel: IChannel,
        val fixHandler: FixHandler,
    )

    @Test
    fun testDisconnectStrategy() {
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.DISCONNECT_WITH_RECONNECT,
                    duration = Duration.of(2, ChronoUnit.SECONDS),
                    cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                    blockOutgoingMessagesConfiguration = BlockMessageConfiguration(2000)
                ),
            )
        ), enableAdditionalHandling = false)
        val channel = testContext.channel
        val handler = testContext.fixHandler

        verify(channel, timeout(2100)).close()

        handler.send(businessMessage(2), Collections.emptyMap(), null)

        verify(channel, timeout(2100)).open()

        val captor = argumentCaptor<ByteBuf> {  }
        verify(channel, timeout(1000).times(3)).send(captor.capture(), any(), anyOrNull(), any())

        channel.close()

        captor.firstValue.apply { contains("35=A") }
        captor.secondValue.apply { contains("35=5") }
        captor.thirdValue.apply { contains("35=AE") }
    }

    @Test
    fun testIgnoreIncomingMessagesStrategy() {
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.IGNORE_INCOMING_MESSAGES,
                    duration = Duration.of(2, ChronoUnit.SECONDS),
                    cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                    missIncomingMessagesConfiguration = MissMessageConfiguration(3)
                ),
            )
        )) { msg, mtd, mode ->
            messages.add(Triple(msg, mtd, mode))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        val captor = argumentCaptor<ByteBuf> {  }

        verify(channel, timeout(2100)).open()
        verify(channel).send(any(), any(), anyOrNull(), any()) // Logon
        clearInvocations(channel)

        handler.onIncoming(channel, businessMessage(3))
        handler.onIncoming(channel, businessMessage(4))
        handler.onIncoming(channel, businessMessage(5))

        verify(channel, timeout(2100)).open()
        verify(channel).send(any(), any(), anyOrNull(), any()) // Logon
        clearInvocations(channel)
        handler.onIncoming(channel, businessMessage(6))

        verify(channel, times(1)).send(captor.capture(), any(), anyOrNull(), any())

        channel.close()

        captor.firstValue.apply { contains("35=2") }
    }

    @Test
    fun testTransformLogonMessagesStrategy() {
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.TRANSFORM_LOGON,
                    duration = Duration.of(4, ChronoUnit.SECONDS),
                    cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                    transformMessageConfiguration = TransformMessageConfiguration("test", "A", numberOfTimesToTransform = 2)
                ),
            )
        )) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel

        messages.clear()
        verify(channel, timeout(6200).times(3)).open()

        // start
        verify(channel, times(5)).send(any(), any(), anyOrNull(), any())

        messages[0].apply { assertTrue { first.contains("35=5") } }
        messages[1].apply {
            assertTrue { first.contains("35=A") }
            assertEquals(second[RULE_NAME_PROPERTY], "test")
        }
        messages[2].apply { assertTrue { first.contains("35=5") } }
        messages[3].apply {
            assertTrue { first.contains("35=A") }
            assertEquals(second[RULE_NAME_PROPERTY], "test")
        }
        messages[4].apply {
            assertTrue { first.contains("35=A") }
            assertEquals(second[RULE_NAME_PROPERTY], null)
        }

        channel.close()
    }

    @Test
    fun testBidirectionalResendRequest() {
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.BI_DIRECTIONAL_RESEND_REQUEST,
                    duration = Duration.of(4, ChronoUnit.SECONDS),
                    cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                    missIncomingMessagesConfiguration = MissMessageConfiguration(2),
                    missOutgoingMessagesConfiguration = MissMessageConfiguration(2)
                ),
            )
        )) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        verify(channel, timeout(2200)).open()
        messages.clear()
        Thread.sleep(400)

        handler.onIncoming(channel, businessMessage(4))
        handler.onIncoming(channel, businessMessage(5))

        handler.onOutgoing(channel, businessMessage(3).asExpandable(), Collections.emptyMap())
        handler.onOutgoing(channel, businessMessage(4).asExpandable(), Collections.emptyMap())

        handler.onIncoming(channel, businessMessage(6))

        handler.onIncoming(channel, businessMessage(4))
        handler.onIncoming(channel, businessMessage(5))

        handler.onIncoming(channel, resendRequest(7, 3, 4))

        messages[0].apply {
            val buff = first
            assertTrue { buff.contains("35=2") } // resend request
            assertTrue { buff.contains("7=4") }
            assertTrue { buff.contains("16=6") }
        }

        messages[1].apply {
            val buff = first
            assertTrue { buff.contains("35=AE") }
            assertTrue { buff.contains("43=Y") }
            assertTrue { buff.contains("122=") }
            assertTrue { buff.contains("34=3") }
        }

        messages[2].apply {
            val buff = first
            assertTrue { buff.contains("35=AE") }
            assertTrue { buff.contains("43=Y") }
            assertTrue { buff.contains("122=") }
            assertTrue { buff.contains("34=4") }
        }

        channel.close()
    }

    @Test
    fun testOutgoingGap() {
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.CREATE_OUTGOING_GAP,
                    duration = Duration.of(2, ChronoUnit.SECONDS),
                    cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                    missOutgoingMessagesConfiguration = MissMessageConfiguration(3)
                ),
            )
        )) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        verify(channel, timeout(2100)).open()
        verify(channel).send(any(), any(), anyOrNull(), any()) // Logon
        clearInvocations(channel)

        handler.onOutgoing(channel, businessMessage(3).asExpandable(), Collections.emptyMap());
        handler.onOutgoing(channel, businessMessage(4).asExpandable(), Collections.emptyMap());
        handler.onOutgoing(channel, businessMessage(5).asExpandable(), Collections.emptyMap())

        verify(channel, timeout(2100)).open()
        verify(channel).send(any(), any(), anyOrNull(), any()) // Logon
        clearInvocations(channel)
        messages.clear()
        handler.onIncoming(channel, resendRequest(5, 3, 5))

        messages.forEachIndexed { idx, message ->
            val buff = message.first
            assertTrue { buff.contains("35=AE") }
            assertTrue { buff.contains("43=Y") }
            assertTrue { buff.contains("122=") }
            assertTrue { buff.contains("34=${idx + 3}") }
        }

        channel.close()
    }

    @Test
    fun testClientOutage() {
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.CLIENT_OUTAGE,
                        duration = Duration.of(5, ChronoUnit.SECONDS),
                        cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                    ),
                )
            ),
            1
        ) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        Thread.sleep(2000) // Waiting for strategy to apply
        messages.clear()

        handler.onIncoming(channel, testRequest(4).asExpandable());
        handler.onIncoming(channel, testRequest(5).asExpandable());
        handler.onIncoming(channel, testRequest(6).asExpandable())

        clearInvocations(channel)

        for (message in messages) {
            val buff = message.first
            assertTrue { !(buff.contains("35=AE") || buff.contains("35=0")) }
        }

        channel.close()
    }

    @Test
    fun testPartialClientOutage() {
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.PARTIAL_CLIENT_OUTAGE,
                        duration = Duration.of(5, ChronoUnit.SECONDS),
                        cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                    ),
                )
            ),
            1
        ) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        Thread.sleep(2100) // Waiting for strategy to apply
        messages.clear()

        handler.onIncoming(channel, testRequest(4).asExpandable());
        handler.onIncoming(channel, testRequest(5).asExpandable());
        handler.onIncoming(channel, testRequest(6).asExpandable())

        clearInvocations(channel)

        for (message in messages) {
            val buff = message.first
            if(buff.isEmpty()) continue
            assertTrue { buff.contains("35=0") && buff.contains("112=test") }
        }

        channel.close()
    }

    @Test
    fun testSequenceResetStrategyOutgoing() {
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.SEQUENCE_RESET,
                        duration = Duration.of(5, ChronoUnit.SECONDS),
                        cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                        changeSequenceConfiguration = ChangeSequenceConfiguration(5, false)
                    ),
                )
            )
        ) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        verify(channel, timeout(2100)).open()
        messages.clear()
        verify(channel).send(any(), any(), anyOrNull(), any()) // Logon

       handler.onIncoming(channel, resendRequest(3, 3, 8))

        clearInvocations(channel)

        assertEquals(1, messages.size)
        messages[0].first.apply {
            assertTrue { contains("35=4") && contains("34=3") && contains("36=8") }
        }

        channel.close()
    }

    @Test
    fun testResendRequestStrategy() {
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.RESEND_REQUEST,
                        duration = Duration.of(5, ChronoUnit.SECONDS),
                        cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                        resendRequestConfiguration = ResendRequestConfiguration(5)
                    ),
                )
            )
        ) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        val captor = argumentCaptor<ByteBuf> {}

        handler.onIncoming(channel, businessMessage(3).asExpandable());
        handler.onIncoming(channel, businessMessage(4).asExpandable());
        handler.onIncoming(channel, businessMessage(5).asExpandable());
        handler.onIncoming(channel, businessMessage(6).asExpandable());
        handler.onIncoming(channel, businessMessage(7).asExpandable());
        verify(channel, timeout(2100).times(1)).send(captor.capture(), any(), anyOrNull(), any())

        captor.firstValue.apply {
            assertTrue { contains("35=2") && contains("7=2") && contains("16=7") }
        }

        clearInvocations(channel)

        channel.close()
    }

    private fun createTestContext(strategyConfig: BrokenConnConfiguration, hbtInt: Int = 30, enableAdditionalHandling: Boolean = true, sendHandlerExtension: (ByteBuf, Map<String, String>, IChannel.SendMode) -> Unit = {_, _, _ ->}): TestContext {
        val handlerSettings = createHandlerSettings(strategyConfig, hbtInt)
        val channel: IChannel = mock {}
        val context: IHandlerContext = mock {
            on { settings }.thenReturn(handlerSettings)
            on { createChannel(any(), any(), any(), any(), any(), any(), any()) }.thenReturn(channel)
        }
        var outgoingSequence = 1
        val handler = FixHandler(context)
        val onSendHandler: (ByteBuf, IChannel.SendMode) -> CompletableFuture<MessageID> = { msg, mode ->
            outgoingSequence++
            if(enableAdditionalHandling) {
                val expanded = msg.asExpandable()
                val metadata = mutableMapOf<String, String>()
                if(mode == IChannel.SendMode.HANDLE || mode == IChannel.SendMode.HANDLE_AND_MANGLE) {
                    handler.onOutgoing(channel, expanded, metadata)
                }
                sendHandlerExtension(expanded, metadata, mode)
            }
            if(msg.contains("35=A\u0001")) {
                handler.onIncoming(channel, logonResponse(outgoingSequence))
            }
            CompletableFuture.completedFuture(MessageID.getDefaultInstance())
        }
        var isOpen = false
        whenever(channel.send(any(), any(), anyOrNull(), any())).doAnswer {
            onSendHandler(it.arguments[0] as ByteBuf, it.arguments[3] as IChannel.SendMode)
        }
        whenever(channel.isOpen).doAnswer {
            isOpen
        }
        whenever(channel.open()).doAnswer {
            isOpen = true
            handler.onOpen(channel)
            mock {  }
        }
        whenever(channel.close()).doAnswer {
            isOpen = false
            handler.onClose(channel)
            mock {  }
        }

        channel.open()
        clearInvocations(channel)

        return TestContext(channel, handler)
    }

    companion object {
        private fun logonResponse(seq: Int): ByteBuf = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=105\u000135=A\u000134=${seq}\u000149=server\u000156=client\u000150=system\u000152=2014-12-22T10:15:30Z\u000198=0\u0001108=30\u00011137=9\u00011409=0\u000110=203\u0001".toByteArray(Charsets.US_ASCII))
        private fun businessMessage(seq: Int?, possDup: Boolean = false): ByteBuf = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=13\u000135=AE${if(seq != null) "\u000134=${seq}" else ""}${if(possDup) "\u000143=Y" else ""}\u0001552=1\u000110=169\u0001".toByteArray(Charsets.US_ASCII))
        private fun resendRequest(seq: Int, begin: Int, end: Int): ByteBuf = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=13\u000135=2\u000134=${seq}\u00017=${begin}\u000116=${end}\u0001552=1\u000110=169".toByteArray(Charsets.US_ASCII))
        private fun testRequest(seq: Int) = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=13\u000135=1\u000134=${seq}\u0001112=test\u00011552=1\u000110=169".toByteArray(Charsets.US_ASCII))
    }
}
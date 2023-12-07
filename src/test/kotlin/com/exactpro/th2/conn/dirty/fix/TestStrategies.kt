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
import com.exactpro.th2.TestUtils.createHandlerSettings
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BatchSendConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BlockMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BrokenConnConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ChangeSequenceConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.MissMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ResendRequestConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.SplitSendConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.TransformMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.TransformationConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.RuleType
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.SchedulerType
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerContext
import com.exactpro.th2.constants.Constants
import com.exactpro.th2.netty.bytebuf.util.asExpandable
import com.exactpro.th2.netty.bytebuf.util.contains
import com.exactpro.th2.netty.bytebuf.util.isEmpty
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import mu.KotlinLogging
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever

@Disabled
class TestStrategies {

    private class TestContext(
        val channel: IChannel,
        val fixHandler: FixHandler,
        val incomingSequence: AtomicInteger
    )

    @Test
    fun testDisconnectStrategy() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(5, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, ChronoUnit.SECONDS)
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, ChronoUnit.MILLIS)),
                RuleConfiguration(
                    RuleType.DISCONNECT_WITH_RECONNECT,
                    duration = businessRuleDuration,
                    cleanUpDuration = businessRuleCleanupDuration
                ),
            )
        ), enableAdditionalHandling = false)
        val channel = testContext.channel
        val handler = testContext.fixHandler

        verify(channel, timeout(defaultRuleDuration.millis() + 300)).close()

        handler.send(businessMessage(2), Collections.emptyMap(), null)

        verify(channel, timeout(businessRuleDuration.millis() + 300)).open()

        val captor = argumentCaptor<ByteBuf> {  }
        verify(channel, timeout(businessRuleCleanupDuration.millis() + 300).times(2)).send(captor.capture(), any(), anyOrNull(), any())

        captor.firstValue.apply {
            assertContains(mapOf(35 to "A"), this)
        }
        captor.secondValue.apply {
            assertContains(mapOf(35 to "AE"), this)
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testIgnoreIncomingMessagesStrategyWithNextExpectedSequenceNumber() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.IGNORE_INCOMING_MESSAGES,
                    duration = businessRuleDuration,
                    cleanUpDuration = businessRuleCleanupDuration,
                    missIncomingMessagesConfiguration = MissMessageConfiguration(3)
                ),
            ),
        ), useNextExpectedSeqNum = true) { msg, mtd, mode ->
            messages.add(Triple(msg, mtd, mode))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler
        val incomingSequence = testContext.incomingSequence

        val captor = argumentCaptor<ByteBuf> {  }

        verify(channel, timeout(defaultRuleDuration.millis() + 1300)).open()
        verify(channel, timeout(300)).send(any(), any(), anyOrNull(), any()) // Logon
        clearInvocations(channel)

        Thread.sleep(200) // wait for strategies to apply

        handler.onIncoming(channel, businessMessage(incomingSequence.incrementAndGet()), getMessageId())
        handler.onIncoming(channel, businessMessage(incomingSequence.incrementAndGet()), getMessageId())
        handler.onIncoming(channel, businessMessage(incomingSequence.incrementAndGet()), getMessageId())

        verify(channel, timeout(businessRuleDuration.millis() + businessRuleCleanupDuration.millis() + 1300)).open()
        verify(channel, timeout(300)).send(captor.capture(), any(), anyOrNull(), any()) // Logon
        clearInvocations(channel)

        captor.firstValue.apply {
            assertContains(mapOf(35 to "A", 789 to "3"), this)
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testIgnoreIncomingMessagesStrategyResendRequest() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()

        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.IGNORE_INCOMING_MESSAGES,
                    duration = businessRuleDuration,
                    cleanUpDuration = businessRuleCleanupDuration,
                    missIncomingMessagesConfiguration = MissMessageConfiguration(3)
                ),
            ),
        )) { msg, mtd, mode ->
            messages.add(Triple(msg, mtd, mode))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler
        val incomingSequence = testContext.incomingSequence

        val captor = argumentCaptor<ByteBuf> {  }

        clearInvocations(channel)
        verify(channel, timeout(defaultRuleDuration.millis() + 300)).open()
        verify(channel, timeout(300)).send(any(), any(), anyOrNull(), any()) // Logon // 2
        clearInvocations(channel)

        Thread.sleep(200) // wait for strategies to apply

        handler.onIncoming(channel, businessMessage(incomingSequence.incrementAndGet()), getMessageId()) // 3
        handler.onIncoming(channel, businessMessage(incomingSequence.incrementAndGet()), getMessageId()) // 4
        handler.onIncoming(channel, businessMessage(incomingSequence.incrementAndGet()), getMessageId()) // 5
        handler.onIncoming(channel, businessMessage(incomingSequence.incrementAndGet()), getMessageId()) // 6

        verify(channel, timeout(businessRuleDuration.millis() + businessRuleCleanupDuration.millis() + 300)).open()
        verify(channel, timeout(600).times(2)).send(captor.capture(), any(), anyOrNull(), any()) // Logon
        clearInvocations(channel)

        captor.firstValue.apply {
            assertContains(mapOf(35 to "2", 7 to "3", 16 to "5"), this)
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testTransformLogonMessagesStrategy() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.TRANSFORM_LOGON,
                    duration = businessRuleDuration,
                    cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS),
                    transformMessageConfiguration = TransformMessageConfiguration(
                        listOf(
                            TransformationConfiguration(
                                listOf(
                                    Action(
                                        replace = FieldSelector(
                                            tag = Constants.PASSWORD_TAG,
                                            matches = Pattern.compile("pass"),
                                            tagOneOf = null
                                        ),
                                        with = FieldDefinition(
                                            tag = Constants.PASSWORD_TAG,
                                            value = "mangledPassword",
                                            tagOneOf = null,
                                            valueOneOf = null
                                        )
                                    )
                            ), false, "A"),
                            TransformationConfiguration(
                                listOf(
                                    Action(
                                        replace = FieldSelector(
                                            tag = Constants.PASSWORD_TAG,
                                            matches = Pattern.compile("pass"),
                                            tagOneOf = null
                                        ),
                                        with = FieldDefinition(
                                            tag = Constants.PASSWORD_TAG,
                                            value = "mangledPassword",
                                            tagOneOf = null,
                                            valueOneOf = null
                                        )
                                    )
                                ), false, "A")
                        )
                    ),
                ),
            )
        )) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        messages.clear()
        verify(channel, timeout(defaultRuleDuration.millis() + businessRuleDuration.millis() + 300).times(3)).open()

        // start
        verify(channel, timeout(800).times(3)).send(any(), any(), anyOrNull(), any())

        messages[0].apply {
            assertContains(mapOf(35 to "A", Constants.PASSWORD_TAG to "mangledPassword"), this.first)
        }
        messages[1].apply {
            assertContains(mapOf(35 to "A", Constants.PASSWORD_TAG to "mangledPassword"), this.first)
        }
        messages[2].apply {
            assertContains(mapOf(35 to "A", Constants.PASSWORD_TAG to "pass"), this.first)
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testBidirectionalResendRequest() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = Duration.of(2, ChronoUnit.SECONDS), cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.BI_DIRECTIONAL_RESEND_REQUEST,
                    duration = Duration.of(6, ChronoUnit.SECONDS),
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
        clearInvocations(channel)
        verify(channel, timeout(defaultRuleDuration.millis() + 300)).open()
        verify(channel, timeout(600).times(1)).send(any(), any(), anyOrNull(), any())

        Thread.sleep(200) // wait for strategies to apply
        messages.clear()

        handler.onIncoming(channel, businessMessage(3), getMessageId())
        handler.onIncoming(channel, businessMessage(4), getMessageId())

        handler.onOutgoing(channel, businessMessage(3).asExpandable(), Collections.emptyMap())
        handler.onOutgoing(channel, businessMessage(4).asExpandable(), Collections.emptyMap())

        // Trigger resend request
        handler.onIncoming(channel, businessMessage(5), getMessageId())

        handler.onIncoming(channel, businessMessage(3, true), getMessageId())
        handler.onIncoming(channel, businessMessage(4, true), getMessageId())
        // end

        // Trigger recovery
        handler.onIncoming(channel, resendRequest(6, 3, 4), getMessageId())
        // end

        messages[0].apply {
            val buff = first
            assertContains(mapOf(35 to "2", 7 to "3", 16 to "4"), buff)
        }

        messages[1].apply {
            val buff = first
            assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "3"), buff)
        }

        messages[2].apply {
            val buff = first
            assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "4"), buff)
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testOutgoingGap() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(3, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(RuleType.DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(2, ChronoUnit.SECONDS)),
                RuleConfiguration(
                    RuleType.CREATE_OUTGOING_GAP,
                    duration = businessRuleDuration,
                    cleanUpDuration = businessRuleCleanupDuration,
                    missOutgoingMessagesConfiguration = MissMessageConfiguration(3)
                ),
            )
        )) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        verify(channel, timeout(defaultRuleDuration.millis() + 300)).open()
        verify(channel).send(any(), any(), anyOrNull(), any()) // Logon
        clearInvocations(channel)

        Thread.sleep(500) // Waiting for strategy to apply

        handler.onOutgoing(channel, businessMessage(3).asExpandable(), Collections.emptyMap())
        handler.onOutgoing(channel, businessMessage(4).asExpandable(), Collections.emptyMap())
        handler.onOutgoing(channel, businessMessage(5).asExpandable(), Collections.emptyMap())
        clearInvocations(channel)
        val captor = argumentCaptor<ByteBuf> {  }

        handler.onIncoming(channel, resendRequest(3, 3, 5), getMessageId())
        verify(channel, timeout(500).times(3)).send(captor.capture(), any(), anyOrNull(), any()) // recovery
        clearInvocations(channel)

        verify(channel, timeout(businessRuleDuration.millis() + 300)).open()
        verify(channel, timeout(300)).send(any(), any(), anyOrNull(), any()) // Logon
        clearInvocations(channel)
        messages.clear()

        captor.firstValue.apply {
            assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "3"), this)
        }

        captor.secondValue.apply {
            assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "4"), this)
        }

        captor.thirdValue.apply {
            assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "5"), this)
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testClientOutage() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.CLIENT_OUTAGE,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                    ),
                )
            ),
            1
        ) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler
        val seq = testContext.incomingSequence

        Thread.sleep(defaultRuleDuration.millis() + 100) // Waiting for strategy to apply
        messages.clear()

        handler.onIncoming(channel, testRequest(seq.incrementAndGet()).asExpandable(), getMessageId())
        handler.onIncoming(channel, testRequest(seq.incrementAndGet()).asExpandable(), getMessageId())
        handler.onIncoming(channel, testRequest(seq.incrementAndGet()).asExpandable(), getMessageId())

        handler.onClose(channel)
        handler.onOpen(channel)

        clearInvocations(channel)

        for (message in messages) {
            val buff = message.first
            assertTrue("Message shouldn't be heartbeat or AE: ${buff.toString(Charsets.US_ASCII)}") { !(buff.contains("35=AE") || buff.contains("35=0")) }
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testPartialClientOutage() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.PARTIAL_CLIENT_OUTAGE,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                    ),
                )
            ),
            1
        ) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler
        val seq = testContext.incomingSequence

        Thread.sleep(defaultRuleDuration.millis() + 100) // Waiting for strategy to apply
        messages.clear()

        handler.onIncoming(channel, testRequest(seq.incrementAndGet()).asExpandable(), getMessageId())
        handler.onIncoming(channel, testRequest(seq.incrementAndGet()).asExpandable(), getMessageId())
        handler.onIncoming(channel, testRequest(seq.incrementAndGet()).asExpandable(), getMessageId())

        handler.onClose(channel)
        handler.onOpen(channel)

        clearInvocations(channel)

        for (message in messages) {
            val buff = message.first
            if(buff.isEmpty()) continue
            if(!buff.contains("35=0")) continue
            assertContains(mapOf(35 to "0", 112 to "test"), buff)
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testSequenceResetStrategyOutgoing() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.SEQUENCE_RESET,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                        changeSequenceConfiguration = ChangeSequenceConfiguration(5, false)
                    ),
                )
            )
        ) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        verify(channel, timeout(defaultRuleDuration.millis() + businessRuleCleanupDuration.millis() + 300)).open()
        messages.clear()
        verify(channel, timeout(500)).send(any(), any(), anyOrNull(), any()) // Logon

        handler.onIncoming(channel, resendRequest(3, 3, 8), getMessageId())

        clearInvocations(channel)

        for(message in messages) {
            if(!message.first.contains("35=4")) continue
            assertContains(mapOf(35 to "4", 34 to "3", 36 to "8"), message.first)
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testResendRequestStrategy() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.RESEND_REQUEST,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
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

        handler.onIncoming(channel, businessMessage(2).asExpandable(), getMessageId())
        handler.onIncoming(channel, businessMessage(3).asExpandable(), getMessageId())
        handler.onIncoming(channel, businessMessage(4).asExpandable(), getMessageId())
        handler.onIncoming(channel, businessMessage(5).asExpandable(), getMessageId())
        handler.onIncoming(channel, businessMessage(6).asExpandable(), getMessageId())
        verify(channel, timeout(defaultRuleDuration.millis() + 300).times(1)).send(captor.capture(), any(), anyOrNull(), any())

        captor.firstValue.apply {
            assertContains(mapOf(35 to "2", 7 to "1", 16 to "6"), this)
        }

        clearInvocations(channel)

        handler.close()
        channel.close()
    }

    @Test
    fun testBatchSend() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.BATCH_SEND,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                        batchSendConfiguration = BatchSendConfiguration(3)
                    ),
                )
            )
        ) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler
        Thread.sleep(defaultRuleDuration.millis() + 300)
        clearInvocations(channel)

        handler.send(businessMessage(2).asExpandable(), mutableMapOf(), null)
        handler.send(businessMessage(3).asExpandable(), mutableMapOf(), null)
        handler.send(businessMessage(4).asExpandable(), mutableMapOf(), null)
        handler.send(businessMessage(5).asExpandable(), mutableMapOf(), null)

        val captor = argumentCaptor<ByteBuf> {}
        verify(channel, timeout(businessRuleDuration.millis() + 600).times(2)).send(captor.capture(), any(), anyOrNull(), any())

        val sizeOfOneMessage = businessMessage(2).asExpandable().also {
            handler.onOutgoingUpdateTag(it, mutableMapOf())
        }.readableBytes()

        captor.firstValue.apply {
            println(readableBytes())
            println(sizeOfOneMessage * 3)
            assertTrue { this.readableBytes() >=  sizeOfOneMessage * 3 }
        }

        captor.secondValue.apply {
            assertEquals(this.readableBytes(), sizeOfOneMessage)
        }

        handler.close()
        channel.close()
    }

    @Test
    fun testSplitSend() {
        val defaultRuleDuration = Duration.of(2, ChronoUnit.SECONDS)
        val businessRuleDuration = Duration.of(6, ChronoUnit.SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, ChronoUnit.SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val testContext = createTestContext(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(RuleType.DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, ChronoUnit.SECONDS)),
                    RuleConfiguration(
                        RuleType.SPLIT_SEND,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                        splitSendConfiguration = SplitSendConfiguration(3, 100)
                    ),
                )
            )
        ) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler
        Thread.sleep(defaultRuleDuration.millis() + 300)
        clearInvocations(channel)
        val businessMessage = businessMessage(2).asExpandable()

        handler.send(businessMessage, mutableMapOf(), null)

        val captor = argumentCaptor<ByteBuf> {}
        verify(channel, timeout(businessRuleDuration.millis() + 300).times(4)).send(captor.capture(), any(), anyOrNull(), any())

        val partSize = businessMessage.readableBytes() / 3

        captor.firstValue.apply {
            assertEquals(partSize, this.readableBytes())
        }

        captor.secondValue.apply {
            assertEquals(partSize, this.readableBytes())
        }

        captor.thirdValue.apply {
            assertTrue { this.readableBytes() >= partSize }
        }

        captor.allValues[3].apply {
            assertEquals(businessMessage.readableBytes(), this.readableBytes())
        }

        handler.close()
        channel.close()
    }

    private fun createTestContext(
        strategyConfig: BrokenConnConfiguration,
        hbtInt: Int = 30,
        enableAdditionalHandling: Boolean = true,
        useNextExpectedSeqNum: Boolean = false,
        sendHandlerExtension: (ByteBuf, Map<String, String>, IChannel.SendMode) -> Unit = {_, _, _ ->}): TestContext {
        val handlerSettings = createHandlerSettings(strategyConfig, hbtInt, useNextExpectedSeqNum)
        val channel: IChannel = mock {}
        val context: IHandlerContext = mock {
            on { settings }.thenReturn(handlerSettings)
            on { createChannel(any(), any(), any(), any(), any(), any(), any()) }.thenReturn(channel)
        }

        var incomingSequence = AtomicInteger(0)
        var outgoingSequence = 0

        val handler = FixHandler(context)
        val onSendHandler: (ByteBuf, IChannel.SendMode) -> CompletableFuture<MessageID> = { msg, mode ->
            outgoingSequence += 1
            if(enableAdditionalHandling) {
                val expanded = msg.copy().asExpandable()
                val metadata = mutableMapOf<String, String>()
                if(mode == IChannel.SendMode.HANDLE || mode == IChannel.SendMode.HANDLE_AND_MANGLE) {
                    handler.onOutgoing(channel, expanded, metadata)
                }
                sendHandlerExtension(expanded, metadata, mode)
            }
            if(msg.contains("35=A\u0001")) {
                if(useNextExpectedSeqNum) {
                    handler.onIncoming(channel, logonResponseWithNextExpectedSeq(incomingSequence.incrementAndGet(), outgoingSequence + 1), getMessageId())
                } else {
                    handler.onIncoming(channel, logonResponse(incomingSequence.incrementAndGet()), getMessageId())
                }
            }
            CompletableFuture.completedFuture(getMessageId())
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

        return TestContext(channel, handler, incomingSequence)
    }

    private fun assertContains(values: Map<Int, String>, message: ByteBuf) {
        val expected = values.map { "${it.key}=${it.value}" }.joinToString(",")
        assertTrue("Expected message to have $expected tags: ${message.toString(Charsets.US_ASCII)}") {
            values.all { message.contains("${it.key}=${it.value}") }
        }
    }

    companion object {
        private fun logonResponse(seq: Int): ByteBuf = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=105\u000135=A\u000134=${seq}\u000149=server\u000156=client\u000150=system\u000152=2014-12-22T10:15:30Z\u000198=0\u0001108=30\u00011137=9\u00011409=0\u000110=203\u0001".toByteArray(Charsets.US_ASCII))
        private fun businessMessage(seq: Int?, possDup: Boolean = false): ByteBuf = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=13\u000135=AE${if(seq != null) "\u000134=${seq}" else ""}${if(possDup) "\u000143=Y" else ""}\u0001552=1\u000110=169\u0001".toByteArray(Charsets.US_ASCII))
        private fun resendRequest(seq: Int, begin: Int, end: Int): ByteBuf = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=13\u000135=2\u000134=${seq}\u00017=${begin}\u000116=${end}\u0001552=1\u000110=169".toByteArray(Charsets.US_ASCII))
        private fun testRequest(seq: Int) = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=13\u000135=1\u000134=${seq}\u0001112=test\u00011552=1\u000110=169".toByteArray(Charsets.US_ASCII))
        private fun logonResponseWithNextExpectedSeq(seq: Int, nextExpected: Int) = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=105\u000135=A\u000134=${seq}\u0001789=${nextExpected}\u000149=server\u000156=client\u000150=system\u000152=2014-12-22T10:15:30Z\u000198=0\u0001108=30\u00011137=9\u00011409=0\u000110=203\u0001".toByteArray(Charsets.US_ASCII))
        private fun Duration.millis() = toMillis()
        private fun getMessageId() = MessageID.newBuilder().apply {
            sequence = System.nanoTime()
            bookName = "Test"
            direction = Direction.FIRST
            timestamp = Instant.now().toTimestamp()
            connectionId = ConnectionID.newBuilder().apply {
                sessionAlias = "SA"
                sessionGroup = "SG"
            }.build()
        }.build()
    }
}
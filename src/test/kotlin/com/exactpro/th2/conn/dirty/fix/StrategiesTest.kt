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
package com.exactpro.th2.conn.dirty.fix

import com.exactpro.th2.FixHandler
import com.exactpro.th2.FixHandlerSettings
import com.exactpro.th2.TestUtils.createHandlerSettings
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toByteArray
import com.exactpro.th2.common.utils.message.toTimestamp
import com.exactpro.th2.common.utils.shutdownGracefully
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BatchSendConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BrokenConnConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ChangeSequenceConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.MissMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RecoveryConfig
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.ResendRequestConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.SplitSendConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.TransformMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.TransformationConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.RuleType
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.RuleType.CREATE_OUTGOING_GAP
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.RuleType.DEFAULT
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.SchedulerType
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerContext
import com.exactpro.th2.constants.Constants
import com.exactpro.th2.constants.Constants.MSG_SEQ_NUM_TAG
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation
import com.exactpro.th2.netty.bytebuf.util.asExpandable
import com.exactpro.th2.netty.bytebuf.util.contains
import com.exactpro.th2.netty.bytebuf.util.isEmpty
import com.google.protobuf.UnsafeByteOperations
import com.google.protobuf.util.Timestamps
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import mu.KotlinLogging
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.anyVararg
import org.mockito.kotlin.argThat
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.description
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.timeout
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import org.mockito.kotlin.whenever
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit.MILLIS
import java.time.temporal.ChronoUnit.SECONDS
import java.util.Collections
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.Pattern
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.text.Charsets.US_ASCII
import com.exactpro.th2.common.grpc.Event as ProtoEvent

private const val TEST_BOOK = "test-book"
private const val TEST_SCOPE = "test-scope"
private const val TEST_SESSION_GROUP = "test-session-group"
private const val TEST_SESSION_ALIAS = "test-session-alias"

class StrategiesTest {

    private class TestContext(
        val context: IHandlerContext,
        val channel: IChannel,
        val fixHandler: FixHandler,
        val incomingSequence: AtomicInteger
    )

    @Test
    @Disabled
    fun testDisconnectStrategy() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(5, SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, SECONDS)
        val handlerSettings: FixHandlerSettings = createHandlerSettings(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, MILLIS)),
                RuleConfiguration(
                    RuleType.DISCONNECT_WITH_RECONNECT,
                    duration = businessRuleDuration,
                    cleanUpDuration = businessRuleCleanupDuration
                ),
            )
        ))
        val testContext = createTestContext(handlerSettings, enableAdditionalHandling = false)
        val channel = testContext.channel
        val handler = testContext.fixHandler

        verify(channel, timeout(defaultRuleDuration.millis() + 300)).close()

        handler.send(businessMessage(2), Collections.emptyMap(), null)

        verify(channel, timeout(businessRuleDuration.millis() + 300)).open()

        val captor = argumentCaptor<ByteBuf> {  }
        verify(channel, timeout(businessRuleCleanupDuration.millis() + 300).times(2)).send(captor.capture(), any(), anyOrNull(), any())

        captor.firstValue.apply {
            this.assertContains(mapOf(35 to "A"))
        }
        captor.secondValue.apply {
            this.assertContains(mapOf(35 to "AE"))
        }

        handler.close()
        channel.close()
    }

    @Test
    @Disabled
    fun testIgnoreIncomingMessagesStrategyWithNextExpectedSequenceNumber() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(6, SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(DEFAULT, duration = Duration.of(2, SECONDS), cleanUpDuration = Duration.of(0, SECONDS)),
                    RuleConfiguration(
                        RuleType.IGNORE_INCOMING_MESSAGES,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                        missIncomingMessagesConfiguration = MissMessageConfiguration(3)
                    ),
                ),
            ), 30, true
        )
        val testContext = createTestContext(handlerSettings) { msg, mtd, mode ->
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
            this.assertContains(mapOf(35 to "A", 789 to "3"))
        }

        handler.close()
        channel.close()
    }

    @Test
    @Disabled
    fun testIgnoreIncomingMessagesStrategyResendRequest() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(6, SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(DEFAULT, duration = Duration.of(2, SECONDS), cleanUpDuration = Duration.of(0, SECONDS)),
                RuleConfiguration(
                    RuleType.IGNORE_INCOMING_MESSAGES,
                    duration = businessRuleDuration,
                    cleanUpDuration = businessRuleCleanupDuration,
                    missIncomingMessagesConfiguration = MissMessageConfiguration(3)
                ),
            ),
        ))
        val testContext = createTestContext(handlerSettings) { msg, mtd, mode ->
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
            this.assertContains(mapOf(35 to "2", 7 to "3", 16 to "5"))
        }

        handler.close()
        channel.close()
    }

    @Test
    @Disabled
    fun testTransformLogonMessagesStrategy() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(6, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(DEFAULT, duration = Duration.of(2, SECONDS), cleanUpDuration = Duration.of(0, SECONDS)),
                    RuleConfiguration(
                        RuleType.TRANSFORM_LOGON,
                        duration = businessRuleDuration,
                        cleanUpDuration = Duration.of(2, SECONDS),
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
            )
        )
        val testContext = createTestContext(handlerSettings) { msg, mode, mtd ->
            messages.add(Triple(msg, mode, mtd))
        }

        val channel = testContext.channel
        val handler = testContext.fixHandler

        messages.clear()
        verify(channel, timeout(defaultRuleDuration.millis() + businessRuleDuration.millis() + 300).times(3)).open()

        // start
        verify(channel, timeout(800).times(3)).send(any(), any(), anyOrNull(), any())

        messages[0].apply {
            this.first.assertContains(mapOf(35 to "A", Constants.PASSWORD_TAG to "mangledPassword"))
        }
        messages[1].apply {
            this.first.assertContains(mapOf(35 to "A", Constants.PASSWORD_TAG to "mangledPassword"))
        }
        messages[2].apply {
            this.first.assertContains(mapOf(35 to "A", Constants.PASSWORD_TAG to "pass"))
        }

        handler.close()
        channel.close()
    }

    @Test
    @Disabled
    fun testBidirectionalResendRequest() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(DEFAULT, duration = Duration.of(2, SECONDS), cleanUpDuration = Duration.of(0, SECONDS)),
                RuleConfiguration(
                    RuleType.BI_DIRECTIONAL_RESEND_REQUEST,
                    duration = Duration.of(6, SECONDS),
                    cleanUpDuration = Duration.of(2, SECONDS),
                    missIncomingMessagesConfiguration = MissMessageConfiguration(2),
                    missOutgoingMessagesConfiguration = MissMessageConfiguration(2)
                ),
            )
        ))
        val testContext = createTestContext(handlerSettings) { msg, mode, mtd ->
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
            buff.assertContains(mapOf(35 to "2", 7 to "3", 16 to "4"))
        }

        messages[1].apply {
            val buff = first
            buff.assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "3"))
        }

        messages[2].apply {
            val buff = first
            buff.assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "4"))
        }

        handler.close()
        channel.close()
    }

    @Test
    fun `outgoing gap strategy test`() {
        val ruleDuration = Duration.of(500, MILLIS)
        val correction = ruleDuration.millis() / 2
        val handlerSettings: FixHandlerSettings = createHandlerSettings(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(DEFAULT, duration = ruleDuration, cleanUpDuration = Duration.ZERO),
                    RuleConfiguration(
                        CREATE_OUTGOING_GAP,
                        duration = ruleDuration,
                        cleanUpDuration = Duration.ZERO,
                        missOutgoingMessagesConfiguration = MissMessageConfiguration(3)
                    ),
                )
            )
        )
        val testContext = createTestContext(handlerSettings)

        val context = testContext.context
        val channel = testContext.channel

        testContext.fixHandler.use { handler ->
            handler.onStart()
            context.verifyInvocationsAndClean {
                verify(this).createChannel(any(), any(), any(), any(), any(), any(), anyVararg())
                channel.verifyInvocationsAndClean {
                    verify(
                        this,
                        times(2).description("Check channel state in the onStart and sendLogon methods"),
                    ).isOpen
                    verify(this, description("open channel with default strategy first")).open()
                    verifySend(mapOf(35 to "A", 34 to "1"), correction)
                }
                verifySendEvent("successful login", "Info", correction)
            }

            verifyChangeStrategy(channel, ruleDuration, DEFAULT, CREATE_OUTGOING_GAP, 2)
            context.verifyInvocationsAndClean {
                val events = captureSendEvents(2, correction)
                assertAll(
                    { assertEquals("successful login", events[0].name) },
                    { assertContains(events[1].name, "$CREATE_OUTGOING_GAP strategy started") },
                )
            }

            handler.onOutgoing(channel, businessMessage(3).asExpandable(), mutableMapOf())
            handler.onOutgoing(channel, businessMessage(4).asExpandable(), mutableMapOf())
            handler.onOutgoing(channel, businessMessage(5).asExpandable(), mutableMapOf())

            handler.onIncoming(channel, resendRequest(3, 3, 5), getMessageId())
            channel.verifyInvocationsAndClean {
                verify(this, timeout(correction).description("check status during handleResendRequest method")).isOpen
                val byteBufs = captureSend(3, correction)
                assertAll(
                    { byteBufs[0].assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "3")) },
                    { byteBufs[1].assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "4")) },
                    { byteBufs[2].assertContains(mapOf(35 to "AE", 43 to "Y", 34 to "5")) },
                )
            }

            verifyChangeStrategy(channel, ruleDuration, CREATE_OUTGOING_GAP, DEFAULT, 6)
            context.verifyInvocationsAndClean {
                val events = captureSendEvents(2, correction)
                assertAll(
                    { assertEquals("successful login", events[0].name) },
                    { assertContains(events[1].name, "$CREATE_OUTGOING_GAP strategy finished") },
                )
            }
        }
    }

    @Test
    fun `outgoing gap strategy - long recovery in case of mixing recovery message with non-recovery messages test`() {
        val ruleDuration = Duration.of(500, MILLIS)
        val correction = ruleDuration.millis() / 2
        val messages = CopyOnWriteArrayList<MessageSearchResponse>()
        val handlerSettings: FixHandlerSettings =
            createHandlerSettings(
                BrokenConnConfiguration(
                    SchedulerType.CONSECUTIVE,
                    listOf(
                        RuleConfiguration(DEFAULT, duration = ruleDuration, cleanUpDuration = Duration.ZERO),
                        RuleConfiguration(
                            CREATE_OUTGOING_GAP,
                            duration = ruleDuration,
                            cleanUpDuration = Duration.ZERO,
                            allowMessagesBeforeRetransmissionFinishes = true,
                            missOutgoingMessagesConfiguration = MissMessageConfiguration(3),
                        ),
                    ),
                ),
            ).apply {
                isLoadMissedMessagesFromCradle = true
                cradleSaveTimeoutMs = 500
            }
        val testContext =
            createTestContext(
                handlerSettings,
                searchMessageGroups = { request -> Thread.sleep(100); searchMessageGroups(messages, request, 1) },
            )

        val context = testContext.context
        val channel = testContext.channel
        val handler = testContext.fixHandler
        val executor = Executors.newCachedThreadPool()

        var msgSender: Future<*>? = null
        try {
            TestServerEmulator(
                executor,
                handler,
                channel,
                ::getMessageId,
                ::logonResponse,
                ::resendRequest,
            ).use { server ->
                whenever(channel.send(any(), any(), anyOrNull(), any())).doAnswer {
                    val sendMode = it.arguments[3] as IChannel.SendMode
                    val byteBuf = it.arguments[0] as ByteBuf
                    handleOutgoingMessages(handler, channel, messages, server, sendMode, byteBuf)
                }

                handler.use {
                    handler.onStart()
                    verify(context, timeout(ruleDuration.toMillis() + correction)).send(
                        argThat {
                            toProto(TEST_BOOK, TEST_SCOPE).name.contains("$CREATE_OUTGOING_GAP strategy started")
                        },
                        anyOrNull(),
                    )

                    msgSender = executor.submit { sendBusinessMessages(handler) }

                    verify(context, timeout(ruleDuration.toMillis() * 10)).send(
                        argThat {
                        toProto(TEST_BOOK, TEST_SCOPE).name.contains("$CREATE_OUTGOING_GAP strategy finished")
                    },
                            anyOrNull())

                }
            }
        } finally {
            msgSender?.cancel(true)
            executor.shutdownGracefully()
        }
    }

    private fun handleOutgoingMessages(
        handler: FixHandler,
        channel: IChannel,
        messages: MutableList<MessageSearchResponse>,
        server: TestServerEmulator,
        sendMode: IChannel.SendMode,
        byteBuf: ByteBuf,
    ): CompletableFuture<MessageID> = try {

        LOGGER.info { "put $sendMode ${byteBuf.toString(US_ASCII)}" }

        if (sendMode.handle) {
            handler.onOutgoing(channel, byteBuf, mutableMapOf())
        }

        if (!byteBuf.isEmpty()) {
            if (sendMode.mqPublish) {
                messages.add(
                    MessageSearchResponse
                        .newBuilder()
                        .apply {
                            messageBuilder.apply {
                                messageIdBuilder.apply {
                                    timestamp = Timestamps.now()
                                    sequence = byteBuf.findField(MSG_SEQ_NUM_TAG)?.value?.toLong() ?: 0L
                                }
                                bodyRaw = UnsafeByteOperations.unsafeWrap(byteBuf.toByteArray())
                            }
                        }.build(),
                )
                LOGGER.info { "publish to mq ${byteBuf.findField(MSG_SEQ_NUM_TAG)?.value}" }
            }

            if (sendMode.socketSend) {
                server.consume(byteBuf)
            }
        }

        CompletableFuture.completedFuture(getMessageId())
    } catch (e: InterruptedException) {
        Thread.currentThread().interrupt()
        throw e
    }

    @Test
    fun `outgoing gap strategy - long recovery in case of sequence reset for admin is false test`() {
        val ruleDuration = Duration.of(500, MILLIS)
        val correction = ruleDuration.millis() / 2
        val messages = CopyOnWriteArrayList<MessageSearchResponse>()
        val handlerSettings: FixHandlerSettings =
            createHandlerSettings(
                BrokenConnConfiguration(
                    SchedulerType.CONSECUTIVE,
                    listOf(
                        RuleConfiguration(DEFAULT, duration = ruleDuration, cleanUpDuration = Duration.ZERO),
                        RuleConfiguration(
                            CREATE_OUTGOING_GAP,
                            duration = ruleDuration,
                            cleanUpDuration = Duration.ZERO,
                            missOutgoingMessagesConfiguration = MissMessageConfiguration(3),
                            recoveryConfig = RecoveryConfig(
                                sequenceResetForAdmin = false
                            )
                        ),
                    ),
                ),
            ).apply {
                isLoadMissedMessagesFromCradle = true
                cradleSaveTimeoutMs = 500
            }
        val testContext =
            createTestContext(
                handlerSettings,
                searchMessageGroups = { request -> searchMessageGroups(messages, request) },
            )

        val context = testContext.context
        val channel = testContext.channel
        val handler = testContext.fixHandler
        val executor = Executors.newCachedThreadPool()

        var msgSender: Future<*>? = null
        try {
            TestServerEmulator(
                executor,
                handler,
                channel,
                ::getMessageId,
                ::logonResponse,
                ::resendRequest,
            ).use { server ->
                whenever(channel.send(any(), any(), anyOrNull(), any())).doAnswer {
                    val sendMode = it.arguments[3] as IChannel.SendMode
                    val byteBuf = it.arguments[0] as ByteBuf
                    handleOutgoingMessages(handler, channel, messages, server, sendMode, byteBuf)
                }

                handler.use {
                    handler.onStart()
                    verify(context, timeout(ruleDuration.toMillis() + correction)).send(
                        argThat {
                            toProto(TEST_BOOK, TEST_SCOPE).name.contains("$CREATE_OUTGOING_GAP strategy started")
                        },
                        anyOrNull(),
                    )

                    msgSender = executor.submit { sendBusinessMessages(handler) }

                    verify(context, timeout(ruleDuration.toMillis() * 10)).send(
                        argThat {
                            toProto(TEST_BOOK, TEST_SCOPE).name.contains("$CREATE_OUTGOING_GAP strategy finished")
                        },
                        anyOrNull())

                }
            }
        } finally {
            msgSender?.cancel(true)
            executor.shutdownGracefully()
        }
    }

    @Test
    fun `outgoing gap strategy - long recovery in case of out of order is true test`() {
        val ruleDuration = Duration.of(500, MILLIS)
        val correction = ruleDuration.millis() / 2
        val messages = CopyOnWriteArrayList<MessageSearchResponse>()
        val handlerSettings: FixHandlerSettings =
            createHandlerSettings(
                BrokenConnConfiguration(
                    SchedulerType.CONSECUTIVE,
                    listOf(
                        RuleConfiguration(DEFAULT, duration = ruleDuration, cleanUpDuration = Duration.ZERO),
                        RuleConfiguration(
                            CREATE_OUTGOING_GAP,
                            duration = ruleDuration,
                            cleanUpDuration = Duration.ZERO,
                            missOutgoingMessagesConfiguration = MissMessageConfiguration(3),
                            recoveryConfig = RecoveryConfig(
                                outOfOrder = true
                            )
                        ),
                    ),
                ),
            ).apply {
                isLoadMissedMessagesFromCradle = true
                cradleSaveTimeoutMs = 500
            }
        val testContext =
            createTestContext(
                handlerSettings,
                searchMessageGroups = { request -> searchMessageGroups(messages, request) },
            )

        val context = testContext.context
        val channel = testContext.channel
        val handler = testContext.fixHandler
        val executor = Executors.newCachedThreadPool()

        var msgSender: Future<*>? = null
        try {
            TestServerEmulator(
                executor,
                handler,
                channel,
                ::getMessageId,
                ::logonResponse,
                ::resendRequest,
            ).use { server ->
                whenever(channel.send(any(), any(), anyOrNull(), any())).doAnswer {
                    val sendMode = it.arguments[3] as IChannel.SendMode
                    val byteBuf = it.arguments[0] as ByteBuf
                    handleOutgoingMessages(handler, channel, messages, server, sendMode, byteBuf)
                }

                handler.use {
                    handler.onStart()
                    verify(context, timeout(ruleDuration.toMillis() + correction)).send(
                        argThat {
                            toProto(TEST_BOOK, TEST_SCOPE).name.contains("$CREATE_OUTGOING_GAP strategy started")
                        },
                        anyOrNull(),
                    )

                    msgSender = executor.submit { sendBusinessMessages(handler) }

                    verify(context, timeout(ruleDuration.toMillis() * 10)).send(
                        argThat {
                            toProto(TEST_BOOK, TEST_SCOPE).name.contains("$CREATE_OUTGOING_GAP strategy finished")
                        },
                        anyOrNull())

                }
            }
        } finally {
            msgSender?.cancel(true)
            executor.shutdownGracefully()
        }
    }

    private fun searchMessageGroups(
        messages: List<MessageSearchResponse>,
        request: MessageGroupsSearchRequest,
        delayMs: Long = 0,
    ): Iterator<MessageSearchResponse> {
        val from = if (request.hasStartTimestamp()) request.startTimestamp else null
        val to = if (request.hasEndTimestamp()) request.endTimestamp else null
        return when (request.searchDirection) {
            TimeRelation.NEXT ->
                messages
                    .toMutableList()
                    .asSequence()
                    .filter { response ->
                        (from == null || Timestamps.compare(from, response.message.messageId.timestamp) <= 0) &&
                                (to == null || Timestamps.compare(to, response.message.messageId.timestamp) >= 0)
                    }

            TimeRelation.PREVIOUS ->
                messages
                    .reversed()
                    .asSequence()
                    .filter { response ->
                        (from == null || Timestamps.compare(from, response.message.messageId.timestamp) >= 0) &&
                                (to == null || Timestamps.compare(to, response.message.messageId.timestamp) <= 0)
                    }

            else -> error("Unsupported search direction")
        }.onEach {
            Thread.sleep(delayMs)
        }.iterator()
    }

    private fun sendBusinessMessages(handler: FixHandler) {
        try {
            var seq = 2
            while (!Thread.currentThread().isInterrupted) {
                seq += 1
                handler.send(businessMessage(seq).asExpandable(), mutableMapOf(), null)
                Thread.sleep(1)
            }
        } catch (e: Exception) {
            LOGGER.error(e) { "Send message problem" }
        } finally {
            LOGGER.info { "Stopped sending" }
        }
    }

    private fun verifyChangeStrategy(
        channel: IChannel,
        ruleDuration: Duration,
        oldRule: RuleType,
        newRule: RuleType,
        logonSeq: Int,
    ) {
        val correction = ruleDuration.millis() / 2
        channel.verifyInvocationsAndClean {
            verify(
                this,
                timeout(ruleDuration.millis() + correction)
                    .description("close in disconnect method at the end of $oldRule strategy"),
            ).close()
            verify(
                this,
                timeout(correction).description("open channel with $newRule strategy"),
            ).open()
            verify(
                this,
                times(2).description("check status in sendLogon and openChannelAndWaitForLogon"),
            ).isOpen
            verifySend(mapOf(35 to "A", 34 to logonSeq.toString()), correction)
        }
    }

    @Test
    @Disabled
    fun testClientOutage() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(6, SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(BrokenConnConfiguration(
            SchedulerType.CONSECUTIVE,
            listOf(
                RuleConfiguration(DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, SECONDS)),
                RuleConfiguration(
                    RuleType.CLIENT_OUTAGE,
                    duration = businessRuleDuration,
                    cleanUpDuration = businessRuleCleanupDuration,
                ),
            )
        ), 1, false)
        val testContext = createTestContext(handlerSettings) { msg, mode, mtd ->
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
            assertTrue("Message shouldn't be heartbeat or AE: ${buff.toString(US_ASCII)}") { !(buff.contains("35=AE") || buff.contains("35=0")) }
        }

        handler.close()
        channel.close()
    }

    @Test
    @Disabled
    fun testPartialClientOutage() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(6, SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, SECONDS)),
                    RuleConfiguration(
                        RuleType.PARTIAL_CLIENT_OUTAGE,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                    ),
                )
            ), 1, false
        )
        val testContext = createTestContext(handlerSettings) { msg, mode, mtd ->
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
            buff.assertContains(mapOf(35 to "0", 112 to "test"))
        }

        handler.close()
        channel.close()
    }

    @Test
    @Disabled
    fun testSequenceResetStrategyOutgoing() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(6, SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, SECONDS)),
                    RuleConfiguration(
                        RuleType.SEQUENCE_RESET,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                        changeSequenceConfiguration = ChangeSequenceConfiguration(5, false)
                    ),
                )
            )
        )
        val testContext = createTestContext(handlerSettings) { msg, mode, mtd ->
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
            message.first.assertContains(mapOf(35 to "4", 34 to "3", 36 to "8"))
        }

        handler.close()
        channel.close()
    }

    @Test
    @Disabled
    fun testResendRequestStrategy() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(6, SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, SECONDS)),
                    RuleConfiguration(
                        RuleType.RESEND_REQUEST,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                        resendRequestConfiguration = ResendRequestConfiguration(5)
                    ),
                )
            )
        )
        val testContext = createTestContext(handlerSettings) { msg, mode, mtd ->
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
            this.assertContains(mapOf(35 to "2", 7 to "1", 16 to "6"))
        }

        clearInvocations(channel)

        handler.close()
        channel.close()
    }

    @Test
    @Disabled
    fun testBatchSend() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(6, SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, SECONDS)),
                    RuleConfiguration(
                        RuleType.BATCH_SEND,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                        batchSendConfiguration = BatchSendConfiguration(3)
                    ),
                )
            )
        )
        val testContext = createTestContext(handlerSettings) { msg, mode, mtd ->
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
    @Disabled
    fun testSplitSend() {
        val defaultRuleDuration = Duration.of(2, SECONDS)
        val businessRuleDuration = Duration.of(6, SECONDS)
        val businessRuleCleanupDuration = Duration.of(2, SECONDS)
        val messages = mutableListOf<Triple<ByteBuf, Map<String, String>, IChannel.SendMode>>()
        val handlerSettings: FixHandlerSettings = createHandlerSettings(
            BrokenConnConfiguration(
                SchedulerType.CONSECUTIVE,
                listOf(
                    RuleConfiguration(DEFAULT, duration = defaultRuleDuration, cleanUpDuration = Duration.of(0, SECONDS)),
                    RuleConfiguration(
                        RuleType.SPLIT_SEND,
                        duration = businessRuleDuration,
                        cleanUpDuration = businessRuleCleanupDuration,
                        splitSendConfiguration = SplitSendConfiguration(3, 100)
                    ),
                )
            )
        )
        val testContext = createTestContext(handlerSettings) { msg, mode, mtd ->
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
        handlerSettings: FixHandlerSettings,
        enableAdditionalHandling: Boolean = true,
        searchMessageGroups: (
            MessageGroupsSearchRequest,
        ) -> Iterator<MessageSearchResponse> = { emptyList<MessageSearchResponse>().iterator() },
        sendHandlerExtension: (ByteBuf, Map<String, String>, IChannel.SendMode) -> Unit = {_, _, _ ->}
    ): TestContext {
        val channel: IChannel = mock {
            on { sessionGroup }.thenReturn(TEST_SESSION_GROUP)
            on { sessionAlias }.thenReturn(TEST_SESSION_ALIAS)
        }
        val dataProviderService: DataProviderService = mock {
            on { this.searchMessageGroups(any()) }.doAnswer {
                searchMessageGroups(it.arguments.first() as MessageGroupsSearchRequest)
            }
        }
        val context: IHandlerContext = mock {
            on { bookName }.thenReturn(TEST_BOOK)
            on { settings }.thenReturn(handlerSettings)
            on { createChannel(any(), any(), any(), any(), any(), any(), anyVararg()) }.thenReturn(channel)
            on { getGrpcService(eq(DataProviderService::class.java)) }.thenReturn(dataProviderService)
        }

        val incomingSequence = AtomicInteger(0)
        var outgoingSequence = 0

        val handler = FixHandler(context)
        context.verifyInvocationsAndClean {
            verifySendEvent("Strategy root event", "Info", 100)
            verify(this).settings
            if (handlerSettings.isLoadMissedMessagesFromCradle) {
                verify(this).bookName
                verify(this).getGrpcService(eq(DataProviderService::class.java))

            }
        }

        val onSendHandler: (ByteBuf, IChannel.SendMode) -> CompletableFuture<MessageID> = { msg, mode ->
            LOGGER.info { "OnSendHandler ${msg.toString(US_ASCII)}" }
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
                if(handlerSettings.useNextExpectedSeqNum()) {
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

        return TestContext(context, channel, handler, incomingSequence)
    }

    private inline fun <T> T.verifyInvocationsAndClean(func: T.() -> Unit) {
        this.func()
        verifyNoMoreInteractions(this)
        clearInvocations(this)
    }

    private fun IChannel.verifySend(
        valueByTag: Map<Int, String>,
        timeout: Long = 0,
    ): IChannel =
        this.also {
            val captor = argumentCaptor<ByteBuf> { }
            verify(this, timeout(timeout)).send(captor.capture(), any(), anyOrNull(), any())
            captor.allValues.single().assertContains(valueByTag)
        }

    private fun IChannel.captureSend(
        times: Int,
        timeout: Long,
    ): List<ByteBuf> {
        val captor = argumentCaptor<ByteBuf> { }
        verify(this, timeout(timeout).times(times)).send(captor.capture(), any(), anyOrNull(), any())
        return captor.allValues
    }

    private fun IHandlerContext.verifySendEvent(
        name: String? = null,
        type: String? = null,
        timeout: Long = 0,
    ): IHandlerContext =
        this.also {
            val captor = argumentCaptor<Event> { }
            verify(this, timeout(timeout)).send(captor.capture(), anyOrNull())
            val event = captor.allValues.single().toProto(TEST_BOOK, TEST_SCOPE)
            assertAll(
                { name?.let { assertContains(event.name, name) } },
                { type?.let { assertContains(event.type, type) } },
            )
        }

    private fun IHandlerContext.captureSendEvents(
        times: Int,
        timeout: Long = 0,
    ): List<ProtoEvent> {
        val captor = argumentCaptor<Event> { }
        verify(this, timeout(timeout).times(times)).send(captor.capture(), anyOrNull())
        return captor.allValues.map { it.toProto(TEST_BOOK, TEST_SCOPE) }
    }

    private fun ByteBuf.assertContains(values: Map<Int, String>) {
        assertAll(
            values.map { (tag, value) ->
                {
                    val field = assertNotNull(findField(tag), "filed for tag: $tag")
                    assertEquals(value, field.value, "field value for tag: $tag")
                }
            }
        )
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        private fun logonResponse(seq: Int): ByteBuf = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=105\u000135=A\u000134=${seq}\u000149=server\u000156=client\u000150=system\u000152=2014-12-22T10:15:30Z\u000198=0\u0001108=30\u00011137=9\u00011409=0\u000110=203\u0001".toByteArray(
            US_ASCII
        ))
        private fun businessMessage(seq: Int?, possDup: Boolean = false): ByteBuf = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=13\u000135=AE${if(seq != null) "\u000134=${seq}" else ""}${if(possDup) "\u000143=Y" else ""}\u0001552=1\u000110=169\u0001".toByteArray(
            US_ASCII
        ))
        private fun resendRequest(seq: Int, begin: Int, end: Int): ByteBuf = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=13\u000135=2\u000134=${seq}\u00017=${begin}\u000116=${end}\u0001552=1\u000110=169".toByteArray(
            US_ASCII
        ))
        private fun testRequest(seq: Int) = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=13\u000135=1\u000134=${seq}\u0001112=test\u00011552=1\u000110=169".toByteArray(
            US_ASCII
        ))
        private fun logonResponseWithNextExpectedSeq(seq: Int, nextExpected: Int) = Unpooled.wrappedBuffer("8=FIXT.1.1\u00019=105\u000135=A\u000134=${seq}\u0001789=${nextExpected}\u000149=server\u000156=client\u000150=system\u000152=2014-12-22T10:15:30Z\u000198=0\u0001108=30\u00011137=9\u00011409=0\u000110=203\u0001".toByteArray(
            US_ASCII
        ))
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
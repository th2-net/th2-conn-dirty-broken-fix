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

import com.exactpro.th2.SequenceHolder
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.util.toInstant
import com.exactpro.th2.constants.Constants.IS_POSS_DUP
import com.exactpro.th2.constants.Constants.MSG_SEQ_NUM_TAG
import com.exactpro.th2.constants.Constants.POSS_DUP_TAG
import com.exactpro.th2.dataprovider.lw.grpc.DataProviderService
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageGroupsSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps.compare
import io.grpc.Context
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import mu.KotlinLogging
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import kotlin.text.Charsets.US_ASCII

class MessageLoader(
    private val executor: ScheduledExecutorService,
    private val dataProvider: DataProviderService,
    private val sessionStartTime: LocalTime?,
    private val bookName: String
) {
    private var sessionStart: ZonedDateTime
    private val searchLock = ReentrantLock()

    init {
        val today = LocalDate.now(ZoneOffset.UTC)
        val start = sessionStartTime?.atDate(today)
        val now = LocalDateTime.now(ZoneOffset.UTC)
        if(start == null) {
            sessionStart = OffsetDateTime
                .now(ZoneOffset.UTC)
                .with(LocalTime.now())
                .atZoneSameInstant(ZoneId.systemDefault())
        } else {
            sessionStart = if(start.isAfter(now)) {
                OffsetDateTime
                    .now(ZoneOffset.UTC)
                    .minusDays(1)
                    .with(sessionStartTime)
                    .atZoneSameInstant(ZoneId.systemDefault())
            } else {
                OffsetDateTime
                    .now(ZoneOffset.UTC)
                    .with(sessionStartTime)
                    .atZoneSameInstant(ZoneId.systemDefault())
            }
        }
    }

    private var sessionStartTimestamp = sessionStart
        .toInstant()
        .toTimestamp()

    private var previousDaySessionStart = sessionStart
        .minusDays(1)
        .toInstant()
        .toTimestamp()

    fun updateTime() {
        searchLock.withLock {
            sessionStart = ZonedDateTime
                .now(ZoneOffset.UTC)
                .with(OffsetTime.now(ZoneOffset.UTC))
            sessionStartTimestamp = sessionStart
                .toInstant()
                .toTimestamp()
            previousDaySessionStart = sessionStart
                .minusDays(1)
                .toInstant()
                .toTimestamp()
        }
    }

    fun loadInitialSequences(sessionGroup: String, sessionAlias: String): SequenceHolder = searchLock.withLock {
        val serverSeq = searchMessage(sessionGroup, sessionAlias, Direction.FIRST, false)
        val clientSeq = searchMessage(sessionGroup, sessionAlias, Direction.SECOND, true)
        K_LOGGER.info { "Loaded sequences: client sequence - $clientSeq; server sequence - $serverSeq" }
        return SequenceHolder(clientSeq, serverSeq)
    }

    fun processMessagesInRange(
        sessionGroup: String,
        sessionAlias: String,
        direction: Direction,
        fromSequence: Long,
        timeout: Long,
        processMessage: (ByteBuf) -> Boolean
    ) = searchLock.withLock {
        processMessagesInRangeInternal(sessionGroup, sessionAlias, direction, fromSequence, timeout, processMessage)
    }

    private fun processMessagesInRangeInternal(
        sessionGroup: String,
        sessionAlias: String,
        direction: Direction,
        fromSequence: Long,
        timeout: Long,
        processMessage: (ByteBuf) -> Boolean
    ) {
        val deadline = System.currentTimeMillis() + timeout
        var timestamp: Timestamp? = null
        var skipRetransmission = false
        ProviderCall.withCancellation {
            val backwardIterator = dataProvider.searchMessageGroups(
                createSearchGroupRequest(
                    from = Instant.now().toTimestamp(),
                    sessionGroup = sessionGroup,
                    sessionAlias = sessionAlias,
                    direction = direction
                ).also {
                    K_LOGGER.info { "Backward iterator params: sessionAlias - $sessionAlias from - ${it.startTimestamp} to - ${it.endTimestamp}" }
                }
            )

            val firstValidMessage = firstValidMessageDetails(backwardIterator)

            if (firstValidMessage == null) {
                K_LOGGER.info { "Not found valid messages to recover." }
                return@withCancellation
            }
            firstValidMessage.let {
                K_LOGGER.info { "Backward search. First valid message seq num: ${it.payloadSequence} timestamp: ${it.timestamp} cradle sequence: ${it.messageSequence}" }
            }

            var messagesToSkip = firstValidMessage.payloadSequence - fromSequence

            timestamp = firstValidMessage.timestamp
            var lastProcessedSequence = -1
            while (backwardIterator.hasNext() && messagesToSkip > 0) {
                val message = backwardIterator.next().message
                if(compare(message.messageId.timestamp, previousDaySessionStart) <= 0) {
                    continue
                }
                timestamp = message.messageId.timestamp
                val buf = Unpooled.copiedBuffer(message.bodyRaw.toByteArray())
                val sequence = buf.findField(MSG_SEQ_NUM_TAG)?.value?.toInt()

                K_LOGGER.debug { "Backward search: Skip message with sequence - $sequence" }

                messagesToSkip -= 1
                if(messagesToSkip == 0L) {

                    sequence ?: continue

                    if(sequence > 1 && lastProcessedSequence == 1 || sequence > 2 && lastProcessedSequence == 2) {
                        skipRetransmission = true
                        K_LOGGER.info { "Retransmission will be skipped. Not found valid message with sequence more than 1." }
                        return@withCancellation
                    }

                    lastProcessedSequence = sequence

                    if(checkPossDup(buf)) {
                        val validMessage = firstValidMessageDetails(backwardIterator) ?: break

                        timestamp = validMessage.timestamp
                        if(validMessage.payloadSequence <= fromSequence) {
                            K_LOGGER.info { "Found valid message with start recovery sequence: ${buf.toString(US_ASCII)}" }
                            break
                        } else {
                            messagesToSkip = validMessage.payloadSequence - fromSequence
                            K_LOGGER.info { "Adjusted number of messages to skip: $messagesToSkip using ${validMessage.payloadSequence} - $fromSequence" }
                        }

                    } else {

                        if(sequence <= fromSequence) {
                            K_LOGGER.info { "Found valid message with start recovery sequence: ${buf.toString(US_ASCII)}" }
                            break
                        } else {
                            messagesToSkip = sequence - fromSequence
                            K_LOGGER.info { "Adjusted number of messages to skip: $messagesToSkip using $sequence - $fromSequence" }
                        }
                    }
                }
            }
        }

        if(skipRetransmission) return

        val startSearchTimestamp = timestamp ?: return

        K_LOGGER.info { "Loading retransmission messages from ${startSearchTimestamp.toInstant()}" }

        withDeadline(deadline - System.currentTimeMillis()) {
            val iterator = dataProvider.searchMessageGroups(
                createSearchGroupRequest(
                    from = startSearchTimestamp,
                    to = Instant.now().toTimestamp(),
                    sessionGroup = sessionGroup,
                    sessionAlias = sessionAlias,
                    direction = direction,
                    timeRelation = TimeRelation.NEXT,
                    keepOpen = true,
                ).also {
                    K_LOGGER.info { "Forward iterator params: sessionAlias - $sessionAlias from - ${it.startTimestamp} to - ${it.endTimestamp}" }
                }
            )

            while (iterator.hasNext()) {
                val next = iterator.next().message
                if(next.messagePropertiesMap.getOrDefault("isCorruptedMessage", "N") == "Y") {
                    continue
                }
                val message = Unpooled.buffer().writeBytes(next.bodyRaw.toByteArray())
                K_LOGGER.info { "Sending message to recovery processor: ${message.toString(US_ASCII)}" }
                if (!processMessage(message)) break
            }
        }.onFailure {
            K_LOGGER.error(it) { "Search message request is interrupted" }
        }
    }

    private fun searchMessage(
        sessionGroup: String,
        sessionAlias: String,
        direction: Direction,
        checkPossFlag: Boolean
    ) = ProviderCall.withCancellation {
        searchMessage(
            dataProvider.searchMessageGroups(
                createSearchGroupRequest(
                    from = Instant.now().toTimestamp(),
                    sessionGroup = sessionGroup,
                    sessionAlias = sessionAlias,
                    direction = direction
                )
            ),
            checkPossFlag
        ) { _, seqNum -> seqNum?.toInt() ?: 0 }
    }

    private fun <T> searchMessage(
        iterator: Iterator<MessageSearchResponse>,
        checkPossFlag: Boolean = false,
        extractValue: (MessageGroupResponse?, String?) -> T
    ): T {
        var message: MessageGroupResponse?
        while (iterator.hasNext()) {
            message = iterator.next().message
            if(sessionStartTime != null && compare(sessionStartTimestamp, message.messageId.timestamp) > 0) {
                return extractValue(message, null)
            }

            val bodyRaw = Unpooled.copiedBuffer(message.bodyRaw.toByteArray())
            val seqNum = bodyRaw.findField(MSG_SEQ_NUM_TAG)?.value ?: continue

            if(checkPossFlag && checkPossDup(bodyRaw)) continue

            return extractValue(message, seqNum)
        }
        return extractValue(null, null)
    }

    private fun firstValidMessageDetails(iterator: Iterator<MessageSearchResponse>): MessageDetails? = searchMessage(
        iterator,
        true
    ) { message, seqNum ->
        if(message == null || seqNum == null) return@searchMessage null
        MessageDetails(seqNum.toInt(), message.messageId.sequence, message.messageId.timestamp)
    }

    private fun createSearchGroupRequest(
        from: Timestamp,
        to: Timestamp = previousDaySessionStart,
        sessionGroup: String,
        sessionAlias: String,
        direction: Direction,
        timeRelation: TimeRelation = TimeRelation.PREVIOUS,
        keepOpen: Boolean = false,
    ) = MessageGroupsSearchRequest.newBuilder().apply {
        startTimestamp = from
        endTimestamp = to
        addResponseFormats(BASE64_FORMAT)
        addStream(
            MessageStream.newBuilder()
                .setName(sessionAlias)
                .setDirection(direction)
        )
        addMessageGroup(MessageGroupsSearchRequest.Group.newBuilder().setName(sessionGroup))
        bookIdBuilder.name = bookName
        searchDirection = timeRelation
        this.keepOpen = keepOpen
    }.build()

    private fun checkPossDup(buf: ByteBuf): Boolean = buf.findField(POSS_DUP_TAG)?.value == IS_POSS_DUP

    data class MessageDetails(val payloadSequence: Int, val messageSequence: Long, val timestamp: Timestamp)

    private fun withDeadline(
        durationMs: Long,
        code: () -> Unit
    ): Result<Unit> {
        return try {
            K_LOGGER.info { "deadline $durationMs" }
            Context.current()
                .withCancellation()
                .withDeadlineAfter(durationMs, TimeUnit.MILLISECONDS, executor)
                .use { context ->
                    context.call { code() }
                    Result.success(Unit)
                }
        } catch (e: Exception) {
            Result.failure(e)
        }
    }

    companion object {
        val K_LOGGER = KotlinLogging.logger {  }
        private const val BASE64_FORMAT = "BASE_64"
    }
}
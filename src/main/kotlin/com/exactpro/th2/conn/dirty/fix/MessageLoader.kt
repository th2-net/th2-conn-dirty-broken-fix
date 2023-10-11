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
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchRequest
import com.exactpro.th2.dataprovider.lw.grpc.MessageSearchResponse
import com.exactpro.th2.dataprovider.lw.grpc.MessageStream
import com.exactpro.th2.dataprovider.lw.grpc.TimeRelation
***REMOVED***
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps.compare
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

class MessageLoader(
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

    fun loadInitialSequences(sessionAlias: String): SequenceHolder = searchLock.withLock {
        val serverSeq = ProviderCall.withCancellation {
            searchMessage(
                dataProvider.searchMessages(
                    createSearchRequest(
                        Instant.now().toTimestamp(),
                        Direction.FIRST,
                        sessionAlias
                    )
                )
            ) {  _, seqNum -> seqNum?.toInt() ?: 0 }
        }
        val clientSeq = ProviderCall.withCancellation {
            searchMessage(
                dataProvider.searchMessages(
                    createSearchRequest(
                        Instant.now().toTimestamp(),
                        Direction.SECOND,
                        sessionAlias
                    )
                ),
                true
            ) { _, seqNum -> seqNum?.toInt() ?: 0 }
        }
        K_LOGGER.info { "Loaded sequences: client sequence - $clientSeq; server sequence - $serverSeq" }
        return SequenceHolder(clientSeq, serverSeq)
    }

    fun processMessagesInRange(
        direction: Direction,
        sessionAlias: String,
        fromSequence: Long,
        processMessage: (ByteBuf) -> Boolean
    ) = searchLock.withLock {
        processMessagesInRangeInternal(direction, sessionAlias, fromSequence, processMessage)
    }

    fun processMessagesInRangeInternal(
        direction: Direction,
        sessionAlias: String,
        fromSequence: Long,
        processMessage: (ByteBuf) -> Boolean
    ) {
        var timestamp: Timestamp? = null
        ProviderCall.withCancellation {
            val backwardIterator = dataProvider.searchMessages(
                createSearchRequest(Instant.now().toTimestamp(), direction, sessionAlias)
            )

            val firstValidMessage = firstValidMessageDetails(backwardIterator) ?: return@withCancellation

            var messagesToSkip = firstValidMessage.payloadSequence - fromSequence

            timestamp = firstValidMessage.timestamp

            while (backwardIterator.hasNext() && messagesToSkip > 0) {
                val message = backwardIterator.next().message
                if(compare(message.messageId.timestamp, previousDaySessionStart) <= 0) {
                    continue
                }
                timestamp = message.messageId.timestamp
                messagesToSkip -= 1
                if(messagesToSkip == 0L) {

                    val buf = Unpooled.copiedBuffer(message.bodyRaw.toByteArray())
                    val sequence = buf.findField(MSG_SEQ_NUM_TAG)?.value?.toInt() ?: continue

                    if(checkPossDup(buf)) {
                        val validMessage = firstValidMessageDetails(backwardIterator) ?: break

                        timestamp = validMessage.timestamp
                        if(validMessage.payloadSequence <= fromSequence) {
                            break
                        } else {
                            messagesToSkip = validMessage.payloadSequence - fromSequence
                        }

                    } else {

                        if(sequence <= fromSequence) {
                            break
                        } else {
                            messagesToSkip = sequence - fromSequence
                        }
                    }
                }
            }
        }

        val startSearchTimestamp = timestamp ?: return

        K_LOGGER.info { "Loading retransmission messages from ${startSearchTimestamp.toInstant()}" }

        ProviderCall.withCancellation {

            val iterator = dataProvider.searchMessages(
                createSearchRequest(
                    startSearchTimestamp,
                    direction,
                    sessionAlias,
                    TimeRelation.NEXT,
                    Instant.now().toTimestamp()
                )
            )

            while (iterator.hasNext()) {
                val message = Unpooled.buffer().writeBytes(iterator.next().message.bodyRaw.toByteArray())
                if (!processMessage(message)) break
            }
        }
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

    private fun createSearchRequest(
        timestamp: Timestamp,
        direction: Direction,
        sessionAlias: String,
        searchDirection: TimeRelation = TimeRelation.PREVIOUS,
        endTimestamp: Timestamp = previousDaySessionStart
    ) = MessageSearchRequest.newBuilder().apply {
        startTimestamp = timestamp
        this.endTimestamp = endTimestamp
        addResponseFormats(BASE64_FORMAT)
        addStream(
            MessageStream.newBuilder()
                .setName(sessionAlias)
                .setDirection(direction)
        )
        bookIdBuilder.name = bookName
        this.searchDirection = searchDirection
    }.build()

    private fun checkPossDup(buf: ByteBuf): Boolean = buf.findField(POSS_DUP_TAG)?.value == IS_POSS_DUP

    data class MessageDetails(val payloadSequence: Int, val messageSequence: Long, val timestamp: Timestamp)

    companion object {
        val K_LOGGER = KotlinLogging.logger {  }
        private const val BASE64_FORMAT = "BASE_64"
    }
}
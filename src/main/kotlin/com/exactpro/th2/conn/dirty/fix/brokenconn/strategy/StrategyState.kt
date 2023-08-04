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
package com.exactpro.th2.conn.dirty.fix.brokenconn.strategy

import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration
import com.google.protobuf.TextFormat.shortDebugString
import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import java.time.Instant
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import mu.KotlinLogging

class StrategyState(val config: RuleConfiguration? = null) {
    val startTime: Instant = Instant.now()
    val type = config?.ruleType ?: RuleType.DEFAULT
    val batchMessageCache: CompositeByteBuf = Unpooled.compositeBuffer()
    val messageIDs = Collections.synchronizedList(ArrayList<MessageID>())


    private val writeLock = ReentrantLock()
    private val missedMessagesCache: MutableMap<Int, ByteBuf> = ConcurrentHashMap<Int, ByteBuf>()

    private val batchMessageCacheSize = AtomicInteger(0)
    fun getBatchMessageCacheSize() = batchMessageCacheSize.get()

    private val missedIncomingMessagesCount = AtomicInteger(0)
    fun getMissedIncomingMessagesCount() = missedIncomingMessagesCount.get()

    private val missedOutgoingMessagesCount = AtomicInteger(0)
    fun getMissedOutgoingMessagesCount() = missedOutgoingMessagesCount.get()

    val transformedIncomingMessagesCount = AtomicInteger(0)

    fun incrementMissedIncomingMessages() { missedIncomingMessagesCount.incrementAndGet() }
    fun incrementIncomingTransformedMessages() { transformedIncomingMessagesCount.incrementAndGet() }

    fun addMissedMessageToCache(sequence: Int, message: ByteBuf) {
        writeLock.withLock {
            missedMessagesCache[sequence] = message
            missedOutgoingMessagesCount.incrementAndGet()
        }
    }

    fun getMissedMessage(sequence: Int): ByteBuf? = missedMessagesCache[sequence]

    fun addMessageToBatchCache(message: ByteBuf) {
        writeLock.withLock {
            batchMessageCacheSize.incrementAndGet()
            batchMessageCache.addComponent(message)
        }
    }

    fun resetBatchMessageCache() {
        writeLock.withLock {
            batchMessageCacheSize.set(0)
            batchMessageCache.clear()
        }
    }

    fun addMessageID(messageID: MessageID?) {
        writeLock.withLock {
            if(messageIDs.size + 1 >= TOO_BIG_MESSAGE_IDS_LIST) {
                K_LOGGER.warn { "Strategy ${type} messageIDs list is too big. Skiping messageID: ${shortDebugString(messageID)}" }
            }
            messageID?.let { messageIDs.add(it) }
        }
    }

    companion object {
        private const val TOO_BIG_MESSAGE_IDS_LIST = 300;
        private val K_LOGGER = KotlinLogging.logger {  }
    }
}
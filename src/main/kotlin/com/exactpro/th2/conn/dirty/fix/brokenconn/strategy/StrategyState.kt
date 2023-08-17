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
import com.exactpro.th2.netty.bytebuf.util.asExpandable
import com.google.protobuf.TextFormat.shortDebugString
import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import java.time.Instant
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import mu.KotlinLogging

class StrategyState(val config: RuleConfiguration? = null) {
    val startTime: Instant = Instant.now()
    val type = config?.ruleType ?: RuleType.DEFAULT
    val batchMessageCache: CompositeByteBuf = Unpooled.compositeBuffer()
    val messageIDs: MutableList<MessageID> = Collections.synchronizedList(ArrayList<MessageID>())

    private val writeLock = ReentrantLock()
    private val missedMessagesCache: MutableMap<Long, ByteBuf> = ConcurrentHashMap<Long, ByteBuf>()
    private val batchMessageCacheSize = AtomicInteger(0)

    private val missedIncomingMessagesCount = AtomicInteger(0)
    fun updateMissedIncomingMessagesCountIfCondition(condition: (Int) -> Boolean): Boolean = writeLock.withLock {
        var updated = false
        missedIncomingMessagesCount.updateAndGet {
            if(condition(it + 1)) {
                updated = true
                it + 1
            } else {
                it
            }
        }
        updated
    }

    private val transformedIncomingMessagesCount = AtomicInteger(0)
    fun getTransformedIncomingMessagesCount() = writeLock.withLock { transformedIncomingMessagesCount.get() }
    fun transformIfCondition(condition: (Int) -> Boolean, transform: () -> Unit): Boolean = writeLock.withLock {
        var updated = false
        transformedIncomingMessagesCount.updateAndGet {
            if(condition(it + 1)) {
                updated = true
                transform()
                it + 1
            } else {
                it
            }
        }
        updated
    }

    private val missedOutgoingMessagesCount = AtomicInteger(0)
    fun addMissedMessageToCacheIfCondition(sequence: Long, message: ByteBuf, condition: (Int) -> Boolean): Boolean {
        writeLock.withLock {
            var updated = false
            missedOutgoingMessagesCount.updateAndGet {
                if(condition(it + 1)) {
                    missedMessagesCache[sequence] = message
                    updated = true
                    it + 1
                } else {
                    it
                }
            }
            return updated
        }
    }

    fun getMissedMessage(sequence: Long): ByteBuf? = missedMessagesCache[sequence]

    fun updateCacheAndRunOnCondition(message: ByteBuf, condition: (Int) -> Boolean, function: (ByteBuf) -> Unit) {
        writeLock.withLock {
            batchMessageCacheSize.updateAndGet {
                batchMessageCache.addComponent(true, message.copy().asExpandable())
                if(condition(it + 1)) {
                    function(batchMessageCache.copy())
                    batchMessageCache.clear()
                    0
                } else {
                    it + 1
                }
            }
        }
    }

    fun executeOnBatchCacheIfCondition(condition: (Int) -> Boolean, function: (ByteBuf) -> Unit) {
        writeLock.withLock {
            if(condition(batchMessageCacheSize.get())) {
                function(batchMessageCache.copy())
                batchMessageCacheSize.set(0)
                batchMessageCache.clear()
            }
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
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

import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration
import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class StrategyState(val config: RuleConfiguration? = null) {
    val startTime: Instant = Instant.now()
    val type = config?.ruleType ?: RuleType.DEFAULT
    val batchMessageCache: CompositeByteBuf = Unpooled.compositeBuffer()

    private val writeLock = ReentrantLock()
    private val missedMessagesCache: MutableMap<Int, ByteBuf> = ConcurrentHashMap<Int, ByteBuf>()

    val batchMessageCacheSize = AtomicInteger(0)

    val missedIncomingMessagesCount = AtomicInteger(0)

    val missedOutgoingMessagesCount = AtomicInteger(0)

    val transformedIncomingMessagesCount = AtomicInteger(0)

    fun incrementMissedIncomingMessages() { missedIncomingMessagesCount.incrementAndGet() }
    fun incrementMissedOutgoingMessages() { missedOutgoingMessagesCount.incrementAndGet() }
    fun incrementIncomingTransformedMessages() { transformedIncomingMessagesCount.incrementAndGet() }

    fun addMissedMessageToCache(sequence: Int, message: ByteBuf) = missedMessagesCache.put(sequence, message)
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
}
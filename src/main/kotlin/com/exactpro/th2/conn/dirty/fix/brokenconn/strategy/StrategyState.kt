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
package com.exactpro.th2.conn.dirty.fix.brokenconn.strategy

import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration
import com.exactpro.th2.netty.bytebuf.util.asExpandable
import io.netty.buffer.ByteBuf
import io.netty.buffer.CompositeByteBuf
import io.netty.buffer.Unpooled
import mu.KotlinLogging
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

class StrategyState(val config: RuleConfiguration? = null,
                    private val missedMessagesCache: ConcurrentHashMap<Long, ByteBuf> = ConcurrentHashMap()
) {
    val startTime: Instant = Instant.now()
    val type = config?.ruleType ?: RuleType.DEFAULT
    private val batchMessageCache: CompositeByteBuf = Unpooled.compositeBuffer()
    private val messageIDs: MutableList<MessageID> = ArrayList()

    private val lock = ReentrantReadWriteLock()
    private var batchMessageCacheSize = 0

    private var missedIncomingMessagesCount = 0
    fun updateMissedIncomingMessagesCountIfCondition(condition: (Int) -> Boolean): Boolean = lock.write {
        if (condition(missedIncomingMessagesCount + 1)) {
            missedIncomingMessagesCount += 1
            true
        } else {
            false
        }
    }

    private var transformedIncomingMessagesCount = 0
    fun getTransformedIncomingMessagesCount() = lock.read { transformedIncomingMessagesCount }
    fun transformIfCondition(condition: (Int) -> Boolean, transform: () -> Unit): Boolean = lock.write {
        if (condition(transformedIncomingMessagesCount + 1)) {
            try {
                transform()
            } catch (e: Exception) {
                K_LOGGER.error(e) { "Error while transforming message" }
            }
            transformedIncomingMessagesCount += 1
            true
        } else {
            false
        }
    }

    private var missedOutgoingMessagesCount = 0
    fun addMissedMessageToCacheIfCondition(sequence: Long, message: ByteBuf, condition: (Int) -> Boolean): Boolean =
        lock.write {
            if (condition(missedOutgoingMessagesCount + 1)) {
                missedOutgoingMessagesCount += 1
                missedMessagesCache[sequence] = message
                true
            } else {
                false
            }
        }

    fun getMissedMessage(sequence: Long): ByteBuf? = lock.read { missedMessagesCache.remove(sequence) }

    fun updateCacheAndRunOnCondition(message: ByteBuf, condition: (Int) -> Boolean, function: (ByteBuf) -> Unit) = lock.write {
        batchMessageCache.addComponent(true, message.copy().asExpandable())
        if(condition(batchMessageCacheSize + 1)) {
            function(batchMessageCache.copy())
            batchMessageCache.removeComponents(0, batchMessageCache.numComponents())
            batchMessageCache.clear()
            batchMessageCacheSize = 0
        } else {
            batchMessageCacheSize += 1
        }
    }

    fun executeOnBatchCacheIfCondition(condition: (Int) -> Boolean, function: (ByteBuf) -> Unit) = lock.write {
        if(condition(batchMessageCacheSize)) {
            function(batchMessageCache.copy())
            batchMessageCache.removeComponents(0, batchMessageCache.numComponents())
            batchMessageCacheSize = 0
            batchMessageCache.clear()
        }
    }

    @JvmOverloads
    fun enrichProperties(properties: MutableMap<String, String>? = null): MutableMap<String, String> {
        if (type != RuleType.DEFAULT) {
            return properties?.apply {
                val previous = put(STRATEGY_PROPERTY, type.name)
                when (previous) {
                    null -> { /* do noting */ }
                    type.name -> K_LOGGER.debug { "Strategy name $type is already set" }
                    else -> K_LOGGER.warn { "Strategy name $properties has been replaced to $type" }
                }
            } ?: hashMapOf(STRATEGY_PROPERTY to type.name)
        }
        return hashMapOf()
    }

    fun addMessageID(messageID: MessageID?) = lock.write {
        if (messageIDs.size + 1 >= TOO_BIG_MESSAGE_IDS_LIST) {
            K_LOGGER.warn { "Strategy $type messageIDs list is too big. Skipping messageID: ${messageID?.toJson()}" }
        }
        messageID?.let { messageIDs.add(it) }
    }

    fun getMessageIDs() = lock.read {
        ArrayList(messageIDs)
    }

    companion object {
        private const val STRATEGY_PROPERTY: String = "th2.broken.strategy"
        private const val TOO_BIG_MESSAGE_IDS_LIST = 300
        private val K_LOGGER = KotlinLogging.logger {  }

        fun StrategyState.resetAndCopyMissedMessages(ruleConfiguration: RuleConfiguration? = null): StrategyState = StrategyState(ruleConfiguration, this.missedMessagesCache)
    }
}
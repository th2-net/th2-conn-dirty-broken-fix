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

import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BatchSendConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BlockMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.MissMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.SplitSendConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.TransformMessageConfiguration
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api.CleanupHandler
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api.MessageProcessor
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api.RecoveryHandler
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

class StatefulStrategy(
    initialSendStrategy: SendStrategy,
    initialIncomingMessagesStrategy: IncomingMessagesStrategy,
    initialOutgoingMessagesStrategy: OutgoingMessagesStrategy,
    initialReceiveStrategy: ReceiveStrategy,
    initialCleanupHandler: CleanupHandler,
    initialRecoveryHandler: RecoveryHandler,
    private val defaultStrategy: DefaultStrategyHolder
) {
    private val lock = ReentrantReadWriteLock()
    private val stateReadLock = lock.readLock()
    private val stateWriteLock = lock.writeLock()

    // configurations
    var blockIncomingMessagesConfiguration: BlockMessageConfiguration? = null
        get() = state.config?.blockOutgoingMessagesConfiguration ?: error("Block outgoing messages config isn't present.")
        private set
    var blockOutgoingMessagesConfiguration: BlockMessageConfiguration? = null
        get() = state.config?.blockOutgoingMessagesConfiguration ?: error("Block outgoing messages config isn't present.")
        private set
    var missIncomingMessagesConfig: MissMessageConfiguration? = null
        get() = state.config?.missIncomingMessagesConfiguration ?: error("Miss incoming messages config isn't present.")
        private set
    var missOutgoingMessagesConfiguration: MissMessageConfiguration? = null
        get() = state.config?.missOutgoingMessagesConfiguration ?: error("Miss incoming messages config isn't present.")
        private set
    var transformMessageConfiguration: TransformMessageConfiguration? = null
        get() = state.config?.transformMessageConfiguration ?: error("Transform message config isn't present.")
        private set
    var batchSendConfiguration: BatchSendConfiguration? = null
        get() = state.config?.batchSendConfiguration ?: error("batch send config isn't present.")
        private set
    var splitSendConfiguration: SplitSendConfiguration? = null
        get() = state.config?.splitSendConfiguration ?: error("split send configuration isn't present.")
        private set


    // strategies

    var sendStrategy = initialSendStrategy
        get() = stateReadLock.withLock { field }
        set(value) = stateWriteLock.withLock { field = value }

    var incomingMessagesStrategy = initialIncomingMessagesStrategy
        get() = stateReadLock.withLock { field }
        set(value) = stateWriteLock.withLock { field = value }

    var outgoingMessagesStrategy = initialOutgoingMessagesStrategy
        get() = stateReadLock.withLock { field }
        set(value) = stateWriteLock.withLock { field = value }

    var receiveStrategy = initialReceiveStrategy
        get() = stateReadLock.withLock { field }
        set(value) = stateWriteLock.withLock { field = value }

    var cleanupHandler = initialCleanupHandler
        get() = stateReadLock.withLock { field }
        set(value) = stateWriteLock.withLock { field = value }

    var recoveryHandler = initialRecoveryHandler
        get() = stateReadLock.withLock { field }
        set(value) = stateWriteLock.withLock { field = value }

    // send strategies aliases
    var presendStrategy: MessageProcessor = sendStrategy.sendPreprocessor
        get() = stateReadLock.withLock { sendStrategy.sendPreprocessor }
        private set

    // incoming message strategies aliases
    var incomingMessagesPreprocessor: MessageProcessor = incomingMessagesStrategy.incomingMessagesPreprocessor
        get() = stateReadLock.withLock { incomingMessagesStrategy.incomingMessagesPreprocessor }
        private set

    var testRequestProcessor: MessageProcessor = incomingMessagesStrategy.testRequestProcessor
        get() = stateReadLock.withLock { incomingMessagesStrategy.testRequestProcessor }
        private set
    var logonProcessor: MessageProcessor = incomingMessagesStrategy.logonStrategy
        get() = stateReadLock.withLock { incomingMessagesStrategy.logonStrategy }
        private set

    // outgoing message strategies aliases
    var outgoingMessageProcessor: MessageProcessor = outgoingMessagesStrategy.outgoingMessageProcessor
        get() = stateReadLock.withLock { outgoingMessagesStrategy.outgoingMessageProcessor }
        private set

    // receive message strategies aliases
    var receivePreprocessor: MessageProcessor = receiveStrategy.receivePreprocessor
        get() = stateReadLock.withLock { receiveStrategy.receivePreprocessor }
        private set

    var state: StrategyState = StrategyState()
        get() = stateReadLock.withLock { field }
        private set

    val type: RuleType
        get() = state.type
    val startTime: Instant
        get() = state.startTime

    fun resetStrategyAndState(config: RuleConfiguration) {
        stateWriteLock.withLock {
            state = StrategyState(config)
            sendStrategy = defaultStrategy.sendStrategy
            receiveStrategy = defaultStrategy.receiveStrategy
            incomingMessagesStrategy = defaultStrategy.incomingMessagesStrategy
            outgoingMessagesStrategy = defaultStrategy.outgoingMessagesStrategy
            recoveryHandler = defaultStrategy.recoveryHandler
            cleanupHandler = defaultStrategy.cleanupHandler
        }
    }

    fun cleanupStrategy() {
        stateWriteLock.withLock {
            state = StrategyState()
            sendStrategy = defaultStrategy.sendStrategy
            receiveStrategy = defaultStrategy.receiveStrategy
            incomingMessagesStrategy = defaultStrategy.incomingMessagesStrategy
            outgoingMessagesStrategy = defaultStrategy.outgoingMessagesStrategy
            recoveryHandler = defaultStrategy.recoveryHandler
            cleanupHandler = defaultStrategy.cleanupHandler
        }
    }
}
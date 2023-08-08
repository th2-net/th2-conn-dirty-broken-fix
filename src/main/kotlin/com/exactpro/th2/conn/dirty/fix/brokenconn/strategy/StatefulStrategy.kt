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
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api.OnCloseHandler
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api.RecoveryHandler
import java.time.Instant
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.withLock
import kotlin.concurrent.write

class StatefulStrategy(
    initialSendStrategy: SendStrategy,
    initialIncomingMessagesStrategy: IncomingMessagesStrategy,
    initialOutgoingMessagesStrategy: OutgoingMessagesStrategy,
    initialReceiveStrategy: ReceiveStrategy,
    initialCleanupHandler: CleanupHandler,
    initialRecoveryHandler: RecoveryHandler,
    initialOnCloseHandler: OnCloseHandler,
    private val defaultStrategy: DefaultStrategyHolder
) {
    private val lock = ReentrantReadWriteLock()

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
        get() = state.config?.missOutgoingMessagesConfiguration ?: error("Miss outgoing messages config isn't present.")
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
    fun updateSendStrategy(func: SendStrategy.() -> Unit) = lock.write {
        sendStrategy.func()
    }

    fun <T> getSendStrategy(func: SendStrategy.() -> T) = lock.read {
        sendStrategy.func()
    }

    fun updateIncomingMessageStrategy(func: IncomingMessagesStrategy.() -> Unit) = lock.write {
        incomingMessagesStrategy.func()
    }

    fun <T> getIncomingMessageStrategy(func: IncomingMessagesStrategy.() -> T) = lock.read {
        incomingMessagesStrategy.func()
    }

    fun updateOutgoingMessageStrategy(func: OutgoingMessagesStrategy.() -> Unit) = lock.write {
        outgoingMessagesStrategy.func()
    }

    fun <T> getOutgoingMessageStrategy(func: OutgoingMessagesStrategy.() -> T) = lock.read {
        outgoingMessagesStrategy.func()
    }

    fun updateReceiveMessageStrategy(func: ReceiveStrategy.() -> Unit) = lock.write {
        receiveStrategy.func()
    }

    fun <T> getReceiveMessageStrategy(func: ReceiveStrategy.() -> T) = lock.read {
        receiveStrategy.func()
    }

    fun getCleanupHandler(): CleanupHandler = lock.read { cleanupHandler }
    fun setCleanupHandler(handler: CleanupHandler) = lock.write { cleanupHandler = handler }

    fun getRecoveryHandler(): RecoveryHandler = lock.read { recoveryHandler }
    fun setRecoveryHandler(handler: RecoveryHandler) = lock.write { recoveryHandler = handler }

    fun getOnCloseHandler(): OnCloseHandler = lock.read { onCloseHandler }
    fun setOnCloseHandler(handler: OnCloseHandler) = lock.write { onCloseHandler = handler }

    private var sendStrategy = initialSendStrategy

    private var incomingMessagesStrategy = initialIncomingMessagesStrategy

    private var outgoingMessagesStrategy = initialOutgoingMessagesStrategy

    private var receiveStrategy = initialReceiveStrategy

    private var cleanupHandler = initialCleanupHandler

    private var recoveryHandler = initialRecoveryHandler

    private var onCloseHandler = initialOnCloseHandler

    var gracefulDisconnect = false
        get() = state.config?.gracefulDisconnect ?: false
        private set

    var state: StrategyState = StrategyState()
        get() = lock.read { field }
        private set

    val type: RuleType
        get() = state.type
    val startTime: Instant
        get() = state.startTime

    fun resetStrategyAndState(config: RuleConfiguration) {
        lock.write {
            state = StrategyState(config)
            sendStrategy.sendHandler = defaultStrategy.sendStrategy.sendHandler
            sendStrategy.sendPreprocessor = defaultStrategy.sendStrategy.sendPreprocessor
            receiveStrategy.receivePreprocessor = defaultStrategy.receiveStrategy.receivePreprocessor
            incomingMessagesStrategy.logonStrategy = defaultStrategy.incomingMessagesStrategy.logonStrategy
            incomingMessagesStrategy.incomingMessagesPreprocessor = defaultStrategy.incomingMessagesStrategy.incomingMessagesPreprocessor
            incomingMessagesStrategy.testRequestProcessor = defaultStrategy.incomingMessagesStrategy.testRequestProcessor
            outgoingMessagesStrategy.outgoingMessageProcessor = defaultStrategy.outgoingMessagesStrategy.outgoingMessageProcessor
            recoveryHandler = defaultStrategy.recoveryHandler
            cleanupHandler = defaultStrategy.cleanupHandler
            onCloseHandler = defaultStrategy.closeHandler
        }
    }

    fun cleanupStrategy() {
        lock.write {
            state = StrategyState()
            sendStrategy.sendHandler = defaultStrategy.sendStrategy.sendHandler
            sendStrategy.sendPreprocessor = defaultStrategy.sendStrategy.sendPreprocessor
            receiveStrategy.receivePreprocessor = defaultStrategy.receiveStrategy.receivePreprocessor
            incomingMessagesStrategy.logonStrategy = defaultStrategy.incomingMessagesStrategy.logonStrategy
            incomingMessagesStrategy.incomingMessagesPreprocessor = defaultStrategy.incomingMessagesStrategy.incomingMessagesPreprocessor
            incomingMessagesStrategy.testRequestProcessor = defaultStrategy.incomingMessagesStrategy.testRequestProcessor
            outgoingMessagesStrategy.outgoingMessageProcessor = defaultStrategy.outgoingMessagesStrategy.outgoingMessageProcessor
            recoveryHandler = defaultStrategy.recoveryHandler
            cleanupHandler = defaultStrategy.cleanupHandler
            onCloseHandler = defaultStrategy.closeHandler
        }
    }
}
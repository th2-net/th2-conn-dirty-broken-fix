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

import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api.ChannelSendHandler
import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api.MessageProcessor
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.withLock

class SendStrategy(
    initialSendPreprocessor: MessageProcessor,
    initialSendHandler: ChannelSendHandler
) {
    private val readWriteLock = ReentrantReadWriteLock()
    private val readLock = readWriteLock.readLock()
    private val writeLock = readWriteLock.writeLock()

    var sendPreprocessor = initialSendPreprocessor
        get() = readLock.withLock { field }
        set(value) = writeLock.withLock { field = value }
    var sendHandler = initialSendHandler
        get() = readLock.withLock { field }
        set(value) = writeLock.withLock { field = value }
}
/*
 * Copyright 2024-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.constants.Constants
import io.netty.buffer.ByteBuf
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import io.github.oshai.kotlinlogging.KotlinLogging

class TestServerEmulator(
    executor: ExecutorService,
    private val handler: FixHandler,
    private val channel: IChannel,
    private val generateMessageId: () -> MessageID,
    private val generateLogon: (seq: Int) -> ByteBuf,
    private val generateResendRequest: (seq: Int, begin: Int, end: Int) -> ByteBuf,
): AutoCloseable {

    private val consumerQueue: BlockingQueue<ByteBuf> = ArrayBlockingQueue(100)
    private val senderQueue: BlockingQueue<ByteBuf> = ArrayBlockingQueue(100)
    private val senderFuture: Future<*>
    private val consumerFuture: Future<*>

    @Volatile
    private var active = true

    init {
        senderFuture = executor.submit(::runSender)
        consumerFuture = executor.submit(::runConsumer)
    }

    fun consume(byteBuf: ByteBuf) {
        consumerQueue.place(byteBuf, "consumer queue overflow")
    }

    private fun runSender() {
        LOGGER.info { "Server sender emulator started" }
        try {
            while (active) {
                val message = senderQueue.take()
                handler.onIncoming(channel, message, generateMessageId())
            }
        } catch (e: Exception) {
            LOGGER.error(e) { "Server sender failure" }
        } finally {
            LOGGER.info { "Server sender emulator stopped" }
        }
    }

    private fun runConsumer() {
        LOGGER.info { "Server consumer emulator started" }
        try {
            var clientSeq = 0
            var serverSeq = 0

            var state = State.GENERAL

            while (active) {
                val message = consumerQueue.take()
                val msgType = requireNotNull(message.findField(35)?.value)
                val seq = requireNotNull(message.findField(Constants.MSG_SEQ_NUM_TAG)?.value?.toInt())
                val posDup = message.findField(Constants.POSS_DUP_TAG)?.value == "Y"
                LOGGER.info { "received msg seq: $seq, msg posDup: $posDup, clientSeq: $clientSeq, state: $state" }

                when (state) {
                    State.GENERAL -> {
                        if (posDup) {
                            // ignore
                        } else {
                            if (msgType == "A") {
                                serverSeq += 1
                                senderQueue.place(generateLogon(serverSeq), "sender queue overflow")
                                LOGGER.info { "sent logon" }
                            }

                            if (clientSeq + 1 == seq) {
                                clientSeq = seq
                                LOGGER.info { "incremented client seq to $clientSeq" }
                            } else {
                                val begin = clientSeq + 1
                                senderQueue.place(generateResendRequest(serverSeq, begin, 0), "sender queue overflow")
                                state = State.WAIT_RESEND
                                LOGGER.info { "sent resend request from $begin" }
                            }
                        }
                    }

                    State.WAIT_RESEND -> {
                        if (posDup) {
                            if (clientSeq + 1 == seq) {
                                clientSeq = seq
                                state = State.PROCESS_RESEND
                                LOGGER.info { "incremented client seq to $clientSeq after resend" }
                            } else {
                                // ignore it
                            }
                        } else {
                            // ignore it
                        }
                    }

                    State.PROCESS_RESEND -> {
                        if (posDup) {
                            if (clientSeq + 1 == seq) {
                                clientSeq = seq
                                LOGGER.info { "incremented client seq to $clientSeq after resend" }
                            } else {
                                val begin = clientSeq + 1
                                senderQueue.place(generateResendRequest(serverSeq, begin, 0), "sender queue overflow")
                                state = State.WAIT_RESEND
                                LOGGER.info { "sent resend request from $begin" }
                            }
                        } else {
                            if (clientSeq + 1 == seq) {
                                clientSeq = seq
                                state = State.GENERAL
                                LOGGER.info { "incremented client seq to $clientSeq" }
                            } else {
                                val begin = clientSeq + 1
                                senderQueue.place(generateResendRequest(serverSeq, begin, 0), "sender queue overflow")
                                state = State.WAIT_RESEND
                                LOGGER.info { "sent resend request from $begin" }
                            }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            LOGGER.error(e) { "Server consumer failure" }
        } finally {
            LOGGER.info { "Server consumer emulator stopped" }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        private fun <T: Any> BlockingQueue<T>.place(item: T, msg: String) {
            check(offer(item, 100, TimeUnit.MILLISECONDS)) {
                "$msg, size: $size"
            }
        }

        private enum class State {
            GENERAL,
            WAIT_RESEND,
            PROCESS_RESEND,
        }
    }

    override fun close() {
        active = false
        consumerFuture.cancel(true)
        senderFuture.cancel(true)
    }
}
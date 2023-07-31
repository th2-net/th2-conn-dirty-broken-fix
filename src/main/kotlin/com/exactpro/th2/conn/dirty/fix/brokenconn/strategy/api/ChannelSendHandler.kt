package com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api

import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import io.netty.buffer.ByteBuf
import java.util.concurrent.CompletableFuture

interface ChannelSendHandler {
    fun send(channel: IChannel, message: ByteBuf, property: Map<String, String>, eventID: EventID?): CompletableFuture<MessageID>
}
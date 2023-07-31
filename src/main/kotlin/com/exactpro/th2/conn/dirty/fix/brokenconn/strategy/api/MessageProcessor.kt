package com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api

import io.netty.buffer.ByteBuf

interface MessageProcessor {
    fun process(message: ByteBuf, map: Map<String, String>): Map<String, String>?
}
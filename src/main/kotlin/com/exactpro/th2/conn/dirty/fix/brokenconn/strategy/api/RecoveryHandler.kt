package com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.api

interface RecoveryHandler {
    fun recovery(beginSeqNo: Int, endSeqNo: Int)
}
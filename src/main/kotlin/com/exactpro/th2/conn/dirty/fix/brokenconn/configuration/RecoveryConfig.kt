package com.exactpro.th2.conn.dirty.fix.brokenconn.configuration

data class RecoveryConfig(
    val outOfOrder: Boolean = false,
    val sequenceResetForAdmin: Boolean = true
)
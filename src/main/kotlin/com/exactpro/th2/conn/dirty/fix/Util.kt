package com.exactpro.th2.conn.dirty.fix

import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.utils.logTimestamp
import com.exactpro.th2.common.utils.message.sessionAlias

fun MessageID.logId(): String = "${bookName}:${sessionAlias}:${direction.ordinal + 1}:${timestamp.logTimestamp}:${sequence}"
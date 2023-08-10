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
package com.exactpro.th2.conn.dirty.fix.brokenconn.configuration

import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.RuleType
import java.time.Duration

// TODO: disconnect type should be configurable
data class RuleConfiguration(
    val ruleType: RuleType,
    val duration: Duration,
    val cleanUpDuration: Duration,
    val weight: Int? = null,
    val gracefulDisconnect: Boolean = false,
    val missIncomingMessagesConfiguration: MissMessageConfiguration? = null,
    val missOutgoingMessagesConfiguration: MissMessageConfiguration? = null,
    val transformMessageConfiguration: TransformMessageConfiguration? = null,
    val batchSendConfiguration: BatchSendConfiguration? = null,
    val splitSendConfiguration: SplitSendConfiguration? = null,
    val changeSequenceConfiguration: ChangeSequenceConfiguration? = null,
    val resendRequestConfiguration: ResendRequestConfiguration? = null
) {
    init {
        when(ruleType) {
            RuleType.DISCONNECT_WITH_RECONNECT -> {}
            RuleType.IGNORE_INCOMING_MESSAGES -> {
                require(missIncomingMessagesConfiguration != null) { "`blockIncomingMessagesConfiguration` is required for $ruleType" }
            }
            RuleType.TRANSFORM_LOGON -> {
                require(transformMessageConfiguration != null) { "`transformMessageConfiguration` is required for $ruleType"}
            }
            RuleType.BI_DIRECTIONAL_RESEND_REQUEST -> {
                require(missIncomingMessagesConfiguration != null) { "`blockIncomingMessagesConfiguration` is required for $ruleType" }
                require(missOutgoingMessagesConfiguration != null) { "`blockOutgoingMessagesConfiguration` is required for $ruleType" }
            }
            RuleType.CREATE_OUTGOING_GAP -> {
                require(missOutgoingMessagesConfiguration != null) { "`blockOutgoingMessagesConfiguration` is required for $ruleType" }
            }
            RuleType.CLIENT_OUTAGE -> {}
            RuleType.PARTIAL_CLIENT_OUTAGE -> {}
            RuleType.RESEND_REQUEST -> {
                require(resendRequestConfiguration != null) { "`resendRequestConfiguration` is required for $ruleType" }
            }
            RuleType.SLOW_CONSUMER -> {}
            RuleType.SEQUENCE_RESET -> {
                require(changeSequenceConfiguration != null) { "`changeSequenceConfiguration` is required for $ruleType" }
            }
            RuleType.BATCH_SEND -> {
                require(batchSendConfiguration != null) { "`batchSendConfiguration` is required for $ruleType" }
            }
            RuleType.SPLIT_SEND -> {
                require(splitSendConfiguration != null) { "`splitSendConfiguration` is required for $ruleType" }
            }
        }
    }
}
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

enum class RuleType {
    DISCONNECT_WITH_RECONNECT,
    IGNORE_INCOMING_MESSAGES, // TODO: Resend Request before receiving recovery from peer
    TRANSFORM_LOGON,
    TRANSFORM_MESSAGE_STRATEGY,
    BI_DIRECTIONAL_RESEND_REQUEST,
    CREATE_OUTGOING_GAP, // TODO: Random recovery order // TODO: Allow non recovery messages during recovery // Recovering admin messages with poss dup
    CLIENT_OUTAGE,
    PARTIAL_CLIENT_OUTAGE,
    RESEND_REQUEST,
    SLOW_CONSUMER,
    SEQUENCE_RESET, // TODO: Without gap fill flag // TODO: Resend Request before receiving recovery from peer
    SEND_SEQUENCE_RESET,
    BATCH_SEND,
    SPLIT_SEND,
    LOGON_AFTER_LOGON,
    FAKE_RETRANSMISSION,
    DEFAULT
}
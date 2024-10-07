/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.conn.dirty.fix.HeaderPosition
import com.exactpro.th2.conn.dirty.fix.TrailerPosition

data class MoveTrailerConfiguration(val position: TrailerPosition)
data class MoveHeaderConfiguration(val position: HeaderPosition)

data class CorruptMessageStructureConfiguration(
    val trailerTags: Set<Int> = setOf(10),
    val headerTags: Set<Int> = setOf(8, 9, 35, 34, 1128, 49, 56, 43, 97, 52, 122),
    val moveHeaderConfiguration: MoveHeaderConfiguration?,
    val moveTrailerConfiguration: MoveTrailerConfiguration?
)
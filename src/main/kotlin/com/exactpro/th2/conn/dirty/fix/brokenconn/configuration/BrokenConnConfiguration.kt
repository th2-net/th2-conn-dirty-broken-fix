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

import com.exactpro.th2.conn.dirty.fix.brokenconn.strategy.SchedulerType

data class BrokenConnConfiguration(
    val schedulerType: SchedulerType = SchedulerType.CONSECUTIVE,
    val rules: List<RuleConfiguration> = emptyList()
) {
    init {
        if(schedulerType == SchedulerType.WEIGHTS) {
            require(rules.all { it.weight != null }) { "With ${SchedulerType.WEIGHTS} all rule configurations must have `weight` property." }
            require(rules.all { (it.weight ?: 0) > 0 }) { "With ${SchedulerType.WEIGHTS} all rule configurations must have `weight` greater than 0." }
        }
    }
}
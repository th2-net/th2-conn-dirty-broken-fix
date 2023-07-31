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

import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.RuleConfiguration

class StrategyScheduler(
    schedulerType: SchedulerType,
    private val rules: List<RuleConfiguration>
) {
    private val totalWeight = rules.asSequence().map { it.weight ?: 0 }.sum()
    private var strategyIndex = 0
    private val nextFunction = if(schedulerType == SchedulerType.CONSECUTIVE) ::nextConsecutiveRule else ::nextWeightedRule

    fun next(): RuleConfiguration {
        return nextFunction()
    }

    private fun nextConsecutiveRule(): RuleConfiguration {
        val rule = rules[strategyIndex]
        strategyIndex = (strategyIndex + 1) % rules.size

        return rule
    }

    private fun nextWeightedRule(): RuleConfiguration {
        val randomWeight = Math.random() * totalWeight;

        var cumulativeWeight = 0
        for( rule in rules ) {
            cumulativeWeight += rule.weight ?: 0
            if (randomWeight < cumulativeWeight) {
                return rule;
            }
        }

        return rules[rules.size - 1];
    }
}
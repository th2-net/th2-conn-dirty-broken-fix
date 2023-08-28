/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.conn.dirty.fix

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils.createMessageBean
import com.exactpro.th2.common.event.bean.IRow
import com.exactpro.th2.common.event.bean.builder.TableBuilder
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel
import com.exactpro.th2.conn.dirty.tcp.core.api.IMangler
import com.exactpro.th2.conn.dirty.tcp.core.api.IManglerContext
import com.exactpro.th2.conn.dirty.tcp.core.api.IManglerFactory
import com.exactpro.th2.conn.dirty.tcp.core.api.IManglerSettings
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.auto.service.AutoService
import io.netty.buffer.ByteBuf
import io.netty.buffer.ByteBufUtil
import mu.KotlinLogging

private val LOGGER = KotlinLogging.logger {}

private val MAPPER = JsonMapper.builder()
    .addModule(KotlinModule(nullIsSameAsDefault = true))
    .build()

private const val RULE_NAME_PROPERTY = "rule-name"
private const val RULE_ACTIONS_PROPERTY = "rule-actions"
private const val MANGLE_EVENT_TYPE = "Mangle"

class FixProtocolMangler(context: IManglerContext) : IMangler {
    private val rules = (context.settings as FixProtocolManglerSettings).rules

    override fun onOutgoing(channel: IChannel, message: ByteBuf, metadata: MutableMap<String, String>): Event? {
        LOGGER.trace { "Processing message: ${message.toString(Charsets.UTF_8)}" }

        val (rule, unconditionally) = try {
            getRule(message, metadata) ?: return null
        } catch (e: Exception) {
            return Event.start().apply {
                name("Message wasn't mangled. Configuration error.")
                type(MANGLE_EVENT_TYPE)
                status(FAILED)
                bodyData(createMessageBean("Message metadata: $metadata"))
                exception(e, true)
            }
        }

        val (name, results, message) = MessageTransformer.transform(message, rule, unconditionally) ?: return null

        return Event.start().apply {
            type(MANGLE_EVENT_TYPE)
            if(results.any { it.statusDesc.status == ActionStatus.FAIL }) {
                name("Message was partially mangled.")
                status(FAILED)

                if (metadata[RULE_ACTIONS_PROPERTY] != null) {
                    bodyData(createMessageBean("Action source is $RULE_ACTIONS_PROPERTY. " +
                            "Data: ${metadata[RULE_ACTIONS_PROPERTY]}"))
                } else {
                    bodyData(createMessageBean("Action source is service configuration. " +
                            "Data: ${MAPPER.writeValueAsString(rules)}"))
                }
            } else {
                name("Message mangled.")
                status(PASSED)
            }

            bodyData(createMessageBean("Original message:"))
            bodyData(createMessageBean(ByteBufUtil.prettyHexDump(message)))

            TableBuilder<ActionRow>().run {
                results.forEach { result ->
                    row(ActionRow(name, result.tag, result.value,
                        result.action.toString(), result.statusDesc.status.name,
                        result.statusDesc.description))
                }
                bodyData(build())
            }
        }
    }

    private fun getRule(message: ByteBuf, metadata: MutableMap<String, String>): Pair<Rule, Boolean>? {
        metadata[RULE_NAME_PROPERTY]?.also { name ->
            val rule = rules.find { it.name == name }
                ?: throw IllegalArgumentException("Invalid '$RULE_NAME_PROPERTY' value - $name. No rule with name found in configuration: $name. Searched in [ ${rules.joinToString(",") {it.name}} ]")
            return rule to true
        }

        metadata[RULE_ACTIONS_PROPERTY]?.also { yaml ->
            return try {
                val actions = MAPPER.readValue<List<Action>>(yaml)
                Rule("custom", listOf(Transform(listOf(), actions))) to true
            } catch (e: Exception) {
                throw IllegalArgumentException("Invalid '$RULE_ACTIONS_PROPERTY' value - $yaml", e)
            }
        }

        if (rules.isEmpty()) return null

        val rule = rules.filter { rule ->
            rule.transform.any { transform ->
                transform.conditions.all { it.matches(message) }
            }
        }.randomOrNull()

        if (rule == null) {
            LOGGER.trace { "No matching rule was found" }
            return null
        }

        return rule to false
    }
}

@AutoService(IManglerFactory::class)
class FixProtocolManglerFactory : IManglerFactory {
    override val name = "demo-fix-mangler"
    override val settings = FixProtocolManglerSettings::class.java
    override fun create(context: IManglerContext) = FixProtocolMangler(context)
}

class FixProtocolManglerSettings(val rules: List<Rule> = emptyList()) : IManglerSettings

private data class ActionRow(
    val corruptionType: String,
    val corruptedTag: Int,
    val corruptedValue: String?,
    val corruptionDescription: String,
    val status: String,
    val errorDescription: String?
) : IRow
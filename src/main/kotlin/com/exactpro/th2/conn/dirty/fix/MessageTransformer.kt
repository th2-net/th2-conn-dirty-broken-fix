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

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.annotation.JsonIgnore
import io.netty.buffer.ByteBuf
import mu.KotlinLogging
import java.util.regex.Pattern

typealias RuleID = String
typealias Tag = Int

object MessageTransformer {
    private val logger = KotlinLogging.logger {}

    fun transformWithoutResults(message: ByteBuf, actions: List<Action>) {
        logger.debug { "Applying rule directly from handler: ${actions}." }
        transform(message, actions).forEach { _ -> }
    }

    fun transform(message: ByteBuf, rule: Rule, unconditionally: Boolean = false): TransformResult? {
        logger.debug { "Applying rule: ${rule.name}" }

        val original = message.copy()

        val results = mutableListOf<ActionResult>().apply {
            for ((conditions, actions) in rule.transform) {
                if (unconditionally || conditions.all { it.matches(message) }) {
                    this += transform(message, actions)
                }
            }
        }

        if (results.isEmpty()) {
            logger.debug("No transformations were applied")
            return null
        }

        results.forEach { logger.trace { "Applied transformation: $it" } }

        if (rule.transform.any(Transform::updateLength)) {
            message.updateLength()
            logger.trace { "Recalculated length" }
        }

        if (rule.transform.any(Transform::updateChecksum)) {
            message.updateChecksum()
            logger.trace { "Recalculated checksum" }
        }

        return TransformResult(rule.name, results, original)
    }

    fun transform(message: ByteBuf, actions: List<Action>) = sequence {
        actions.forEach { action ->
            try {
                action.set?.apply {
                    val tag = singleTag
                    val value = singleValue

                    if (message.setField(tag, value)) {
                        yield(ActionResult(tag, value, action))
                    }
                }

                action.add?.also { field ->
                    val tag = field.singleTag
                    val value = field.singleValue

                    action.before?.find(message)?.let { next ->
                        next.insertPrevious(tag, value)
                        yield(ActionResult(tag, value, action))
                    }

                    action.after?.find(message)?.let { previous ->
                        previous.insertNext(tag, value)
                        yield(ActionResult(tag, value, action))
                    }
                }

                action.move?.find(message)?.let { field ->
                    val tag = checkNotNull(field.tag) { "Field tag for move was empty." }
                    val value = field.value

                    action.before?.find(message)?.let { next ->
                        field.clear()
                        next.insertPrevious(tag, value)
                        yield(ActionResult(tag, value, action))
                    }

                    action.after?.find(message)?.let { previous ->
                        previous.insertNext(tag, value)
                        field.clear()
                        yield(ActionResult(tag, value, action))
                    }
                }

                action.remove?.find(message)?.let { field ->
                    val tag = checkNotNull(field.tag) { "Field tag for remove was empty." }
                    field.clear()
                    yield(ActionResult(tag, null, action))
                }

                action.replace?.find(message)?.let { field ->
                    val with = action.with!!
                    val tag = with.singleTag
                    val value = with.singleValue

                    field.tag = tag
                    field.value = value

                    yield(ActionResult(tag, value, action))
                }
            } catch (e: Exception) {
                logger.error(e) { "Error while applying action $action" }
                yield(ActionResult(-1, null, action, ActionStatusDescription("Error while applying action: $action. Message: ${e.message}", ActionStatus.FAIL)))
            }
        }
    }
}

data class FieldSelector(
    val tag: Tag?,
    val tagOneOf: List<Tag>?,
    val matches: Pattern,
) {
    init {
        require((tag != null) xor !tagOneOf.isNullOrEmpty()) { "Either 'tag' or 'tagOneOf' must be specified" }
    }

    @JsonIgnore private val predicate = matches.asMatchPredicate()

    fun matches(message: ByteBuf): Boolean = find(message) != null

    fun find(message: ByteBuf): FixField? = when {
        tag != null -> message.findField { it.tag == tag && it.value?.run(predicate::test) ?: false }
        else -> message.findFields { it.tag in tagOneOf!! && it.value?.run(predicate::test) ?: false }.randomOrNull()
    }

    override fun toString() = buildString {
        tag?.apply { append("tag $tag") }
        tagOneOf?.apply { append("one of tags $tagOneOf") }
        append(" ~= /$matches/")
    }
}

data class FieldDefinition(
    val tag: Tag?,
    val value: String?,
    val tagOneOf: List<Tag>?,
    val valueOneOf: List<String>?
) {
    init {
        require((tag != null) xor !tagOneOf.isNullOrEmpty()) { "Either 'tag' or 'tagOneOf' must be specified" }
        require((value != null) xor !valueOneOf.isNullOrEmpty()) { "Either 'value' or 'valueOneOf' must be specified" }
    }

    @JsonIgnore val singleTag: Tag = tag ?: tagOneOf!!.random()
    @JsonIgnore val singleValue: String = value ?: valueOneOf!!.random()

    override fun toString() = buildString {
        tag?.apply { append("tag $tag") }
        tagOneOf?.apply { append("one of tags $tagOneOf") }
        append(" = ")
        value?.apply { append("'$value'") }
        valueOneOf?.apply { append("one of $valueOneOf") }
    }
}

data class Action(
    val set: FieldDefinition? = null,
    val add: FieldDefinition? = null,
    val move: FieldSelector? = null,
    val replace: FieldSelector? = null,
    val remove: FieldSelector? = null,
    val with: FieldDefinition? = null,
    val before: FieldSelector? = null,
    val after: FieldSelector? = null,
) {
    init {
        val operations = listOfNotNull(set, add, move, remove, replace)

        require(operations.isNotEmpty()) { "Action must have at least one operation" }
        require(operations.size == 1) { "Action has more than one operation" }

        require(set == null && remove == null || with == null && before == null && after == null) {
            "'set'/'remove' operations cannot have 'with', 'before', 'after' options"
        }

        require(add == null && move == null || with == null) {
            "'add'/'move' operations cannot have 'with' option"
        }

        require(replace == null || with != null) {
            "'replace' operation requires 'with' option"
        }

        require(before == null || after == null) {
            "'before' and 'after' options are mutually exclusive"
        }

        require(add == null && move == null || before != null || after != null) {
            "'add'/'move' option requires 'before' or 'after' option"
        }
    }

    override fun toString() = buildString {
        set?.apply { append("set $this") }
        add?.apply { append("add $this") }
        move?.apply { append("move $this") }
        replace?.apply { append("replace $this") }
        remove?.apply { append("remove $this") }
        with?.apply { append(" with $this") }
        before?.apply { append(" before $this") }
        after?.apply { append(" after $this") }
    }
}


data class Transform(
    @JsonAlias("when") val conditions: List<FieldSelector> = listOf(),
    @JsonAlias("then") val actions: List<Action>,
    @JsonAlias("update-length") val updateLength: Boolean = true,
    @JsonAlias("update-checksum") val updateChecksum: Boolean = true,
) {
    init {
        require(actions.isNotEmpty()) { "Transformation must have at least one action" }
    }

    override fun toString() = buildString {
        appendLine("when")
        conditions.forEach { appendLine("    $it") }
        appendLine("then")
        actions.forEach { appendLine("    $it") }
    }
}

data class Rule(
    val name: RuleID,
    val transform: List<Transform>
) {
    init {
        require(transform.isNotEmpty()) { "Rule must have at least one transform" }
    }

    override fun toString() = buildString {
        appendLine("name: $name")
        appendLine("transforms: ")
        transform.forEach { appendLine("    $it") }
    }
}

data class TransformResult(val rule: RuleID, val results: List<ActionResult>, val message: ByteBuf)
data class ActionResult(val tag: Tag, val value: String?, val action: Action, val statusDesc: ActionStatusDescription = SUCCESS_ACTION_DESCRIPTION)
data class ActionStatusDescription(val description: String? = null, val status: ActionStatus)
enum class ActionStatus { SUCCESS, FAIL; }
private val SUCCESS_ACTION_DESCRIPTION = ActionStatusDescription(status = ActionStatus.SUCCESS)
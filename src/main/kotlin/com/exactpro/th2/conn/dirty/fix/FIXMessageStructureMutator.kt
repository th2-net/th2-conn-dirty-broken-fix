/*
 * Copyright 2024-2025 Exactpro (Exactpro Systems Limited)
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

import io.github.oshai.kotlinlogging.KotlinLogging
import io.netty.buffer.ByteBuf
import kotlin.text.Charsets.US_ASCII

enum class HeaderPosition {
    INSIDE_BODY,
    AFTER_BODY,
    INSIDE_TRAILER,
    AFTER_TRAILER
}

enum class TrailerPosition {
    BEFORE_HEADER,
    INSIDE_HEADER,
    AFTER_HEADER,
    INSIDE_BODY
}

class FIXMessageStructureMutator(
    private val headerTags: Set<Int>,
    private val trailerTags: Set<Int>
) {

    fun moveHeader(dst: HeaderPosition, message: ByteBuf): ByteBuf {
        val condition: (FixField) -> Boolean = {headerTags.contains(it.tag)}

        val field = when (dst) {
            HeaderPosition.INSIDE_BODY -> getBodyFirstTag(message)
            HeaderPosition.AFTER_BODY -> getBodyLastTag(message)
            HeaderPosition.INSIDE_TRAILER -> getTrailerFirstTag(message)
            HeaderPosition.AFTER_TRAILER -> getTrailerLastTag(message)
        }

        if (field?.tag == null) {
            K_LOGGER.warn { "Move header operation failed. Not found position to insert for $dst in message: ${message.toString(
                US_ASCII)}" }
            return message
        }

        moveBlockAfter(field.tag!!, message, condition)
        return message
    }

    fun moveTrailer(dst: TrailerPosition, message: ByteBuf): ByteBuf {
        val condition: (FixField) -> Boolean = {trailerTags.contains(it.tag)}

        val field = when(dst) {
            TrailerPosition.BEFORE_HEADER -> getHeaderFirstTag(message)
            TrailerPosition.INSIDE_HEADER -> getHeaderFirstTag(message)
            TrailerPosition.AFTER_HEADER -> getHeaderLastTag(message)
            TrailerPosition.INSIDE_BODY -> getBodyFirstTag(message)
        }

        if (field?.tag == null) {
            K_LOGGER.warn { "Move trailer operation failed. Not found position to insert for $dst in message: ${message.toString(
                US_ASCII)}" }
            return message
        }
        if(dst == TrailerPosition.BEFORE_HEADER) {
            moveBlockBefore(field.tag!!, message, condition)
        } else {
            moveBlockAfter(field.tag!!, message, condition)
        }
        return message
    }

    private fun moveBlockAfter(afterTag: Int, message: ByteBuf, condition: (FixField) -> Boolean) {
        val toMove = mutableListOf<Pair<Int, String>>()
        message.forEachField {
            if(condition(it)) {
                toMove.add(Pair(it.tag!!, it.value!!))
                it.clear()
            }
        }

        val afterField = message.findField(afterTag)

        if (afterField == null) {
            K_LOGGER.warn { "Something went wrong while moving block. Tag $afterTag is missing from message. Impossible to move block." }
            return
        }

        var previous: FixField = afterField
        toMove.forEach {
            previous = previous.insertNext(it.first, it.second)
        }
    }

    private fun moveBlockBefore(beforeTag: Int, message: ByteBuf, condition: (FixField) -> Boolean) {
        val toMove = mutableListOf<Pair<Int, String>>()
        message.forEachField {
            if(condition(it)) {
                toMove.add(0, Pair(it.tag!!, it.value!!))
                it.clear()
            }
        }

        val afterField = message.findField(beforeTag)

        if (afterField == null) {
            K_LOGGER.warn { "Something went wrong while moving block. Tag $beforeTag is missing from message. Impossible to move block." }
            return
        }

        var previous: FixField = afterField
        toMove.forEach {
            previous = previous.insertPrevious(it.first, it.second)
        }
    }

    private fun getBodyFirstTag(message: ByteBuf): FixField? = findFirstTag(message) { !headerTags.contains(it.tag) }
    private fun getBodyLastTag(message: ByteBuf): FixField? = findLastTag(message) { !headerTags.contains(it.tag) && !trailerTags.contains(it.tag) }

    private fun getTrailerFirstTag(message: ByteBuf): FixField? = findFirstTag(message) { trailerTags.contains(it.tag) }
    private fun getTrailerLastTag(message: ByteBuf): FixField? = findLastTag(message) { trailerTags.contains(it.tag) }

    private fun getHeaderFirstTag(message: ByteBuf): FixField? = findFirstTag(message) { headerTags.contains(it.tag) }
    private fun getHeaderLastTag(message: ByteBuf): FixField? = findLastTag(message) { headerTags.contains(it.tag) }

    private fun findFirstTag(message: ByteBuf, condition: (FixField) -> Boolean): FixField? {
        message.forEachField {
            if(condition(it)) {
                return it
            }
        }
        return null
    }

    private fun findLastTag(message: ByteBuf, condition: (FixField) -> Boolean): FixField? {
        var lastField: FixField? = null
        message.forEachField {
            if(condition(it)) {
                lastField = it
            }
        }
        return lastField
    }

    companion object {
        val K_LOGGER = KotlinLogging.logger {  }
    }
}
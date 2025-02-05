/*
 * Copyright 2022-2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.conn.dirty.fix.FixField.FixGroup
import com.exactpro.th2.netty.bytebuf.util.EMPTY_STRING
import com.exactpro.th2.netty.bytebuf.util.endsWith
import com.exactpro.th2.netty.bytebuf.util.get
import com.exactpro.th2.netty.bytebuf.util.indexOf
import com.exactpro.th2.netty.bytebuf.util.insert
import com.exactpro.th2.netty.bytebuf.util.lastIndexOf
import com.exactpro.th2.netty.bytebuf.util.replace
import com.exactpro.th2.netty.bytebuf.util.requireReadable
import com.exactpro.th2.netty.bytebuf.util.subsequence
import io.netty.buffer.ByteBuf
import java.nio.charset.Charset
import kotlin.text.Charsets.UTF_8

const val SOH_CHAR = '\u0001'
const val SOH_BYTE = SOH_CHAR.code.toByte()
const val SEP_CHAR = '='
const val SEP_BYTE = '='.code.toByte()

private val LENGTH_START_BYTES = "${SOH_CHAR}9${SEP_CHAR}".toByteArray()
private val CHECKSUM_START_BYTES = "${SOH_CHAR}10${SEP_CHAR}".toByteArray()

@JvmOverloads
fun ByteBuf.firstField(charset: Charset = UTF_8): FixField? = FixField.atOffset(this, readerIndex(), charset)

@JvmOverloads
fun ByteBuf.lastField(charset: Charset = UTF_8): FixField? = FixField.atOffset(this, writerIndex() - 1, charset)

val ByteBuf.fields: Iterable<FixField>
    get() = Iterable { iterator { forEachField { yield(it) } } }

@JvmOverloads
inline fun ByteBuf.forEachField(
    charset: Charset = UTF_8,
    start: FixField? = firstField(charset),
    action: (FixField) -> Unit,
) {
    var field = start

    while (field != null) {
        action(field)
        field = field.next()
    }
}

@JvmOverloads
inline fun ByteBuf.forEachFieldDesc(
    charset: Charset = UTF_8,
    start: FixField? = lastField(charset),
    action: (FixField) -> Unit,
) {
    var field = start

    while (field != null) {
        action(field)
        field = field.previous()
    }
}

@JvmOverloads
inline fun ByteBuf.findField(
    charset: Charset = UTF_8,
    start: FixField? = firstField(charset),
    predicate: (FixField) -> Boolean,
): FixField? {
    forEachField(charset, start) { if (predicate(it)) return it }
    return null
}

@JvmOverloads
inline fun ByteBuf.findFields(charset: Charset = UTF_8, crossinline predicate: (FixField) -> Boolean): List<FixField> {
    return getFields(charset, predicate).asSequence().toList()
}

@JvmOverloads
inline fun ByteBuf.findLastField(
    charset: Charset = UTF_8,
    start: FixField? = lastField(charset),
    predicate: (FixField) -> Boolean,
): FixField? {
    forEachFieldDesc(charset, start) { if (predicate(it)) return it }
    return null
}

@JvmOverloads
fun ByteBuf.findField(
    tag: Int?,
    charset: Charset = UTF_8,
    start: FixField? = firstField(charset),
): FixField? {
    return findField(charset, start) { it.tag == tag }
}

@JvmOverloads
fun ByteBuf.findField(
    tag: Int?,
    value: String?,
    charset: Charset = UTF_8,
    start: FixField? = firstField(charset),
): FixField? {
    return findField(charset, start) { it.tag == tag && it.value == value }
}

@JvmOverloads
fun ByteBuf.findLastField(
    tag: Int?,
    charset: Charset = UTF_8,
    start: FixField? = lastField(charset),
): FixField? {
    return findLastField(charset, start) { it.tag == tag }
}

@JvmOverloads
fun ByteBuf.findLastField(
    tag: Int?,
    value: String?,
    charset: Charset = UTF_8,
    start: FixField? = lastField(charset),
): FixField? {
    return findLastField(charset, start) { it.tag == tag && it.value == value }
}

@JvmOverloads
inline fun ByteBuf.modifyField(
    predicate: (FixField) -> Boolean,
    charset: Charset = UTF_8,
    start: FixField? = firstField(charset),
    action: (FixField) -> Unit,
): Boolean = findField(charset, start, predicate)?.apply(action) != null

@JvmOverloads
inline fun ByteBuf.modifyLastField(
    predicate: (FixField) -> Boolean,
    charset: Charset = UTF_8,
    start: FixField? = lastField(charset),
    action: (FixField) -> Unit,
): Boolean = findLastField(charset, start, predicate)?.apply(action) != null

@JvmOverloads
fun ByteBuf.setField(tag: Int?, value: String?, charset: Charset = UTF_8): Boolean {
    return modifyField({ it.tag == tag }, charset) { it.value = value }
}

@JvmOverloads
fun ByteBuf.setLastField(tag: Int?, value: String?, charset: Charset = UTF_8): Boolean {
    return modifyLastField({ it.tag == tag }, charset) { it.value = value }
}

@JvmOverloads
fun ByteBuf.addFieldAfter(
    tag: Int?,
    value: String?,
    field: FixField = lastField() ?: error("message is empty"),
): FixField = field.insertNext(tag, value)

@JvmOverloads
fun ByteBuf.addFieldBefore(
    tag: Int?,
    value: String?,
    field: FixField = firstField() ?: error("message is empty"),
): FixField = field.insertPrevious(tag, value)

@JvmOverloads
fun ByteBuf.replaceFieldValue(
    tag: Int?,
    oldValue: String?,
    newValue: String?,
    charset: Charset = UTF_8,
): Boolean {
    return modifyField({ it.tag == tag && it.value == oldValue }, charset) { it.value = newValue }
}

@JvmOverloads
fun ByteBuf.replaceLastFieldValue(
    tag: Int?,
    oldValue: String?,
    newValue: String?,
    charset: Charset = UTF_8,
): Boolean {
    return modifyLastField({ it.tag == tag && it.value == oldValue }, charset) { it.value = newValue }
}

inline fun ByteBuf.getFields(
    charset: Charset = UTF_8,
    crossinline predicate: (FixField) -> Boolean,
) = iterator {
    forEachField(charset) { if (predicate(it)) yield(it) }
}

inline fun ByteBuf.getFieldsDesc(
    charset: Charset = UTF_8,
    crossinline predicate: (FixField) -> Boolean,
) = iterator {
    forEachFieldDesc(charset) { if (predicate(it)) yield(it) }
}

fun ByteBuf.calculateChecksum(): Int {
    var checksum = 0

    forEachField { field ->
        if (field.tag == 10 && field.next() == null) return@forEachField
        checksum += field.bytes.sum()
    }

    return checksum % 256
}

fun ByteBuf.setChecksum(checksum: Int): Boolean {
    val value = checksum.toString().padStart(3, '0')

    if (!setLastField(10, value)) {
        lastField()?.insertNext(10, value) ?: return false
    }

    return true
}

fun ByteBuf.updateChecksum(): ByteBuf {
    val checksumIndex = lastIndexOf(CHECKSUM_START_BYTES)
    if (checksumIndex < 0) return this
    val checksum = (readerIndex()..checksumIndex).sumOf { this[it].toInt() }
    if (!endsWith(SOH_BYTE)) writeByte(SOH_BYTE.toInt())
    val checksumString = (checksum % 256).toString().padStart(3, '0')
    return replace(checksumIndex + CHECKSUM_START_BYTES.size, writerIndex() - 1, checksumString)
}

fun ByteBuf.calculateLength(): Int {
    var length = 0
    var field = findField(9) ?: return -1

    while (true) {
        field = field.next() ?: return length
        if (field.tag == 10) return length
        length += field.bytes.size
    }
}

fun ByteBuf.setLength(length: Int): Boolean {
    val value = length.toString()

    if (!setField(9, value)) {
        findField(8)?.insertNext(9, value) ?: return false
    }

    return true
}

fun ByteBuf.updateLength(): ByteBuf {
    val lengthIndex = indexOf(LENGTH_START_BYTES)
    if (lengthIndex < 0) return this
    val valueIndex = lengthIndex + LENGTH_START_BYTES.size
    val sohIndex = indexOf(SOH_BYTE, valueIndex)
    if (sohIndex < 0) return this
    val startIndex = sohIndex + 1
    val checksumIndex = lastIndexOf(CHECKSUM_START_BYTES, startIndex)
    val endIndex = if (checksumIndex < 0) writerIndex() else checksumIndex + 1
    return replace(valueIndex, sohIndex, (endIndex - startIndex).toString())
}

fun Iterable<FixField>.findGroup(counter: Int, delimiter: Int, tags: Iterable<Int>): FixGroup? {
    val counter = find { it.tag == counter } ?: return null
    val delimiter = counter.next()?.takeIf { it.tag == delimiter } ?: return null
    return FixGroup(counter, delimiter, tags)
}

fun Iterable<FixField>.findGroups(counter: Int, delimiter: Int, tags: Iterable<Int>) = sequence {
    for(counter in filter { it.tag == counter }) {
        val delimiter = counter.next()?.takeIf { it.tag == delimiter } ?: continue
        yield(FixGroup(counter, delimiter, tags))
    }
}

interface FixElement {
    fun previous(): FixElement?
    fun next(): FixElement?
}

class FixField private constructor(
    private val buffer: ByteBuf,
    private var startIndex: Int,
    private var endIndex: Int,
    private val charset: Charset = UTF_8,
) : FixElement {
    private var previous: FixField? = null
    private var next: FixField? = null

    private var _bytes: ByteArray? = null
    private var _raw: String? = null
    private var _rawTag: String? = null
    private var _tag: Int? = null
    private var _value: String? = null

    var bytes: ByteArray
        get() = _bytes ?: buffer.subsequence(startIndex, endIndex).apply {
            _bytes = this
        }
        set(bytes) {
            buffer.replace(startIndex, endIndex, bytes)
            endIndex = startIndex + bytes.size
            updateNextIndices()
            _bytes = bytes
            _raw = null
            _rawTag = null
            _tag = null
            _value = null
        }

    var raw: String
        get() = _raw ?: bytes.toString(charset).apply {
            _raw = this
        }
        set(raw) {
            bytes = raw.toByteArray(charset)
            _raw = raw
        }

    var rawTag: String?
        get() = _rawTag ?: raw.run {
            val tag = substringBefore(SEP_CHAR)
            if (tag == this) return null
            _rawTag = tag
            _tag = null
            return _rawTag
        }
        set(rawTag) {
            val value = value
            raw = toRaw(rawTag, value)
            _rawTag = rawTag
            _tag = null
            _value = value
        }

    var tag: Int?
        get() = _tag ?: rawTag?.toIntOrNull().apply {
            _tag = this
        }
        set(tag) {
            rawTag = tag?.toString()
            _tag = tag
        }

    var value: String?
        get() = _value ?: raw.run {
            val value = substringAfter(SEP_CHAR)
            if (value == this) return null
            _value = if (value.endsWith(SOH_CHAR)) value.substring(0, value.lastIndex) else value
            return _value
        }
        set(value) {
            val rawTag = rawTag
            val tag = tag
            raw = toRaw(rawTag, value)
            _rawTag = rawTag
            _tag = tag
            _value = value
        }

    fun insertPrevious(bytes: ByteArray): FixField {
        val previous = FixField(buffer, startIndex, startIndex + bytes.size)

        buffer.insert(bytes, startIndex)

        this.previous?.next = previous
        previous.previous = this.previous
        previous.next = this
        this.previous = previous

        previous.updateNextIndices()

        return previous
    }

    fun insertPrevious(raw: String): FixField = insertPrevious(raw.toByteArray(charset))

    fun insertPrevious(tag: String?, value: String?): FixField = insertPrevious(toRaw(tag, value))

    fun insertPrevious(tag: Int?, value: String?): FixField = insertPrevious(toRaw(tag, value))

    fun insertNext(bytes: ByteArray): FixField {
        val next = FixField(buffer, endIndex, endIndex + bytes.size)

        buffer.insert(bytes, endIndex)

        this.next?.previous = next
        next.previous = this
        next.next = this.next
        this.next = next

        next.updateNextIndices()

        return next
    }

    fun insertNext(raw: String): FixField = insertNext(raw.toByteArray(charset))

    fun insertNext(tag: String?, value: String?): FixField = insertNext(toRaw(tag, value))

    fun insertNext(tag: Int?, value: String?): FixField = insertNext(toRaw(tag, value))

    fun clear() {
        bytes = byteArrayOf()
    }

    private fun updateNextIndices(): Unit? = next?.let { next ->
        val indexDiff = endIndex - next.startIndex
        next.startIndex += indexDiff
        next.endIndex += indexDiff
        next.updateNextIndices()
    }

    private fun toRaw(tag: Int?, value: String?): String {
        return toRaw(tag?.toString(), value)
    }

    private fun toRaw(tag: String?, value: String?): String {
        return "${tag ?: EMPTY_STRING}$SEP_CHAR${value ?: EMPTY_STRING}${SOH_CHAR}"
    }

    override fun next(): FixField? = next ?: when (endIndex) {
        buffer.writerIndex() -> null
        else -> atOffset(
            buffer,
            endIndex,
            charset,
            endIndex,
            buffer.writerIndex()
        )?.also {
            next = it
            it.previous = this
        }
    }

    override fun previous(): FixField? = previous ?: when (startIndex) {
        buffer.readerIndex() -> null
        else -> atOffset(
            buffer,
            startIndex - 1,
            charset,
            buffer.readerIndex(),
            startIndex
        )?.also {
            previous = it
            it.next = this
        }
    }

    fun first(): FixField {
        var current = this

        while (true) {
            current = current.previous() ?: return current
        }
    }

    fun last(): FixField {
        var current = this

        while (true) {
            current = current.next() ?: return current
        }
    }

    override fun toString() = raw

    companion object {
        @JvmOverloads
        fun atOffset(
            buffer: ByteBuf,
            offset: Int,
            charset: Charset = UTF_8,
            fromIndex: Int = buffer.readerIndex(),
            toIndex: Int = buffer.writerIndex(),
        ): FixField? {
            buffer.requireReadable(fromIndex, toIndex)

            if (fromIndex == toIndex) {
                return null
            }

            require(offset in fromIndex until toIndex) {
                "Offset is outside of the specified range: $fromIndex..$toIndex"
            }

            val startIndex = buffer.run {
                val sohIndex = lastIndexOf(SOH_BYTE, fromIndex, offset)
                if (sohIndex < 0) fromIndex else sohIndex + 1
            }

            val endIndex = buffer.run {
                val sohIndex = indexOf(SOH_BYTE, offset, toIndex)
                if (sohIndex < 0) toIndex else sohIndex + 1
            }

            return when {
                startIndex != endIndex -> FixField(buffer, startIndex, endIndex, charset)
                else -> null
            }
        }
    }

    class FixGroup(
        val counter: FixField,
        val delimiter: FixField,
        val tags: Iterable<Int>,
    ) : Iterable<FixField>, FixElement {
        fun decrement() {
            counter.value?.let {
                val value = it.toInt()
                counter.value = (value - 1).toString()
                if(value - 1 == 0) {
                    counter.clear()
                }
            }
        }

        fun increment() {
            counter.value?.let {
                counter.value = (it.toInt() + 1).toString()
            }
        }

        override fun iterator(): Iterator<FixField> = iterator {
            var field: FixField? = delimiter

            while (field != null) {
                if (field === delimiter || field.tag in tags) yield(field) else break
                field = field.next()
            }
        }

        override fun next(): FixGroup? = find(delimiter.next()) { next() }

        override fun previous(): FixGroup? = find(delimiter.previous()) { previous() }

        private inline fun find(start: FixField?, next: FixField.() -> FixField?): FixGroup? {
            val delimiter = delimiter.tag
            var field = start

            while (field != null) {
                val tag = field.tag
                if (tag == delimiter) return FixGroup(counter, field, tags)
                if (tag !in tags) break
                field = field.next()
            }

            return null
        }
    }
}

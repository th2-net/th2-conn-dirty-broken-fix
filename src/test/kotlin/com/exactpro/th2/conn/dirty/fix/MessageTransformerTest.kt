/*
 * Copyright 2022-2024 Exactpro (Exactpro Systems Limited)
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

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.lang.System.lineSeparator
import kotlin.text.Charsets.UTF_8

class MessageTransformerTest {
    @Test fun `set field`() {
        val buffer = MESSAGE.toBuffer()
        val transform = set(49 to "abc") onlyIf (35 matches "A")
        val description = MessageTransformer.transform(buffer, Rule("0", transform))!!.results.joinToString(lineSeparator()) { it.action.toString() }
        assertEquals("set tag 49 = 'abc'", description)
        assertEquals("8=FIX.4.2|9=62|35=A|49=abc|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|10=138|", buffer.asString())
    }

    @Test fun `add field before`() {
        val buffer = MESSAGE.toBuffer()
        val transform = add(123 eq "abc") before (34 matches "177") onlyIf (35 matches "A")
        val description = MessageTransformer.transform(buffer, Rule("0", transform))!!.results.joinToString(lineSeparator()) { it.action.toString() }
        assertEquals("add tag 123 = 'abc' before tag 34 ~= /177/", description)
        assertEquals("8=FIX.4.2|9=73|35=A|49=SERVER|56=CLIENT|123=abc|34=177|52=20090107-18:15:16|98=0|108=30|10=055|", buffer.asString())
    }

    @Test fun `add field after`() {
        val buffer = MESSAGE.toBuffer()
        val transform = add(124 eq "cde") after (34 matches "177") onlyIf (35 matches "A")
        val description = MessageTransformer.transform(buffer, Rule("0", transform))!!.results.joinToString(lineSeparator()) { it.action.toString() }
        assertEquals("add tag 124 = 'cde' after tag 34 ~= /177/", description)
        assertEquals("8=FIX.4.2|9=73|35=A|49=SERVER|56=CLIENT|34=177|124=cde|52=20090107-18:15:16|98=0|108=30|10=062|", buffer.asString())
    }

    @Test fun `move field before`() {
        val buffer = MESSAGE.toBuffer()
        val transform = move(56 matching ".*") before (49 matches ".*") onlyIf (35 matches "A")
        val description = MessageTransformer.transform(buffer, Rule("0", transform))!!.results.joinToString(lineSeparator()) { it.action.toString() }
        assertEquals("move tag 56 ~= /.*/ before tag 49 ~= /.*/", description)
        assertEquals("8=FIX.4.2|9=65|35=A|56=CLIENT|49=SERVER|34=177|52=20090107-18:15:16|98=0|108=30|10=062|", buffer.asString())
    }

    @Test fun `move field after`() {
        val buffer = MESSAGE.toBuffer()
        val transform = move(49 matching ".*") after (56 matches ".*") onlyIf (35 matches "A")
        val description = MessageTransformer.transform(buffer, Rule("0", transform))!!.results.joinToString(lineSeparator()) { it.action.toString() }
        assertEquals("move tag 49 ~= /.*/ after tag 56 ~= /.*/", description)
        assertEquals("8=FIX.4.2|9=65|35=A|56=CLIENT|49=SERVER|34=177|52=20090107-18:15:16|98=0|108=30|10=062|", buffer.asString())
    }

    @Test fun `add field after random one of`() {
        val buffer = MESSAGE.toBuffer()
        val transform = add(124 oneOf listOf("cde", "cbe")) after (34 matches "177") onlyIf (35 matches "A")
        val description = MessageTransformer.transform(buffer, Rule("0", transform))!!.results.joinToString(lineSeparator()) { it.action.toString() }
        assertEquals(description, "add tag 124 = one of [cde, cbe] after tag 34 ~= /177/")
        val resultString = buffer.asString()
        assertTrue("8=FIX.4.2|9=73|35=A|49=SERVER|56=CLIENT|34=177|124=cde|52=20090107-18:15:16|98=0|108=30|10=062|" == resultString ||
                "8=FIX.4.2|9=73|35=A|49=SERVER|56=CLIENT|34=177|124=cbe|52=20090107-18:15:16|98=0|108=30|10=060|" == resultString
        ) { "message $resultString wasn't filled right" }
    }

    @Test fun `replace field`() {
        val buffer = MESSAGE.toBuffer()
        val transform = replace(98 matching "0") with (100 eq "1") onlyIf (35 matches "A")
        val description = MessageTransformer.transform(buffer, Rule("0", transform))!!.results.joinToString(lineSeparator()) { it.action.toString() }
        assertEquals("replace tag 98 ~= /0/ with tag 100 = '1'", description)
        assertEquals("8=FIX.4.2|9=66|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|100=1|108=30|10=096|", buffer.asString())
    }

    @Test fun `remove field`() {
        val buffer = MESSAGE.toBuffer()
        val transform = remove(52 matching ".*") onlyIf (35 matches "A")
        val description = MessageTransformer.transform(buffer, Rule("0", transform))!!.results.joinToString(lineSeparator()) { it.action.toString() }
        assertEquals("remove tag 52 ~= /.*/", description)
        assertEquals("8=FIX.4.2|9=44|35=A|49=SERVER|56=CLIENT|34=177|98=0|108=30|10=044|", buffer.asString())
    }

    companion object {
        private const val MESSAGE = "8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|10=062|"
        private fun String.toBuffer() = Unpooled.buffer().writeBytes(replace('|', SOH_CHAR).toByteArray(UTF_8))
        private fun ByteBuf.asString() = toString(UTF_8).replace(SOH_CHAR, '|')
        private fun field(tag: Int, value: String) = FieldDefinition(tag, value, null, null)
        private fun select(tag: Int, pattern: String) = FieldSelector(tag, null, pattern.toPattern())
        private infix fun Int.eq(value: String) = field(this, value)
        private infix fun Int.to(value: String) = field(this, value)
        private infix fun Int.oneOf(value: List<String>) = FieldDefinition(this, null, null, value)
        private infix fun Int.matches(pattern: String) = select(this, pattern)
        private infix fun Int.matching(pattern: String) = select(this, pattern)
        private fun set(field: FieldDefinition) = Action(set = field)
        private fun add(field: FieldDefinition) = field
        private fun move(field: FieldSelector) = field
        private fun replace(field: FieldSelector) = field
        private fun remove(field: FieldSelector) = Action(remove = field)
        private infix fun FieldDefinition.before(field: FieldSelector) = Action(add = this, before = field)
        private infix fun FieldSelector.before(field: FieldSelector) = Action(move = this, before = field)
        private infix fun FieldDefinition.after(field: FieldSelector) = Action(add = this, after = field)
        private infix fun FieldSelector.after(field: FieldSelector) = Action(move = this, after = field)
        private infix fun FieldSelector.with(field: FieldDefinition) = Action(replace = this, with = field)
        private infix fun Action.onlyIf(condition: FieldSelector) = listOf(Transform(listOf(condition), listOf(this)))
    }
}
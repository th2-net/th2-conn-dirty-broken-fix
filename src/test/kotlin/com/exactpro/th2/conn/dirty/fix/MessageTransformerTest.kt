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

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.lang.System.lineSeparator
import kotlin.text.Charsets.UTF_8
import kotlin.test.assertNotNull

class MessageTransformerTest {
    @Test fun `set field`() {
        val buffer = MESSAGE.toBuffer()
        val transform = set(49 to "abc") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)
        assertEquals("set tag 49 = 'abc'", description)
        assertEquals("8=FIX.4.2|9=62|35=A|49=abc|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|10=138|", buffer.asString())
    }

    @Test fun `add field before`() {
        val buffer = MESSAGE.toBuffer()
        val transform = add(123 eq "abc") before (34 matches "177") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)
        assertEquals("add tag 123 = 'abc' before tag 34 ~= /177/", description)
        assertEquals("8=FIX.4.2|9=73|35=A|49=SERVER|56=CLIENT|123=abc|34=177|52=20090107-18:15:16|98=0|108=30|10=055|", buffer.asString())
    }

    @Test fun `add field after`() {
        val buffer = MESSAGE.toBuffer()
        val transform = add(124 eq "cde") after (34 matches "177") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)
        assertEquals("add tag 124 = 'cde' after tag 34 ~= /177/", description)
        assertEquals("8=FIX.4.2|9=73|35=A|49=SERVER|56=CLIENT|34=177|124=cde|52=20090107-18:15:16|98=0|108=30|10=062|", buffer.asString())
    }

    @Test fun `add duplicate tag`() {
        val buffer = MESSAGE.toBuffer()
        val transform = add(35 eq "A") before (35 matches "A") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)
        assertEquals("8=FIX.4.2|9=70|35=A|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|10=033|", buffer.asString())
    }

    @Test fun `move field before`() {
        val buffer = MESSAGE.toBuffer()
        val transform = move(56 matching ".*") before (49 matches ".*") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)
        assertEquals("move tag 56 ~= /.*/ before tag 49 ~= /.*/", description)
        assertEquals("8=FIX.4.2|9=65|35=A|56=CLIENT|49=SERVER|34=177|52=20090107-18:15:16|98=0|108=30|10=062|", buffer.asString())
    }

    @Test fun `move field after`() {
        val buffer = MESSAGE.toBuffer()
        val transform = move(49 matching ".*") after (56 matches ".*") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)
        assertEquals("move tag 49 ~= /.*/ after tag 56 ~= /.*/", description)
        assertEquals("8=FIX.4.2|9=65|35=A|56=CLIENT|49=SERVER|34=177|52=20090107-18:15:16|98=0|108=30|10=062|", buffer.asString())
    }

    @Test fun `add field after random one of`() {
        val buffer = MESSAGE.toBuffer()
        val transform = add(124 oneOf listOf("cde", "cbe")) after (34 matches "177") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)
        assertEquals(description, "add tag 124 = one of [cde, cbe] after tag 34 ~= /177/")
        val resultString = buffer.asString()
        assertTrue("8=FIX.4.2|9=73|35=A|49=SERVER|56=CLIENT|34=177|124=cde|52=20090107-18:15:16|98=0|108=30|10=062|" == resultString ||
                "8=FIX.4.2|9=73|35=A|49=SERVER|56=CLIENT|34=177|124=cbe|52=20090107-18:15:16|98=0|108=30|10=060|" == resultString
        ) { "message $resultString wasn't filled right" }
    }

    @Test fun `replace field`() {
        val buffer = MESSAGE.toBuffer()
        val transform = replace(98 matching "0") with (100 eq "1") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)
        assertEquals("replace tag 98 ~= /0/ with tag 100 = '1'", description)
        assertEquals("8=FIX.4.2|9=66|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|100=1|108=30|10=096|", buffer.asString())
    }

    @Test fun `remove field`() {
        val buffer = MESSAGE.toBuffer()
        val transform = remove(52 matching ".*") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)
        assertEquals("remove tag 52 ~= /.*/", description)
        assertEquals("8=FIX.4.2|9=44|35=A|49=SERVER|56=CLIENT|34=177|98=0|108=30|10=044|", buffer.asString())
    }

    @Test fun `set field in group`() {
        val buffer = GROUP_MESSAGE.toBuffer()
        val transform = set(110 to "abc") inGroup ("test" where (100 matches "d")) onlyIf (99 matches "2")
        val description = transform.applyTo(buffer)
        assertEquals("set tag 110 = 'abc' on group 'test' where tag 100 ~= /d/", description)
        assertEquals("99=2|100=a|110=b|120=c|100=d|110=abc|120=f|", buffer.asString())
    }

    @Test fun `remove field in group`() {
        val buffer = GROUP_MESSAGE.toBuffer()
        val transform = remove(120 matching "c") inGroup ("test" where (110 matches "b")) onlyIf (99 matches "2")
        val description = transform.applyTo(buffer)
        assertEquals("remove tag 120 ~= /c/ on group 'test' where tag 110 ~= /b/", description)
        assertEquals("99=2|100=a|110=b|100=d|110=e|120=f|", buffer.asString())
    }

    @Test fun `apply action to multiple group entries`() {
        val buffer = GROUP_MESSAGE.toBuffer()
        val transform = remove(120 matching "(.*)") inGroup ("test") onlyIf (99 matches "2")
        transform.applyTo(buffer)
        assertEquals("99=2|100=a|110=b|100=d|110=e|", buffer.asString())
    }

    @Test fun `remove group`() {
        val buffer = GROUP_MESSAGE.toBuffer()
        val transform = removeGroup("test" where (110 matches "b")) onlyIf (99 matches "2")
        val description = transform.applyTo(buffer)
        assertEquals("removeGroup on group 'test' where tag 110 ~= /b/", description)
        assertEquals("99=1|100=d|110=e|120=f|", buffer.asString())
    }

    @Test fun `remove group fully`() {
        val buffer = GROUP_MESSAGE.toBuffer()
        val transform = removeGroup("test" where (110 matches "(.*)")) onlyIf (99 matches "2")
        transform.applyTo(buffer)
        assertEquals("", buffer.asString())
    }

    @Test fun `add group`() {
        val buffer = GROUP_MESSAGE.toBuffer()
        val transform = addGroup(GroupDefinition(99, listOf(field(100, "c"), field(110, "d")))) inGroup ("test") onlyIf (99 matches "2")
        val description = transform.applyTo(buffer)
        assertEquals("addGroup [tag 100 = 'c', tag 110 = 'd'] on group 'test'", description)
        assertEquals("99=3|100=a|110=b|120=c|100=d|110=e|120=f|100=c|110=d|", buffer.asString())
    }

    @Test fun `add group entry when there is no group`() {
        val buffer = MESSAGE.toBuffer()
        val transform = GroupDefinition(199, listOf(field(200, "c"), field(210, "d"))) after (34 matches ".*") onlyIf (34 matches "177")
        val description = transform.applyTo(buffer)
        assertEquals("addGroup [tag 200 = 'c', tag 210 = 'd'] after tag 34 ~= /.*/", description)
        assertEquals("8=FIX.4.2|9=83|35=A|49=SERVER|56=CLIENT|34=177|199=1|200=c|210=d|52=20090107-18:15:16|98=0|108=30|10=184|", buffer.asString())
    }

    @Test fun `delete all delimiter group fields`() {
        val buffer = NESTED_GROUP.toBuffer()
        val transform = remove(100 matching ".*") inGroup ("test") onlyIf (99 matches "2")
        val description = transform.applyTo(buffer)

        assertEquals("8=FIX.4.2|9=138|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|50=2|23=1|99=2|110=b|120=c|110=e|120=f|23=2|99=2|110=b|120=c|110=e|120=f|98=0|108=30|10=085|", buffer.asString())
    }

    @Test fun `move counter in group`() {
        val buffer = NESTED_GROUP.toBuffer()
        val transform = move(99 matching ".*") after (100 matching ".*") inGroup ("test2") onlyIf (99 matches "2")
        transform.applyTo(buffer)

        assertEquals("8=FIX.4.2|9=162|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|50=2|23=1|100=a|99=2|110=b|120=c|100=d|110=e|120=f|23=2|100=a|99=2|110=b|120=c|100=d|110=e|120=f|98=0|108=30|10=024|", buffer.asString())
    }

    @Test fun `delete counter field`() {
        val buffer = NESTED_GROUP.toBuffer()
        val transform = remove(99 matchingAll  ".*") onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)

        assertEquals("8=FIX.4.2|9=152|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|50=2|23=1|100=a|110=b|120=c|100=d|110=e|120=f|23=2|100=a|110=b|120=c|100=d|110=e|120=f|98=0|108=30|10=083|", buffer.asString())
    }

    @Test fun `change counter`() {
        val buffer = GROUP_MESSAGE.toBuffer()
        val transform = set(99 to "1") onlyIf (99 matches "2")
        val description = transform.applyTo(buffer)

        assertEquals("99=1|100=a|110=b|120=c|100=d|110=e|120=f|", buffer.asString())
    }

    @Test fun `change counter nested`() {
        val buffer = NESTED_GROUP.toBuffer()
        val transform = set(99 to "1") inGroup("test2".where(23 matches "2")) onlyIf (35 matches "A")
        val description = transform.applyTo(buffer)

        assertEquals("8=FIX.4.2|9=162|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|50=2|23=1|99=2|100=a|110=b|120=c|100=d|110=e|120=f|23=2|99=1|100=a|110=b|120=c|100=d|110=e|120=f|98=0|108=30|10=023|", buffer.asString())
    }

    private fun List<Transform>.applyTo(buffer: ByteBuf): String {
        val transform = MessageTransformer.transform(buffer, Rule("test", this))
        assertNotNull(transform, "transformation yielded no results")
        return transform.results.joinToString(lineSeparator()) { it.action.toString() }
    }

    companion object {
        private const val MESSAGE = "8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|108=30|10=062|"
        private const val GROUP_MESSAGE = "99=2|100=a|110=b|120=c|100=d|110=e|120=f|"
        private const val NESTED_GROUP = "8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|50=2|23=1|99=2|100=a|110=b|120=c|100=d|110=e|120=f|23=2|99=2|100=a|110=b|120=c|100=d|110=e|120=f|98=0|108=30|10=062|"
        private val CONTEXT = Context(
            groups = mapOf(
                "test" to Group(99, 100, setOf(110, 120)),
                "test2" to Group(50, 23, setOf(99, 100, 110, 120))
            )
        )

        private fun String.toBuffer() = Unpooled.buffer().writeBytes(replace('|', SOH_CHAR).toByteArray(UTF_8))
        private fun ByteBuf.asString() = toString(UTF_8).replace(SOH_CHAR, '|')
        private fun field(tag: Int, value: String) = FieldDefinition(tag, value, null, null)
        private fun select(tag: Int, pattern: String) = FieldSelector(tag, null, pattern.toPattern())
        private infix fun Int.eq(value: String) = field(this, value)
        private infix fun Int.to(value: String) = field(this, value)
        private infix fun Int.oneOf(value: List<String>) = FieldDefinition(this, null, null, value)
        private infix fun Int.matches(pattern: String) = select(this, pattern)
        private infix fun Int.matching(pattern: String) = select(this, pattern)
        private fun selectAll(tag: Int, pattern: String) = FieldSelector(tag, null, pattern.toPattern(), false)
        private infix fun Int.matchingAll(pattern: String) = selectAll(this, pattern)
        private fun set(field: FieldDefinition) = Action(set = field)
        private fun add(field: FieldDefinition) = field
        private fun move(field: FieldSelector) = field
        private fun replace(field: FieldSelector) = field
        private fun remove(field: FieldSelector) = Action(remove = field)
        private fun removeGroup(group: GroupSelector) = Action(removeGroup = group)
        private fun addGroup(group: GroupDefinition) = Action(addGroup = group)
        private infix fun FieldDefinition.before(field: FieldSelector) = Action(add = this, before = field)
        private infix fun GroupDefinition.after(field: FieldSelector) = Action(addGroup = this, after = field)
        private infix fun FieldSelector.before(field: FieldSelector) = Action(move = this, before = field)
        private infix fun FieldDefinition.after(field: FieldSelector) = Action(add = this, after = field)
        private infix fun FieldSelector.after(field: FieldSelector) = Action(move = this, after = field)
        private infix fun FieldSelector.with(field: FieldDefinition) = Action(replace = this, with = field)
        private infix fun Action.onlyIf(condition: FieldSelector) = listOf(Transform(listOf(condition), listOf(this)))
        private infix fun String.where(selector: FieldSelector) = GroupSelector(this, listOf(selector)).apply { init(CONTEXT) }
        private infix fun Action.inGroup(name: String) = copy(groupScope = GroupSelector(name, emptyList()).apply { init(CONTEXT) })
        private infix fun Action.inGroup(group: GroupSelector) = copy(groupScope = group)
    }
}
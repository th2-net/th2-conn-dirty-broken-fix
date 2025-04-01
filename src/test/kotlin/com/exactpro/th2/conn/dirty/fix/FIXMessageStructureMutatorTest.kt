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
package com.exactpro.th2.conn.dirty.fix

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.text.Charsets.UTF_8

class FIXMessageStructureMutatorTest {

    @Test fun `move header inside body fields`() {
        val newMessage = MESSAGE_STRUCTURE_MUTATOR.moveHeader(HeaderPosition.INSIDE_BODY, MESSAGE.toBuffer())
        assertEquals("11=TEST|8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|55=TEST|89=10|10=062|", newMessage.asString())
    }

    @Test fun `move header after body fields`() {
        val newMessage = MESSAGE_STRUCTURE_MUTATOR.moveHeader(HeaderPosition.AFTER_BODY, MESSAGE.toBuffer())
        assertEquals("11=TEST|55=TEST|8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|89=10|10=062|", newMessage.asString())
    }

    @Test fun `move header inside trailer fields`() {
        val newMessage = MESSAGE_STRUCTURE_MUTATOR.moveHeader(HeaderPosition.INSIDE_TRAILER, MESSAGE.toBuffer())
        assertEquals("11=TEST|55=TEST|89=10|8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|10=062|", newMessage.asString())
    }

    @Test fun `move header after trailer fields`() {
        val newMessage = MESSAGE_STRUCTURE_MUTATOR.moveHeader(HeaderPosition.AFTER_TRAILER, MESSAGE.toBuffer())
        assertEquals("11=TEST|55=TEST|89=10|10=062|8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|", newMessage.asString())
    }

    @Test fun `move trailer before header`() {
        val newMessage = MESSAGE_STRUCTURE_MUTATOR.moveTrailer(TrailerPosition.BEFORE_HEADER, MESSAGE.toBuffer())
        assertEquals("89=10|10=062|8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|11=TEST|55=TEST|", newMessage.asString())
    }

    @Test fun `move trailer inside header`() {
        val newMessage = MESSAGE_STRUCTURE_MUTATOR.moveTrailer(TrailerPosition.INSIDE_HEADER, MESSAGE.toBuffer())
        assertEquals("8=FIX.4.2|89=10|10=062|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|11=TEST|55=TEST|", newMessage.asString())
    }

    @Test fun `move trailer after header`() {
        val newMessage = MESSAGE_STRUCTURE_MUTATOR.moveTrailer(TrailerPosition.AFTER_HEADER, MESSAGE.toBuffer())
        assertEquals("8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|89=10|10=062|11=TEST|55=TEST|", newMessage.asString())
    }

    @Test fun `move trailer inside body`() {
        val newMessage = MESSAGE_STRUCTURE_MUTATOR.moveTrailer(TrailerPosition.INSIDE_BODY, MESSAGE.toBuffer())
        assertEquals("8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|11=TEST|89=10|10=062|55=TEST|", newMessage.asString())
    }

    companion object {
        private const val MESSAGE = "8=FIX.4.2|9=65|35=A|49=SERVER|56=CLIENT|34=177|52=20090107-18:15:16|98=0|11=TEST|55=TEST|89=10|10=062|"
        private val HEADER_TAGS = setOf<Int>(8, 9, 35, 49, 56, 34, 52, 98)
        private val TRAILER_TAGS = setOf<Int>(89, 10)
        private val MESSAGE_STRUCTURE_MUTATOR = FIXMessageStructureMutator(
            HEADER_TAGS,
            TRAILER_TAGS
        )
        private fun String.toBuffer() = Unpooled.buffer().writeBytes(replace('|', SOH_CHAR).toByteArray(UTF_8))
        private fun ByteBuf.asString() = toString(UTF_8).replace(SOH_CHAR, '|')
    }
}
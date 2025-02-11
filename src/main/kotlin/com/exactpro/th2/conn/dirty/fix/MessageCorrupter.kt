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

import com.exactpro.th2.constants.Constants.*
import io.netty.buffer.ByteBuf



const val VERY_BIG_NUMBER = "1_000_000_000_000_000_000_000"
enum class DataType {
    INT, STRING, BOOLEAN
}

fun getRandomString(length: Int) : String {
    val allowedChars = ('A'..'Z') + ('a'..'z') + ('0'..'9')
    return (1..length)
        .map { allowedChars.random() }
        .joinToString("")
}

data class TagInfo(val dataType: DataType, val length: Int)
data class CorruptionConfiguration(val tagsThatAreNotRelatedToHeaderTrailer: List<Tag>, val tagsThatNotExist: List<Tag>, val insertBefore: Boolean = true)

typealias MetadataUpdate = Map<String, String>

val DEFAULT_CONTEXT = Context()
enum class CorruptionAction {
    REMOVE_TAG {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate {
            val actions = listOf(
                Action(
                    remove = FieldSelector(tag)
                )
            )
            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if(updateLength) {
                buf.updateLength()
            }

            if(updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "REMOVE_TAG",
                "tag" to "$tag"
            )
        }
    },
    NULL_TAG {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate {
            val actions = listOf(
                Action(
                    set = FieldDefinition(
                        tag,
                        value = "\u0000"
                    )
                )
            )
            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if(updateLength) {
                buf.updateLength()
            }

            if(updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "NULL_TAG",
                "tag" to "$tag"
            )
        }
    },
    EMPTY_TAG {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate {
            val actions = listOf(
                Action(
                    set = FieldDefinition(
                        tag,
                        value = ""
                    )
                )
            )
            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if(updateLength) {
                buf.updateLength()
            }

            if(updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "EMPTY_TAG",
                "tag" to "$tag"
            )
        }
    },
    DUPLICATE {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate {
            val actions = listOf(
                Action(
                    add = FieldDefinition(
                        tag,
                        value = buf.findField(tag)?.value ?: ""
                    ),
                    after = FieldSelector(
                        tag
                    )
                )
            )
            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if(updateLength) {
                buf.updateLength()
            }

            if(updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "DUPLICATE",
                "tag" to "$tag"
            )
        }
    },
    OUT_OF_RANGE {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate? {
            val actions = when(tagInfo.dataType) {
                DataType.INT -> {
                    listOf(
                        Action(
                            set = FieldDefinition(tag, VERY_BIG_NUMBER)
                        )
                    )
                }
                DataType.STRING -> {
                    listOf(
                        Action(
                            set = FieldDefinition(tag, getRandomString(tagInfo.length + 1))
                        )
                    )
                }
                DataType.BOOLEAN -> {
                    return null
                }
            }
            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if(updateLength) {
                buf.updateLength()
            }

            if(updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "OUT_OF_RANGE",
                "tag" to "$tag"
            )
        }
    },
    NEGATIVE_NUMBER {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate? {
            val actions = when (tagInfo.dataType) {
                DataType.INT -> {
                    listOf(
                        Action(
                            set = FieldDefinition(tag, "-1")
                        )
                    )
                }

                DataType.STRING -> {
                    return null
                }
                DataType.BOOLEAN -> {
                    return null
                }
            }
            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if (updateLength) {
                buf.updateLength()
            }

            if (updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "NEGATIVE_NUMBER",
                "tag" to "$tag"
            )
        }
    },
    INVALID_DATA_TYPE {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate? {
            val actions = when (tagInfo.dataType) {
                DataType.INT -> {
                    listOf(
                        Action(
                            set = FieldDefinition(tag, "string_value")
                        )
                    )
                }

                DataType.STRING -> {
                    listOf(
                        Action(
                            set = FieldDefinition(tag, "123")
                        )
                    )
                }

                DataType.BOOLEAN -> {
                    listOf(
                        Action(
                            set = FieldDefinition(tag, "string_value")
                        )
                    )
                }
            }
            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if (updateLength) {
                buf.updateLength()
            }

            if (updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "INVALID_DATA_TYPE",
                "tag" to "$tag"
            )
        }
    },
    MOVE_TAG_OUT_OF_ORDER {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate? {
            val field = buf.findField(tag) ?: return null

            val fieldBefore = field.previous()
            val fieldAfter = field.next()

            if(fieldAfter == null && fieldBefore == null) {
                return null
            }

            val actions = if(fieldBefore != null) {
                listOf(
                    Action(
                        move = FieldSelector(tag),
                        before = FieldSelector(fieldBefore.tag)
                    )
                )
            } else if(fieldAfter != null) {
                listOf(
                    Action(
                        move = FieldSelector(tag),
                        after = FieldSelector(fieldAfter.tag)
                    )
                )
            } else {
                return null
            }

            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if (updateLength) {
                buf.updateLength()
            }

            if (updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "TAG_OUT_OF_ORDER",
                "tag" to "$tag"
            )
        }
    },
    TAG_THAT_IS_NOT_EXIST {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate {
            val actions = if(corruptionConfiguration.insertBefore) {
                listOf(
                    Action(
                        add = FieldDefinition(corruptionConfiguration.tagsThatNotExist.random(), "value"),
                        before = FieldSelector(tag)
                    )
                )
            } else {
                listOf(
                    Action(
                        add = FieldDefinition(corruptionConfiguration.tagsThatNotExist.random(), "value"),
                        after = FieldSelector(tag)
                    )
                )
            }

            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if (updateLength) {
                buf.updateLength()
            }

            if (updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "NON_EXSISTING_TAG",
                "tag" to "$tag"
            )
        }
    },
    TAG_THAT_NOT_BELONGS_TO_HEADER_TRAILER {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate {
            val actions = if(corruptionConfiguration.insertBefore) {
                listOf(
                    Action(
                        add = FieldDefinition(corruptionConfiguration.tagsThatAreNotRelatedToHeaderTrailer.random(), "value"),
                        before = FieldSelector(tag)
                    )
                )
            } else {
                listOf(
                    Action(
                        add = FieldDefinition(corruptionConfiguration.tagsThatAreNotRelatedToHeaderTrailer.random(), "value"),
                        after = FieldSelector(tag)
                    )
                )
            }

            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if (updateLength) {
                buf.updateLength()
            }

            if (updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "TAG_THAT_NOT_BELONGS_TO_HEADER_TRAILER",
                "tag" to "$tag"
            )
        }
    },
    INVALID_LENGTH_UP {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate? {

            val field = buf.findField(BODY_LENGTH_TAG) ?: return null

            val actions = listOf(
                Action(
                    replace = FieldSelector(BODY_LENGTH_TAG),
                    with = FieldDefinition(BODY_LENGTH_TAG, ((field.value?.toIntOrNull() ?: buf.writerIndex()) + 1).toString())
                )
            )

            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if (updateLength) {
                buf.updateLength()
            }

            if (updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "TAG_THAT_NOT_BELONGS_TO_HEADER_TRAILER",
                "tag" to "$tag"
            )
        }
    },
    INVALID_LENGTH_DOWN {
        override fun transform(
            buf: ByteBuf,
            tag: Tag,
            updateChecksum: Boolean,
            updateLength: Boolean,
            tagInfo: TagInfo,
            corruptionConfiguration: CorruptionConfiguration
        ): MetadataUpdate? {

            val field = buf.findField(BODY_LENGTH_TAG) ?: return null

            val actions = listOf(
                Action(
                    replace = FieldSelector(BODY_LENGTH_TAG),
                    with = FieldDefinition(BODY_LENGTH_TAG, ((field.value?.toIntOrNull() ?: buf.writerIndex()) - 1).toString())
                )
            )

            MessageTransformer.transformWithoutResults(buf, actions, DEFAULT_CONTEXT)

            if (updateLength) {
                buf.updateLength()
            }

            if (updateChecksum) {
                buf.updateChecksum()
            }

            return mapOf(
                "corruptionType" to "TAG_THAT_NOT_BELONGS_TO_HEADER_TRAILER",
                "tag" to "$tag"
            )
        }
    };

    abstract fun transform(
        buf: ByteBuf,
        tag: Tag,
        updateChecksum: Boolean,
        updateLength: Boolean,
        tagInfo: TagInfo,
        corruptionConfiguration: CorruptionConfiguration
    ): MetadataUpdate?
}

class HeaderTrailerTags {
    companion object {
        val HEADER_TRAILER_TAGS = listOf(
            BEGIN_STRING_TAG,
            BODY_LENGTH_TAG,
            MSG_TYPE_TAG,
            DEFAULT_APPL_VER_ID_TAG,
            SENDER_COMP_ID_TAG,
            TARGET_COMP_ID_TAG,
            MSG_SEQ_NUM_TAG,
            POSS_DUP_TAG,
            POSS_RESEND_TAG,
            SENDING_TIME_TAG,
            CHECKSUM_TAG
        )

        val HEADER_TRAILER_TAGS_INFO = mapOf(
            BEGIN_STRING_TAG to TagInfo(DataType.STRING, 8),
            BODY_LENGTH_TAG to TagInfo(DataType.STRING, 4),
            MSG_TYPE_TAG to TagInfo(DataType.STRING, 3),
            DEFAULT_APPL_VER_ID_TAG to TagInfo(DataType.STRING, 1),
            SENDER_COMP_ID_TAG to TagInfo(DataType.STRING, 10),
            TARGET_COMP_ID_TAG to TagInfo(DataType.STRING, 10),
            MSG_SEQ_NUM_TAG to TagInfo(DataType.INT, 9),
            POSS_DUP_TAG to TagInfo(DataType.BOOLEAN, 1),
            POSS_RESEND_TAG to TagInfo(DataType.BOOLEAN, 1),
            SENDING_TIME_TAG to TagInfo(DataType.STRING, 1),
            CHECKSUM_TAG to TagInfo(DataType.INT, 3)
        )
    }
}

val HEADER_TRAILER_TAGS = HeaderTrailerTags.HEADER_TRAILER_TAGS

val HEADER_TRAILER_TAGS_INFO = HeaderTrailerTags.HEADER_TRAILER_TAGS_INFO

object CorruptionGenerator {
    fun createTransformationSequenceJava(tags: List<Tag>, tagInfos: Map<Tag, TagInfo>, includeAdditionalTags: Boolean = false) = createTransformationSequence(tags, tagInfos, includeAdditionalTags).toList()
    fun createTransformationSequence(tags: List<Tag>, tagInfos: Map<Tag, TagInfo>, includeAdditionalTags: Boolean = false) = sequence {
        check(tags.all { tag -> tagInfos.containsKey(tag) }) { "Missing tag information for tags: ${tags.filter { !tagInfos.containsKey(it) }.joinToString(",")}" }
        for(tag in tags) {
            for (corruption in CorruptionAction.values()) {

                var updateChecksum = true
                var updateLength = true

                if(tag == 10) {
                    updateChecksum = false
                }

                if(!includeAdditionalTags) {
                    if(corruption == CorruptionAction.TAG_THAT_IS_NOT_EXIST) {
                        continue
                    }

                    if(corruption == CorruptionAction.TAG_THAT_NOT_BELONGS_TO_HEADER_TRAILER) {
                        continue
                    }
                }

                if(tag == 9) {
                    when(corruption) {
                        CorruptionAction.REMOVE_TAG -> { updateLength = false}
                        CorruptionAction.NULL_TAG -> { updateLength = false}
                        CorruptionAction.EMPTY_TAG -> { updateLength = false }
                        CorruptionAction.DUPLICATE -> { }
                        CorruptionAction.OUT_OF_RANGE -> { updateLength = false}
                        CorruptionAction.NEGATIVE_NUMBER -> { updateLength = false}
                        CorruptionAction.INVALID_DATA_TYPE -> { updateLength = false }
                        CorruptionAction.MOVE_TAG_OUT_OF_ORDER -> {}
                        CorruptionAction.TAG_THAT_IS_NOT_EXIST -> {}
                        CorruptionAction.TAG_THAT_NOT_BELONGS_TO_HEADER_TRAILER -> {}
                        CorruptionAction.INVALID_LENGTH_UP -> { updateLength = false }
                        CorruptionAction.INVALID_LENGTH_DOWN -> {updateLength = false}
                    }
                }

                if(tagInfos[tag]!!.dataType == DataType.STRING && corruption == CorruptionAction.NEGATIVE_NUMBER) {
                    continue
                }

                if(tagInfos[tag]!!.dataType == DataType.BOOLEAN && corruption == CorruptionAction.NEGATIVE_NUMBER) {
                    continue
                }

                if(tagInfos[tag]!!.dataType == DataType.BOOLEAN && corruption == CorruptionAction.OUT_OF_RANGE) {
                    continue
                }

                if(tag != BODY_LENGTH_TAG && corruption == CorruptionAction.INVALID_LENGTH_UP) {
                    continue
                }

                if(tag != BODY_LENGTH_TAG && corruption == CorruptionAction.INVALID_LENGTH_DOWN) {
                    continue
                }

                val corruptionFunction = {
                        buf: ByteBuf ->
                    createCorruptionCarry(corruption, buf, tag, updateChecksum, updateLength, tagInfos)
                }
                yield(corruptionFunction)
            }
        }
    }

    private fun createCorruptionCarry(
        corruption: CorruptionAction,
        buf: ByteBuf,
        tag: Tag,
        updateChecksum: Boolean,
        updateLength: Boolean,
        tagInfos: Map<Tag, TagInfo>,
        corruptionConfiguration: CorruptionConfiguration = CorruptionConfiguration(listOf(453), listOf(10001))
    ) = corruption.transform(
        buf = buf,
        tag = tag,
        updateChecksum = updateChecksum,
        updateLength = updateLength,
        tagInfo = tagInfos[tag]!!,
        corruptionConfiguration = corruptionConfiguration
    )
}

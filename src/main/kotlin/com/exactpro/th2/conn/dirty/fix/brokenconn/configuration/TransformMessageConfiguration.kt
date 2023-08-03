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

import com.exactpro.th2.conn.dirty.fix.Action
import com.exactpro.th2.conn.dirty.fix.FieldDefinition
import com.exactpro.th2.constants.Constants

data class TransformMessageConfiguration(
    val actions: List<Action> = emptyList(),
    val messageType: String,
    val numberOfTimesToTransform: Int,
    val useOldPasswords: Boolean = false,
    val newUsername: String? = null,
    val newPassword: String? = null,
    val newCompId: String? = null,
    val newTargetId: String? = null
) {
    private val simpleActions: List<Action>
    init {
        val simpleActions = mutableListOf<Action>()
        newUsername?.let {
            simpleActions.add(
                Action(
                    set = FieldDefinition(
                        tag = Constants.USERNAME_TAG,
                        value = it,
                        tagOneOf = null,
                        valueOneOf = null
                    )
                )
            )
        }

        newCompId?.let {
            simpleActions.add(
                Action(
                    set = FieldDefinition(
                        tag = Constants.SENDER_COMP_ID_TAG,
                        value = it,
                        tagOneOf = null,
                        valueOneOf = null
                    )
                )
            )
        }

        newTargetId?.let {
            simpleActions.add(
                Action(
                    set = FieldDefinition(
                        tag = Constants.TARGET_COMP_ID_TAG,
                        value = it,
                        tagOneOf = null,
                        valueOneOf = null
                    )
                )
            )
        }

        this.simpleActions = simpleActions
    }
    val combinedActions = actions + simpleActions
}
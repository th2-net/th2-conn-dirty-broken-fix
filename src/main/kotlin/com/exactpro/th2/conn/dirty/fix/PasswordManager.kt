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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write
import mu.KotlinLogging
import net.lingala.zip4j.io.inputstream.ZipInputStream
import org.apache.hc.client5.http.classic.methods.HttpGet
import org.apache.hc.client5.http.impl.classic.HttpClients
import org.apache.hc.core5.http.HttpStatus


class PasswordManager(
    private val infraUrl: String?,
    initialPassword: String?,
    initialNewPassword: String?,
    username: String,
) {
    private val lock = ReentrantReadWriteLock()
    private val passwordSecretName = "${username}_password"
    private val newPasswordSecretName = "${username}_new_password"
    private val previousPasswordSecretName = "${username}_previous_password_json"

    private val schemaName = infraUrl?.split("/")?.lastOrNull() ?: ""
    private val secretFileName = "${schemaName}-${CUSTOM_SECRETS_SUFFIX}"

    var password: String? = initialPassword?.let { it.ifBlank { null } }
        private set
    var newPassword: String? = initialNewPassword?.let { it.ifBlank { null } }
        private set
    var previouslyUsedPasswords: MutableList<String> = mutableListOf()
        private set

    fun <T> use(func: PasswordManager.() -> T) = lock.read {
        this.func()
    }

    fun poll(): Unit = lock.write {
        if(infraUrl == null) return@write
        HttpClients.createDefault().use { httpClient ->
            val httpGet = HttpGet(infraUrl)

            httpClient.execute(httpGet) { response ->
                if (response.code != HttpStatus.SC_OK) {
                    K_LOGGER.error { "Error while pulling passwords: ${response.code}" }
                    return@execute
                }
                val responseMap: Map<String, String> = OBJECT_MAPPER.readValue(response.entity.content)

                val propContent = responseMap[CONTENT_PROPERTY]
                    ?: error("Error while polling new passwords. No $CONTENT_PROPERTY in response.")
                val zipPassword = responseMap[PASSWORD_PROPERTY]?.toCharArray()
                    ?: error("Error while polling new passwords. No $PASSWORD_PROPERTY in response.")

                val zipContent: ByteArray = Base64.getDecoder().decode(propContent.toByteArray())

                val zipInputStream = ZipInputStream(ByteArrayInputStream(zipContent), zipPassword)
                val reader = BufferedReader(InputStreamReader(zipInputStream))
                var entry = zipInputStream.nextEntry
                while (entry != null) {
                    val entryName = entry.fileName
                    K_LOGGER.info { "Archive entry name: $entryName" }
                    K_LOGGER.info { "Secret file name: $secretFileName" }
                    if (entryName.contains(secretFileName)) {
                        val lineContent = reader.readLine()
                        if (lineContent.isNotBlank()) {
                            runCatching { OBJECT_MAPPER.readValue<Map<String, String>>(lineContent) }
                                .onFailure { K_LOGGER.error(it) { "Error while getting secrets" } }
                                .onSuccess { secrets ->
                                    K_LOGGER.info { "Decoded secrets: $secrets" }
                                    secrets[newPasswordSecretName]?.let {
                                        newPassword = Base64.getDecoder().decode(it).decodeToString().ifBlank { null }
                                    }

                                    secrets[passwordSecretName]?.let {
                                        password = Base64.getDecoder().decode(it).decodeToString().ifBlank { null }
                                    }

                                    secrets[previousPasswordSecretName]?.let { secret ->
                                        val json = Base64.getDecoder().decode(secret).decodeToString().ifBlank { null }

                                        if(json == null) {
                                            previouslyUsedPasswords.clear()
                                        } else {
                                            runCatching { OBJECT_MAPPER.readValue<List<String>>(json) }
                                                .onFailure { K_LOGGER.error(it) { "Error while getting $previousPasswordSecretName." } }
                                                .onSuccess {
                                                    previouslyUsedPasswords.clear()
                                                    previouslyUsedPasswords.addAll(it)
                                                }
                                        }
                                    }
                            }
                        }
                    }
                    entry = zipInputStream.nextEntry
                }
            }
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {  }
        private val OBJECT_MAPPER = ObjectMapper()
        private const val CONTENT_PROPERTY = "content"
        private const val PASSWORD_PROPERTY = "password"
        private const val CUSTOM_SECRETS_SUFFIX = "custom-secrets"
    }
}
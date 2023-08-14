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

import com.fasterxml.jackson.databind.ObjectMapper
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.util.Base64
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

    var password: String? = initialPassword
        private set
    var newPassword: String? = initialNewPassword
        private set
    var previouslyUsedPasswords: MutableList<String> = mutableListOf()
        private set

    fun <T> use(func: PasswordManager.() -> T) = lock.read {
        this.func()
    }

    fun poll(): Unit = lock.write {
        if(infraUrl == null) return@write
        try {
            HttpClients.createDefault().use { httpClient ->
                val httpGet = HttpGet(infraUrl)

                httpClient.execute(httpGet) { response ->
                    if (response.code != HttpStatus.SC_OK) {
                        K_LOGGER.error { "Error while pulling passwords: ${response.code}" }
                    }
                    val responseMap: Map<String, String> =
                        OBJECT_MAPPER.readValue(response.entity.content, Map::class.java) as Map<String, String>

                    val content = responseMap[CONTENT_PROPERTY]
                        ?: error("Error while polling new passwords. No $CONTENT_PROPERTY in response.")
                    val zipPassword = responseMap[PASSWORD_PROPERTY]?.toCharArray()
                        ?: error("Error while polling new passwords. No $PASSWORD_PROPERTY in response.")

                    val zipContent: ByteArray = Base64.getDecoder().decode(content.toByteArray())

                    val zipInputStream = ZipInputStream(ByteArrayInputStream(zipContent), zipPassword)
                    val reader = BufferedReader(InputStreamReader(zipInputStream))
                    var entry = zipInputStream.nextEntry
                    while (entry != null) {
                        val entryName = entry.fileName
                        if (entryName == newPasswordSecretName) {
                            newPassword = reader.readLine()?.ifBlank { null }
                        }

                        if (entryName == passwordSecretName) {
                            password = reader.readLine()?.ifBlank { null }
                        }

                        if (entryName == previousPasswordSecretName) {
                            val json = reader.readLine()?.ifBlank { null }
                            if(json != null) {
                                (OBJECT_MAPPER.readValue(json, List::class.java) as List<String>).apply {
                                    previouslyUsedPasswords.clear()
                                    previouslyUsedPasswords.addAll(this)
                                }
                            } else {
                                previouslyUsedPasswords.clear()
                            }
                        }
                        entry = zipInputStream.nextEntry
                    }
                }
            }
        } catch (e: Exception) {
            K_LOGGER.error(e) { "Error while polling passwords." }
        }
    }

    companion object {
        private val K_LOGGER = KotlinLogging.logger {  }
        private val OBJECT_MAPPER = ObjectMapper()
        private const val CONTENT_PROPERTY = "content"
        private const val PASSWORD_PROPERTY = "password"
    }
}
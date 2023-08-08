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
package com.exactpro.th2.conn.dirty.fix

import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyFactory
import java.security.interfaces.RSAPublicKey
import java.security.spec.X509EncodedKeySpec
import java.util.*
import javax.crypto.Cipher

enum class KeyFileType {
    PEM_PUBLIC_KEY {
        override fun encrypt(
            key: String,
            value: String,
            keyEncryptAlgorithm: String,
            encryptAlgorithm: String,
            operationMode: OperationMode
        ): String {
            val privateKeyPEM = key
                .replace(BEGIN_PUBLIC_KEY, "")
                .replace(System.lineSeparator(), "")
                .replace(END_PUBLIC_KEY, "")

            val publicKey = KeyFactory.getInstance(keyEncryptAlgorithm)
                .generatePublic(X509EncodedKeySpec(Base64.getDecoder().decode(privateKeyPEM))) as RSAPublicKey

            Cipher.getInstance(encryptAlgorithm).apply {
                init(Cipher.ENCRYPT_MODE, publicKey)
                return String(Base64.getEncoder().encode(doFinal(value.toByteArray())))
            }
        }
    };

    abstract fun encrypt(
        key: String,
        value: String,
        keyEncryptAlgorithm: String,
        encryptAlgorithm: String,
        operationMode: OperationMode = OperationMode.ENCRYPT_MODE
    ): String

    companion object {
        private const val BEGIN_PUBLIC_KEY = "-----BEGIN PUBLIC KEY-----"
        private const val END_PUBLIC_KEY = "-----END PUBLIC KEY-----"

        enum class OperationMode(val value: Int) {
            ENCRYPT_MODE(Cipher.ENCRYPT_MODE),
        }
    }
}
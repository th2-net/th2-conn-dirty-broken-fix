/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2;

import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BrokenConnConfiguration;
import org.jetbrains.annotations.NotNull;

import static java.util.Objects.requireNonNullElse;

public class TestUtils {
    @NotNull
    public static FixHandlerSettings createHandlerSettings() {
        return createHandlerSettings(null, null, false);
    }

    public static FixHandlerSettings createHandlerSettings(BrokenConnConfiguration brokenConfig) {
        return createHandlerSettings(brokenConfig, 30, false);
    }

    public static FixHandlerSettings createHandlerSettings(
        BrokenConnConfiguration brokenConfig,
        Integer hbtInt,
        boolean useNextExpectedSeqNumber
    ) {
        final FixHandlerSettings fixHandlerSettings = new FixHandlerSettings();
        fixHandlerSettings.setHost("127.0.0.1");
        fixHandlerSettings.setPort(8080);
        fixHandlerSettings.setBeginString("FIXT.1.1");
        fixHandlerSettings.setHeartBtInt(requireNonNullElse(hbtInt, 30));
        fixHandlerSettings.setDisconnectCleanUpTimeoutMs(100);
        fixHandlerSettings.setUseNextExpectedSeqNum(useNextExpectedSeqNumber);
        fixHandlerSettings.setSenderCompID("client");
        fixHandlerSettings.setTargetCompID("server");
        fixHandlerSettings.setEncryptMethod("0");
        fixHandlerSettings.setUsername("username");
        fixHandlerSettings.setPassword("pass");
        fixHandlerSettings.setTestRequestDelay(10);
        fixHandlerSettings.setReconnectDelay(5);
        fixHandlerSettings.setDisconnectRequestDelay(5);
        fixHandlerSettings.setResetSeqNumFlag(false);
        fixHandlerSettings.setResetOnLogon(false);
        fixHandlerSettings.setDefaultApplVerID("9");
        fixHandlerSettings.setSenderSubID("trader");
        fixHandlerSettings.setCradleSaveTimeoutMs(100);
        fixHandlerSettings.setBrokenConnConfiguration(brokenConfig);
        return fixHandlerSettings;
    }
}

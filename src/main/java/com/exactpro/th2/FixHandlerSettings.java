/*
 * Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.Security;
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerSettings;

public class FixHandlerSettings implements IHandlerSettings {
    private String host = null;
    private int port = 0;
    private Security security = new Security();
    private String beginString = "FIXT.1.1";
    private long heartBtInt = 30;
    private String senderCompID;
    private String targetCompID;
    private String defaultApplVerID;
    private String senderSubID;
    private String encryptMethod;
    private String username;
    private String password;
    private Boolean resetSeqNumFlag = false;
    private Boolean resetOnLogon = false;
    private int testRequestDelay = 60;
    private int reconnectDelay = 5;
    private int disconnectRequestDelay = 5;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Security getSecurity() {
        return security;
    }

    public void setSecurity(Security security) {
        this.security = security;
    }

    public String getBeginString() {
        return beginString;
    }

    public void setBeginString(String beginString) {
        this.beginString = beginString;
    }

    public long getHeartBtInt() {
        return heartBtInt;
    }

    public void setHeartBtInt(long heartBtInt) {
        this.heartBtInt = heartBtInt;
    }

    public String getSenderCompID() {
        return senderCompID;
    }

    public void setSenderCompID(String senderCompID) {
        this.senderCompID = senderCompID;
    }

    public String getTargetCompID() {
        return targetCompID;
    }

    public void setTargetCompID(String targetCompID) {
        this.targetCompID = targetCompID;
    }

    public String getDefaultApplVerID() {
        return defaultApplVerID;
    }

    public void setDefaultApplVerID(String defaultApplVerID) {
        this.defaultApplVerID = defaultApplVerID;
    }

    public String getSenderSubID() {
        return senderSubID;
    }

    public void setSenderSubID(String senderSubID) {
        this.senderSubID = senderSubID;
    }

    public String getEncryptMethod() {
        return encryptMethod;
    }

    public void setEncryptMethod(String encryptMethod) {
        this.encryptMethod = encryptMethod;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public Boolean getResetSeqNumFlag() { return resetSeqNumFlag; }

    public Boolean getResetOnLogon() { return resetOnLogon; }

    public int getTestRequestDelay() {
        return testRequestDelay;
    }

    public int getReconnectDelay() {
        return reconnectDelay;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setResetSeqNumFlag(Boolean resetSeqNumFlag) { this.resetSeqNumFlag = resetSeqNumFlag; }

    public void setResetOnLogon(Boolean resetOnLogon) { this.resetOnLogon = resetOnLogon; }

    public void setTestRequestDelay(int testRequestDelay) {
        this.testRequestDelay = testRequestDelay;
    }

    public void setReconnectDelay(int reconnectDelay) {
        this.reconnectDelay = reconnectDelay;
    }

    public int getDisconnectRequestDelay() {
        return disconnectRequestDelay;
    }

    public void setDisconnectRequestDelay(int disconnectRequestDelay) {
        this.disconnectRequestDelay = disconnectRequestDelay;
    }
}

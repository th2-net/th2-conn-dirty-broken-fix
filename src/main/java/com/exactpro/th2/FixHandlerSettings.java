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

package com.exactpro.th2;

import com.exactpro.th2.conn.dirty.fix.brokenconn.configuration.BrokenConnConfiguration;
import com.exactpro.th2.conn.dirty.fix.KeyFileType;
import com.exactpro.th2.conn.dirty.tcp.core.api.IChannel.Security;
import com.exactpro.th2.conn.dirty.tcp.core.api.IHandlerSettings;
import com.exactpro.th2.util.DateTimeFormatterDeserializer;
import com.exactpro.th2.util.LocalTimeDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

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
    private String newPassword;
    private String previousPasswords;
    private String passwordEncryptKey;
    private KeyFileType passwordEncryptKeyFileType = KeyFileType.PEM_PUBLIC_KEY;
    /**
     * Value from Java Security Standard Algorithm Names
     */
    private String passwordKeyEncryptAlgorithm = "RSA";
    /**
     * Value from Java Security Standard Algorithm Names
     */
    private String passwordEncryptAlgorithm = "RSA";
    private Boolean resetSeqNumFlag = false;
    private Boolean resetOnLogon = false;
    private Boolean useNextExpectedSeqNum = false;
    private Boolean loadSequencesFromCradle = false;
    private Boolean loadMissedMessagesFromCradle = false;
    private Boolean resetStateOnServerReset = false;
    private Boolean logoutOnIncorrectServerSequence = false;

    @JsonDeserialize(using = LocalTimeDeserializer.class)
    private LocalTime sessionStartTime;

    @JsonDeserialize(using = LocalTimeDeserializer.class)
    private LocalTime sessionEndTime;

    private int rateLimit = Integer.MAX_VALUE;

    private int testRequestDelay = 60;
    private int reconnectDelay = 5;
    private int disconnectRequestDelay = 5;

    private BrokenConnConfiguration brokenConnConfiguration;

    @JsonDeserialize(using = DateTimeFormatterDeserializer.class)
    private DateTimeFormatter sendingDateTimeFormat = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss.SSSSSSSSS");

    public DateTimeFormatter getSendingDateTimeFormat() {
        return this.sendingDateTimeFormat;
    }

    public void setSendingDateTimeFormat(DateTimeFormatter sendingDateTimeFormat) {
        this.sendingDateTimeFormat = sendingDateTimeFormat;
    }

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

    public String getNewPassword() {
        return newPassword;
    }

    public String getPreviousPasswords() { return previousPasswords; }

    public String getPasswordEncryptKey() {return passwordEncryptKey;}

    public KeyFileType getPasswordEncryptKeyFileType() {
        return passwordEncryptKeyFileType;
    }

    public String getPasswordKeyEncryptAlgorithm() {
        return passwordKeyEncryptAlgorithm;
    }

    public String getPasswordEncryptAlgorithm() {
        return passwordEncryptAlgorithm;
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

    public void setNewPassword(String newPassword) {
        this.newPassword = newPassword;
    }

    public void setPreviousPasswords(String previousPasswords) { this.previousPasswords = previousPasswords; }

    public void setPasswordEncryptKey(String passwordEncryptKey) {this.passwordEncryptKey = passwordEncryptKey;}

    public void setPasswordEncryptKeyFileType(KeyFileType passwordEncryptKeyFileType) {
        this.passwordEncryptKeyFileType = passwordEncryptKeyFileType;
    }

    public void setPasswordKeyEncryptAlgorithm(String passwordKeyEncryptAlgorithm) {
        this.passwordKeyEncryptAlgorithm = passwordKeyEncryptAlgorithm;
    }

    public void setPasswordEncryptAlgorithm(String passwordEncryptAlgorithm) {
        this.passwordEncryptAlgorithm = passwordEncryptAlgorithm;
    }

    public Boolean isLoadSequencesFromCradle() {
        return loadSequencesFromCradle;
    }

    public void setLoadSequencesFromCradle(Boolean loadSequencesFromCradle) {
        this.loadSequencesFromCradle = loadSequencesFromCradle;
    }

    public Boolean isLoadMissedMessagesFromCradle() {
        return loadMissedMessagesFromCradle;
    }

    public void setLoadMissedMessagesFromCradle(Boolean loadMissedMessagesFromCradle) {
        this.loadMissedMessagesFromCradle = loadMissedMessagesFromCradle;
    }

    public Boolean getResetStateOnServerReset() {
        return resetStateOnServerReset;
    }

    public void setResetStateOnServerReset(Boolean resetStateOnServerReset) {
        this.resetStateOnServerReset = resetStateOnServerReset;
    }

    public Boolean useNextExpectedSeqNum() {
        return useNextExpectedSeqNum;
    }

    public void setUseNextExpectedSeqNum(Boolean useNextExpectedSeqNum) {
        this.useNextExpectedSeqNum = useNextExpectedSeqNum;
    }

    public Boolean isLogoutOnIncorrectServerSequence() {
        return logoutOnIncorrectServerSequence;
    }

    public void setLogoutOnIncorrectServerSequence(Boolean logoutOnIncorrectServerSequence) {
        this.logoutOnIncorrectServerSequence = logoutOnIncorrectServerSequence;
    }

    public LocalTime getSessionStartTime() {
        return sessionStartTime;
    }

    public void setSessionStartTime(LocalTime sessionStartTime) {
        this.sessionStartTime = sessionStartTime;
    }

    public LocalTime getSessionEndTime() {
        return sessionEndTime;
    }

    public void setSessionEndTime(LocalTime sessionEndTime) {
        this.sessionEndTime = sessionEndTime;
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

    public BrokenConnConfiguration getBrokenConnConfiguration() {
        return brokenConnConfiguration;
    }

    public void setBrokenConnConfiguration(BrokenConnConfiguration brokenConnConfiguration) {
        this.brokenConnConfiguration = brokenConnConfiguration;
    }

    public int getRateLimit() {
        return rateLimit;
    }

    public void setRateLimit(int rateLimit) {
        this.rateLimit = rateLimit;
    }
}

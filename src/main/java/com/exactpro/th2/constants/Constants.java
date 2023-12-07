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

package com.exactpro.th2.constants;


import java.util.Collections;
import java.util.Set;

public class Constants {

    public static final String SOH = "\001";

    //Tags
    public static final Integer BEGIN_STRING_TAG = 8;
    public static final Integer BODY_LENGTH_TAG = 9;
    public static final Integer MSG_TYPE_TAG = 35;
    public static final Integer SENDER_COMP_ID_TAG = 49;
    public static final Integer TARGET_COMP_ID_TAG = 56;
    public static final Integer MSG_SEQ_NUM_TAG = 34;
    public static final Integer SENDING_TIME_TAG = 52;
    public static final Integer ORIG_SENDING_TIME_TAG = 122;
    public static final Integer CHECKSUM_TAG = 10;
    public static final Integer DEFAULT_APPL_VER_ID_TAG = 1137;
    public static final Integer SENDER_SUB_ID_TAG = 50;
    public static final Integer ENCRYPT_METHOD_TAG = 98;
    public static final Integer HEART_BT_INT_TAG = 108;
    public static final Integer USERNAME_TAG = 553;
    public static final Integer PASSWORD_TAG = 554;
    public static final Integer NEW_PASSWORD_TAG = 925;
    public static final Integer ENCRYPTED_PASSWORD_TAG = 1402;
    public static final Integer NEW_ENCRYPTED_PASSWORD_TAG = 1404;
    public static final Integer SESSION_STATUS_TAG = 1409;
    public static final Integer TEST_REQ_ID_TAG = 112;
    public static final Integer BEGIN_SEQ_NO_TAG = 7;
    public static final Integer END_SEQ_NO_TAG = 16;
    public static final Integer NEW_SEQ_NO_TAG = 36;
    public static final Integer GAP_FILL_FLAG_TAG = 123;
    public static final Integer TEXT_TAG = 58;
    public static final Integer RESET_SEQ_NUM_TAG = 141;
    public static final Integer NEXT_EXPECTED_SEQ_NUMBER_TAG = 789;
    public static final Integer POSS_DUP_TAG = 43;
    public static final Integer POSS_RESEND_TAG = 97;

    //Fields
    public static final String BEGIN_STRING = SOH + BEGIN_STRING_TAG + "=";
    public static final String BODY_LENGTH = SOH + BODY_LENGTH_TAG + "=";
    public static final String MSG_TYPE = SOH + MSG_TYPE_TAG + "=";
    public static final String SENDER_COMP_ID = SOH + SENDER_COMP_ID_TAG + "=";
    public static final String TARGET_COMP_ID = SOH + TARGET_COMP_ID_TAG + "=";
    public static final String MSG_SEQ_NUM = SOH + MSG_SEQ_NUM_TAG + "=";
    public static final String SENDING_TIME = SOH + SENDING_TIME_TAG + "=";
    public static final String ORIG_SENDING_TIME = SOH + ORIG_SENDING_TIME_TAG + "=";
    public static final String CHECKSUM = SOH + CHECKSUM_TAG + "=";
    public static final String ENCRYPT_METHOD = SOH + ENCRYPT_METHOD_TAG + "=";
    public static final String HEART_BT_INT = SOH + HEART_BT_INT_TAG + "=";
    public static final String USERNAME = SOH + USERNAME_TAG + "=";
    public static final String PASSWORD = SOH + PASSWORD_TAG + "=";
    public static final String NEW_PASSWORD = SOH + NEW_PASSWORD_TAG + "=";
    public static final String ENCRYPTED_PASSWORD = SOH + ENCRYPTED_PASSWORD_TAG + "=";
    public static final String NEW_ENCRYPTED_PASSWORD = SOH + NEW_ENCRYPTED_PASSWORD_TAG + "=";
    public static final String TEST_REQ_ID = SOH + TEST_REQ_ID_TAG + "=";
    public static final String BEGIN_SEQ_NO = SOH + BEGIN_SEQ_NO_TAG + "=";
    public static final String END_SEQ_NO = SOH + END_SEQ_NO_TAG + "=";
    public static final String NEW_SEQ_NO = SOH + NEW_SEQ_NO_TAG + "=";
    public static final String GAP_FILL_FLAG = SOH + GAP_FILL_FLAG_TAG + "=";
    public static final String DEFAULT_APPL_VER_ID = SOH + DEFAULT_APPL_VER_ID_TAG + "=";
    public static final String SENDER_SUB_ID = SOH + SENDER_SUB_ID_TAG + "=";
    public static final String RESET_SEQ_NUM = SOH + RESET_SEQ_NUM_TAG + "=";
    public static final String NEXT_EXPECTED_SEQ_NUM = SOH + NEXT_EXPECTED_SEQ_NUMBER_TAG + "=";
    public static final String POSS_DUP = SOH + POSS_DUP_TAG + "=";
    public static final String POSS_RESEND = SOH + POSS_RESEND_TAG + "=";
    public static final String TEXT = SOH + TEXT_TAG + "=";

    //message types
    public static final String MSG_TYPE_LOGON = "A";
    public static final String MSG_TYPE_LOGOUT = "5";
    public static final String MSG_TYPE_HEARTBEAT = "0";
    public static final String MSG_TYPE_TEST_REQUEST = "1";
    public static final String MSG_TYPE_RESEND_REQUEST = "2";
    public static final String MSG_TYPE_SEQUENCE_RESET = "4";

    public static final Set<String> ADMIN_MESSAGES = Collections.unmodifiableSet(
            Set.of(
                    MSG_TYPE_LOGON, MSG_TYPE_LOGOUT,
                    MSG_TYPE_HEARTBEAT, MSG_TYPE_RESEND_REQUEST,
                    MSG_TYPE_SEQUENCE_RESET, MSG_TYPE_TEST_REQUEST
            )
    );

    public static final String IS_POSS_DUP = "Y";
    public static final String IS_SEQUENCE_RESET_FLAG = "Y";
    public static final int SUCCESSFUL_LOGOUT_CODE = 4;
}

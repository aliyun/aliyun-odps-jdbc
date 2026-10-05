/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */

package com.aliyun.odps.jdbc;

import com.aliyun.odps.jdbc.utils.ConnectionResource;

import java.util.Properties;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The retry wait time is read like every other transport option, and it stays out of the way when
 * it is not given.
 *
 * <p>The SDK reuses the connect timeout as the interval between retried GET requests unless a retry
 * wait time is set explicitly. That made the two options one knob for consumers: a connect timeout
 * written in milliseconds -- which is what this driver's README used to say -- turned into seconds
 * of waiting per retry. {@code retryWaitTime} gives the back-off its own knob without changing the
 * default, so these cases hold the option plumbing in place: absent means "the SDK decides",
 * present means "the value reaches the REST client".
 *
 * <p>Offline on purpose. Reading an option needs no service, and a case that needs one cannot say
 * whether a timeout was honoured without waiting for the very delay it is trying to measure. The
 * measured delay before and after this option is in the release notes and the PR description.
 */
class RetryWaitTimeOptionTest {

  private static final String BASE_URL =
      "jdbc:odps:http://127.0.0.1:1/api?project=retry_wait_time_project"
      + "&accessId=retry_wait_time_access_id&accessKey=retry_wait_time_access_key";

  @Test
  void absentOptionStaysUnsetSoTheSdkDecides() {
    ConnectionResource cr = new ConnectionResource(BASE_URL, null);

    // -1 is the SDK's own "not set" marker: it then falls back to the connect timeout.
    assertEquals(-1, cr.getRetryWaitTime());
  }

  @Test
  void urlKeySetsTheRetryWaitTime() {
    ConnectionResource cr = new ConnectionResource(BASE_URL + "&retryWaitTime=2", null);

    assertEquals(2, cr.getRetryWaitTime());
  }

  @Test
  void propertyKeySetsTheRetryWaitTime() {
    Properties info = new Properties();
    info.setProperty("retry_wait_time", "3");

    assertEquals(3, new ConnectionResource(BASE_URL, info).getRetryWaitTime());
  }

  @Test
  void retryWaitTimeIsIndependentOfTheConnectTimeout() {
    ConnectionResource cr = new ConnectionResource(
        BASE_URL + "&connectTimeout=2000&readTimeout=5000&retryWaitTime=1", null);

    assertEquals("2000", cr.getConnectTimeout());
    assertEquals("5000", cr.getReadTimeout());
    assertEquals(1, cr.getRetryWaitTime());
  }

  @Test
  void nonIntegerRetryWaitTimeIsRejectedWhileTheUrlIsRead() {
    NumberFormatException e = assertThrows(NumberFormatException.class, () ->
        new ConnectionResource(BASE_URL + "&retryWaitTime=soon", null));

    // The value, not the option name: this is the raw NumberFormatException that every numeric
    // option in this class still throws. A sibling change makes the message name the option too.
    assertTrue(e.getMessage().contains("soon"), e.getMessage());
  }
}

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

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class ConnectionResourceTest {

  @Test
  public void connectionURLMinimumTest() {

    ConnectionResource cr = new ConnectionResource("jdbc:odps:haha?project=xixi", null);
    Assert.assertEquals("haha", cr.getEndpoint());
    Assert.assertEquals("xixi", cr.getProject());
    Assert.assertEquals(null, cr.getAccessId());
    Assert.assertEquals(null, cr.getAccessKey());
    Assert.assertEquals(null, cr.getLogview());
    Assert.assertEquals("UTF-8", cr.getCharset());
    Assert.assertEquals("3", cr.getLifecycle());
  }

  @Test
  public void connectionURLFullTest() {

    ConnectionResource cr = new ConnectionResource("jdbc:odps:haha?project=xixi&accessId=idid&accessKey=keykey&"
                                                   + "logview=loglog&charset=setset&lifecycle=5&loglevel=FATAL", null);
    Assert.assertEquals("haha", cr.getEndpoint());
    Assert.assertEquals("xixi", cr.getProject());
    Assert.assertEquals("idid", cr.getAccessId());
    Assert.assertEquals("keykey", cr.getAccessKey());
    Assert.assertEquals("loglog", cr.getLogview());
    Assert.assertEquals("setset", cr.getCharset());
    Assert.assertEquals("5", cr.getLifecycle());
    Assert.assertEquals("FATAL", cr.getLogLevel());
  }

  @Test
  public void connectionInfoFullTest() {

    Properties info = new Properties();
    info.put("access_id", "idid");
    info.put("access_key", "keykey");
    info.put("logview_host", "loglog");
    info.put("charset", "setset");
    info.put("lifecycle", "5");
    info.put("log_level", "FATAL");

    ConnectionResource cr = new ConnectionResource("jdbc:odps:haha?project=xixi", info);

    Assert.assertEquals("haha", cr.getEndpoint());
    Assert.assertEquals("xixi", cr.getProject());
    Assert.assertEquals("idid", cr.getAccessId());
    Assert.assertEquals("keykey", cr.getAccessKey());
    Assert.assertEquals("loglog", cr.getLogview());
    Assert.assertEquals("setset", cr.getCharset());
    Assert.assertEquals("5", cr.getLifecycle());
    Assert.assertEquals("FATAL", cr.getLogLevel());
  }

  @Test
  public void connectionInfoOverrideTest() {

    Properties info = new Properties();
    info.put("access_id", "id");
    info.put("access_key", "key");
    info.put("logview_host", "log");
    info.put("charset", "set");
    info.put("lifecycle", "100");
    info.put("log_level", "FATAL");

    ConnectionResource cr = new ConnectionResource("jdbc:odps:haha?project=xixi&accessId=idid&accessKey=keykey&"
                                                   + "logview=loglog&charset=setset&lifecycle=5&loglevel=INFO", info);
    Assert.assertEquals("haha", cr.getEndpoint());
    Assert.assertEquals("xixi", cr.getProject());
    Assert.assertEquals("id", cr.getAccessId());
    Assert.assertEquals("key", cr.getAccessKey());
    Assert.assertEquals("log", cr.getLogview());
    Assert.assertEquals("set", cr.getCharset());
    Assert.assertEquals("100", cr.getLifecycle());
    Assert.assertEquals("FATAL", cr.getLogLevel());
  }
}

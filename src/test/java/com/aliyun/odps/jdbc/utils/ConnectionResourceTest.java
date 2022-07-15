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

package com.aliyun.odps.jdbc.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

public class ConnectionResourceTest {

  String odpsConfigFile =
      getClass().getClassLoader().getResource("odps_config.ini").getPath();

  @Test
  public void connectionURLMinimumTest() {

    ConnectionResource cr = new ConnectionResource("jdbc:odps:haha?project=xixi", null);
    Assert.assertEquals("haha", cr.getEndpoint());
    Assert.assertEquals("xixi", cr.getProject());
    Assert.assertEquals(null, cr.getAccessId());
    Assert.assertEquals(null, cr.getAccessKey());
    Assert.assertEquals(null, cr.getLogview());
    Assert.assertEquals("UTF-8", cr.getCharset());
    Assert.assertEquals("-1", cr.getConnectTimeout());
    Assert.assertEquals("-1", cr.getReadTimeout());
  }

  @Test
  public void connectionURLFullTest() {

    ConnectionResource
        cr =
        new ConnectionResource("jdbc:odps:haha?project=xixi&accessId=idid&accessKey=keykey&"
                               + "logview=loglog&charset=setset&lifecycle=5&loglevel=FATAL&readTimeout=20&connectTimeout=30", null);
    Assert.assertEquals("haha", cr.getEndpoint());
    Assert.assertEquals("xixi", cr.getProject());
    Assert.assertEquals("idid", cr.getAccessId());
    Assert.assertEquals("keykey", cr.getAccessKey());
    Assert.assertEquals("loglog", cr.getLogview());
    Assert.assertEquals("setset", cr.getCharset());
    Assert.assertEquals("20", cr.getReadTimeout());
    Assert.assertEquals("30", cr.getConnectTimeout());
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
    info.put("settings", "{\"enable_limit\":\"True\"}");
    info.put("read_timeout", "20");

    ConnectionResource cr = new ConnectionResource("jdbc:odps:haha?project=xixi", info);

    Assert.assertEquals("haha", cr.getEndpoint());
    Assert.assertEquals("xixi", cr.getProject());
    Assert.assertEquals("idid", cr.getAccessId());
    Assert.assertEquals("keykey", cr.getAccessKey());
    Assert.assertEquals("loglog", cr.getLogview());
    Assert.assertEquals("setset", cr.getCharset());
    Assert.assertEquals("20", cr.getReadTimeout());
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
    info.put("connect_timeout", "300");

    ConnectionResource
        cr =
        new ConnectionResource("jdbc:odps:haha?project=xixi&accessId=idid&accessKey=keykey&"
                               + "logview=loglog&charset=setset&lifecycle=5&loglevel=INFO&readTimeout=20&connectTimeout=30", info);
    Assert.assertEquals("haha", cr.getEndpoint());
    Assert.assertEquals("xixi", cr.getProject());
    Assert.assertEquals("id", cr.getAccessId());
    Assert.assertEquals("key", cr.getAccessKey());
    Assert.assertEquals("log", cr.getLogview());
    Assert.assertEquals("set", cr.getCharset());
    Assert.assertEquals("20", cr.getReadTimeout());
    Assert.assertEquals("300", cr.getConnectTimeout());
  }

  @Test
  public void connectionURLWithOdpsConfig() {
    String url1 =
        "jdbc:odps:http://1.1.1.1:8100/api?project=p1&loglevel=debug&accessId=123&accessKey=234%3D"
        + "&tunnel_endpoint=http%3A%2F%2F1.1.1.1%3A8066&logConfFile=/Users/emerson/logback.xml"
        + "&odps_config=" + odpsConfigFile;

    ConnectionResource resource = new ConnectionResource(url1, null);
    Assert.assertEquals("http://1.1.1.1:8100/api", resource.getEndpoint());
    Assert.assertEquals("p2", resource.getProject());
    Assert.assertEquals("345", resource.getAccessId());
    Assert.assertEquals("456=", resource.getAccessKey());
    Assert.assertEquals("UTF-8", resource.getCharset());
    Assert.assertEquals("logback1.xml", resource.getLogConfFile());
    Assert.assertEquals(null, resource.getLogview());
    Assert.assertEquals("http://1.1.1.1:8066", resource.getTunnelEndpoint());

    Assert.assertEquals(false, resource.isInteractiveMode());
    Assert.assertEquals("sn", resource.getInteractiveServiceName());
    Assert.assertEquals("default1", resource.getMajorVersion());
    Map<String, Map<String, List<String>>> tables = resource.getTables();
    Assert.assertEquals(2, tables.size());
    Assert.assertTrue(tables.containsKey("project1"));
    Assert.assertEquals(2, tables.get("project1").size());
    Assert.assertTrue(tables.get("project1").get("project1").contains("table1"));
    Assert.assertTrue(tables.get("project1").get("project1").contains("table2"));
    Assert.assertTrue(tables.containsKey("project2"));
    Assert.assertEquals(3, tables.get("project2").size());
    Assert.assertTrue(tables.get("project2").get("project2").contains("table1"));
    Assert.assertTrue(tables.get("project2").get("project2").contains("table2"));
    Assert.assertTrue(tables.get("project2").get("project2").contains("table3"));
    Assert.assertEquals("21", resource.getReadTimeout());
  }

  @Test
  public void testLogConfigFile() throws IOException {
    String logConfigFile = getClass().getClassLoader().getResource("logback.xml").getPath();
    String url2 =
        "jdbc:odps:http://1.1.1.1:8100/api?project=p1&loglevel=debug&accessId=123" +
        "&accessKey=234%3D&logview_host=http://abc.com:8080" +
        "&tunnelEndpoint=http://1.1.1.1:8066&interactiveMode=true" +
        "&interactiveServiceName=sn&interactiveTimeout=11" +
        "&tableList=project1.table1,project1.table2,project2.table1,project2.table2,project2.table3"
        +
        "&majorVersion=default1&logConfFile="
        + logConfigFile;
    ConnectionResource resource = new ConnectionResource(url2, null);
    Assert.assertEquals(true, resource.isInteractiveMode());
    Assert.assertEquals("http://1.1.1.1:8100/api", resource.getEndpoint());
    Assert.assertEquals("p1", resource.getProject());
    Assert.assertEquals("123", resource.getAccessId());
    Assert.assertEquals("234=", resource.getAccessKey());
    Assert.assertEquals(logConfigFile, resource.getLogConfFile());
    Assert.assertEquals("UTF-8", resource.getCharset());
    Assert.assertEquals(logConfigFile, resource.getLogConfFile());
    Assert.assertEquals("http://abc.com:8080", resource.getLogview());
    Assert.assertEquals("http://1.1.1.1:8066", resource.getTunnelEndpoint());
    // Map<String, List<String>> tables = resource.getTables();
    // Assert.assertEquals(2, tables.size());
    // Assert.assertTrue(tables.containsKey("project1"));
    // Assert.assertEquals(2, tables.get("project1").size());
    // Assert.assertTrue(tables.get("project1").contains("table1"));
    // Assert.assertTrue(tables.get("project1").contains("table2"));
    // Assert.assertTrue(tables.containsKey("project2"));
    // Assert.assertEquals(3, tables.get("project2").size());
    // Assert.assertTrue(tables.get("project2").contains("table1"));
    // Assert.assertTrue(tables.get("project2").contains("table2"));
    // Assert.assertTrue(tables.get("project2").contains("table3"));
    Properties info = new Properties();
    info.load(new FileInputStream(odpsConfigFile));
    resource = new ConnectionResource(url2, info);
    Assert.assertEquals("http://1.1.1.1:8100/api", resource.getEndpoint());
    Assert.assertEquals("p2", resource.getProject());
    Assert.assertEquals("345", resource.getAccessId());
    Assert.assertEquals("456=", resource.getAccessKey());
    Assert.assertEquals("UTF-8", resource.getCharset());
    Assert.assertEquals("logback1.xml", resource.getLogConfFile());
    Assert.assertEquals("http://abc.com:8080", resource.getLogview());
    Assert.assertEquals("http://1.1.1.1:8066", resource.getTunnelEndpoint());
    Assert.assertEquals("sn", resource.getInteractiveServiceName());
    Assert.assertEquals("default1", resource.getMajorVersion());
    // tables = resource.getTables();
    // Assert.assertEquals(2, tables.size());
    // Assert.assertTrue(tables.containsKey("project1"));
    // Assert.assertEquals(2, tables.get("project1").size());
    // Assert.assertTrue(tables.get("project1").contains("table1"));
    // Assert.assertTrue(tables.get("project1").contains("table2"));
    // Assert.assertTrue(tables.containsKey("project2"));
    // Assert.assertEquals(3, tables.get("project2").size());
    // Assert.assertTrue(tables.get("project2").contains("table1"));
    // Assert.assertTrue(tables.get("project2").contains("table2"));
    // Assert.assertTrue(tables.get("project2").contains("table3"));
  }

  @Test
  public void testTables() throws IOException {
    String url1 =
        "jdbc:odps:http://1.1.1.1:8100/api?project=p1&accessId=123&accessKey=234%3D"
        + "&logConfFile=/Users/emerson/logback.xml";
    String url2 = url1 + "&tableList=project1.table1,project1.table2,project2.table1,project2.table2,project2.table3";
    ConnectionResource resource = new ConnectionResource(url2, null);
    Map<String, Map<String, List<String>>> tables = resource.getTables();
    Assert.assertTrue(tables.get("project1").get(null).contains("table1"));
    Assert.assertEquals(2, tables.get("project1").get(null).size());

    String url3 = url1 + "&tableList=p1.s1.t1,p1.s1.t2,p2.s2.t1,p2.s2.t2,p2.s2.t3"
                  + "&odpsNamespaceSchema=true&schema=s1";
    resource = new ConnectionResource(url3, null);
    tables = resource.getTables();
    Assert.assertTrue(tables.get("p1").get("s1").contains("t2"));
    Assert.assertEquals(3, tables.get("p2").get("s2").size());

  }
}
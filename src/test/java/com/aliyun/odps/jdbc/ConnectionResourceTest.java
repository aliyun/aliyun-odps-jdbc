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

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;

import com.aliyun.odps.jdbc.utils.ConnectionResource;

public class ConnectionResourceTest {

  @Test
  public void testConnectionResourceTest() throws Exception {
    String odpsConfigFile = getClass().getClassLoader().getResource("odps_config.ini").getPath();
    String url1 =
        "jdbc:odps:http://1.1.1.1:8100/api?project=p1&loglevel=debug&accessId=123&accessKey=234%3D" +
            "&tunnel_endpoint=http%3A%2F%2F1.1.1.1%3A8066&logconffile=/Users/emerson/logback.xml" +
            "&odps_config=" + odpsConfigFile;
    ConnectionResource resource = new ConnectionResource(url1, null);
    Assert.assertEquals("http://1.1.1.1:8100/api", resource.getEndpoint());
    Assert.assertEquals("p2", resource.getProject());
    Assert.assertEquals("345", resource.getAccessId());
    Assert.assertEquals("456=", resource.getAccessKey());
    Assert.assertEquals(null, resource.getLogLevel());
    Assert.assertEquals("2", resource.getLifecycle());
    Assert.assertEquals("UTF-8", resource.getCharset());
    Assert.assertEquals("logback1.xml", resource.getLogConfFile());
    Assert.assertEquals(null, resource.getLogview());
    Assert.assertEquals("http://1.1.1.1:8066", resource.getTunnelEndpoint());
    Assert.assertEquals(false, resource.isInteractiveMode());
    Assert.assertEquals("sn", resource.getInteractiveServiceName());
    Assert.assertEquals("default1", resource.getMajorVersion());
    List<String> tableList = resource.getTableList();
    Assert.assertEquals(2, tableList.size());
    Assert.assertEquals("table1", tableList.get(0));
    Assert.assertEquals("table2", tableList.get(1));

    String logConfigFile = getClass().getClassLoader().getResource("logback.xml").getPath();
    String url2 =
        "jdbc:odps:http://1.1.1.1:8100/api?project=p1&loglevel=debug&accessId=123" +
            "&accessKey=234%3D&logview_host=http://abc.com:8080" +
            "&tunnelEndpoint=http://1.1.1.1:8066&interactiveMode=true" +
            "&interactiveServiceName=sn&interactiveTimeout=11" +
            "&table_list=table1,table2" +
            "&majorVersion=default1&logconffile="
            + logConfigFile;
    resource = new ConnectionResource(url2, null);
    Assert.assertEquals(true, resource.isInteractiveMode());
    Assert.assertEquals("http://1.1.1.1:8100/api", resource.getEndpoint());
    Assert.assertEquals("p1", resource.getProject());
    Assert.assertEquals("123", resource.getAccessId());
    Assert.assertEquals("234=", resource.getAccessKey());
    Assert.assertEquals(logConfigFile, resource.getLogConfFile());
    Assert.assertEquals("debug", resource.getLogLevel());
    Assert.assertEquals("3", resource.getLifecycle());
    Assert.assertEquals("UTF-8", resource.getCharset());
    Assert.assertEquals(logConfigFile, resource.getLogConfFile());
    Assert.assertEquals("http://abc.com:8080", resource.getLogview());
    Assert.assertEquals("http://1.1.1.1:8066", resource.getTunnelEndpoint());
    tableList = resource.getTableList();
    Assert.assertEquals(2, tableList.size());
    Assert.assertEquals("table1", tableList.get(0));
    Assert.assertEquals("table2", tableList.get(1));
    Properties info = new Properties();
    info.load(new FileInputStream(odpsConfigFile));
    resource = new ConnectionResource(url2, info);
    Assert.assertEquals("http://1.1.1.1:8100/api", resource.getEndpoint());
    Assert.assertEquals("p2", resource.getProject());
    Assert.assertEquals("345", resource.getAccessId());
    Assert.assertEquals("456=", resource.getAccessKey());
    Assert.assertEquals("debug", resource.getLogLevel());
    Assert.assertEquals("2", resource.getLifecycle());
    Assert.assertEquals("UTF-8", resource.getCharset());
    Assert.assertEquals("logback1.xml", resource.getLogConfFile());
    Assert.assertEquals("http://abc.com:8080", resource.getLogview());
    Assert.assertEquals("http://1.1.1.1:8066", resource.getTunnelEndpoint());
    Assert.assertEquals("sn", resource.getInteractiveServiceName());
    Assert.assertEquals("default1", resource.getMajorVersion());
    tableList = resource.getTableList();
    Assert.assertEquals(2, tableList.size());
    Assert.assertEquals("table1", tableList.get(0));
    Assert.assertEquals("table2", tableList.get(1));
  }


}

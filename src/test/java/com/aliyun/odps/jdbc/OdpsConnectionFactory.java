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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.junit.Assert;

/**
 * This class manage a global JDBC connection and multiple testing instances
 * can access it simultaneously. It will also close the connection automatically.
 */
public class OdpsConnectionFactory {

  public Connection conn;

  private static final OdpsConnectionFactory cf = new OdpsConnectionFactory();

  private OdpsConnectionFactory() {
    try {
      Properties odpsConfig = new Properties();

      InputStream is =
          Thread.currentThread().getContextClassLoader().getResourceAsStream("bvt_conf.properties");
      odpsConfig.load(is);

      Class.forName("com.aliyun.odps.jdbc.OdpsDriver");
      String url = odpsConfig.getProperty("end_point");
      conn = DriverManager.getConnection("jdbc:odps:" + url, odpsConfig);
      Assert.assertNotNull(conn);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (java.sql.SQLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static OdpsConnectionFactory getInstance() {
    return cf;
  }
}


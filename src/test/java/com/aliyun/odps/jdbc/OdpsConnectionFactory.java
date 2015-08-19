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
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
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
      org.apache.log4j.BasicConfigurator.configure();

      Properties odpsConfig = new Properties();

      InputStream is =
          Thread.currentThread().getContextClassLoader().getResourceAsStream("bvt_conf.properties");
      odpsConfig.load(is);

      Class.forName("com.aliyun.odps.jdbc.OdpsDriver");

      String host =  odpsConfig.getProperty("end_point");
      String project = odpsConfig.getProperty("project_name");
      String username = odpsConfig.getProperty("access_id");
      String password = odpsConfig.getProperty("access_key");

      String url = "jdbc:odps:" + host;
      String url_with_project = "jdbc:odps:" + host + "@" + project;

      // pass project name via url
      //conn = DriverManager.getConnection(url_with_project, odpsConfig);
      conn = DriverManager.getConnection(url_with_project, username, password);

      Assert.assertNotNull(conn);
      Assert.assertEquals(odpsConfig.getProperty("end_point"), conn.getCatalog());
      Assert.assertEquals(odpsConfig.getProperty("project_name"), conn.getSchema());

      // Print info
      Driver driver = DriverManager.getDriver(url);
      DriverPropertyInfo[] dpi = driver.getPropertyInfo(url, odpsConfig);
      for (int i = 0; i < dpi.length; i++) {
        System.out.printf("%s\t%s\t%s\t%s\n", dpi[i].name, dpi[i].required, dpi[i].description,
                          dpi[i].value);
      }

      // change to funny names
      conn.setCatalog("xixi");
      conn.setSchema("haha");
      System.out.printf("change to %s:%s\n", conn.getCatalog(), conn.getSchema());

      // change back
      conn.setCatalog(odpsConfig.getProperty("end_point"));
      conn.setSchema(odpsConfig.getProperty("project_name"));
      System.out.printf("change to %s:%s\n", conn.getCatalog(), conn.getSchema());

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


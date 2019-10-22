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
import java.sql.Statement;
import java.util.Properties;

import org.junit.Assert;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;

/**
 * This class manage a global JDBC connection and multiple testing instances
 * can access it simultaneously. It will also close the connection automatically.
 */
public class TestManager {

  public Connection conn;
  public Connection sessionConn;
  public Odps odps;
  public TableTunnel tunnel;

  private static final TestManager cf = new TestManager();

  protected void finalize() throws Throwable{
    if (sessionConn != null) {
      sessionConn.close();
    }
  }

  private TestManager() {
    try {
      Properties odpsConfig = new Properties();

      InputStream is =
          Thread.currentThread().getContextClassLoader().getResourceAsStream("conf.properties");
      odpsConfig.load(is);

      Class.forName("com.aliyun.odps.jdbc.OdpsDriver");

      String endpoint = odpsConfig.getProperty("end_point");
      String project = odpsConfig.getProperty("project_name");
      String username = odpsConfig.getProperty("access_id");
      String password = odpsConfig.getProperty("access_key");
      String loglevel = odpsConfig.getProperty("log_level");
      String logview = odpsConfig.getProperty("logview_host");
      String url = String.format("jdbc:odps:%s?project=%s&loglevel=%s&logview=%s", endpoint, project, loglevel, logview);

      // pass project name via url
      conn = DriverManager.getConnection(url, username, password);
      Assert.assertNotNull(conn);
      Statement stmt = conn.createStatement();
      stmt.execute("set odps.sql.hive.compatible=true;");
      stmt.execute("set odps.sql.preparse.odps2=lot;");
      stmt.execute("set odps.sql.planner.mode=lot;");
      stmt.execute("set odps.sql.planner.parser.odps2=true;");
      stmt.execute("set odps.sql.ddl.odps2=true;");
      stmt.execute("set odps.sql.runtime.mode=executionengine;");
      stmt.execute("set odps.compiler.verify=true;");
      stmt.execute("set odps.compiler.output.format=lot,pot;");

      String sessionName = odpsConfig.getProperty("session_name");
      String urlSession = String.format("jdbc:odps:%s?project=%s&loglevel=%s&logview=%s&sessionName=%s", endpoint, project, loglevel, logview, sessionName);

      // pass project name via url
      sessionConn = DriverManager.getConnection(urlSession, username, password);
      Assert.assertNotNull(sessionConn);
      Statement sessionConnStatement = sessionConn.createStatement();
      sessionConnStatement.execute("set odps.sql.hive.compatible=true;");
      sessionConnStatement.execute("set odps.sql.preparse.odps2=lot;");
      sessionConnStatement.execute("set odps.sql.planner.mode=lot;");
      sessionConnStatement.execute("set odps.sql.planner.parser.odps2=true;");
      sessionConnStatement.execute("set odps.sql.ddl.odps2=true;");
      sessionConnStatement.execute("set odps.sql.runtime.mode=executionengine;");
      sessionConnStatement.execute("set odps.compiler.verify=true;");
      sessionConnStatement.execute("set odps.compiler.output.format=lot,pot;");

      Account account = new AliyunAccount(username, password);
      odps = new Odps(account);
      odps.setEndpoint(endpoint);
      odps.setDefaultProject(project);
      Assert.assertNotNull(odps);

      tunnel = new TableTunnel(odps);
      Assert.assertNotNull(tunnel);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (java.sql.SQLException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static TestManager getInstance() {
    return cf;
  }
}

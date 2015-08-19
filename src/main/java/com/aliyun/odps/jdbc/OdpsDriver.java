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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class OdpsDriver implements Driver {

  static {
    try {
      DriverManager.registerDriver(new OdpsDriver());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  /**
   * Is this driver JDBC compliant?
   */
  private static final boolean JDBC_COMPLIANT = false;

  private static final String END_POINT_KEY = "end_point";
  private static final String ACCESS_ID_KEY = "access_id";
  private static final String ACCESS_KEY_KEY = "access_key";
  private static final String PROJECT_NAME_KEY = "project_name";


  public final static String URL_PREFIX = "jdbc:odps:";

  public OdpsDriver() {
    SecurityManager security = System.getSecurityManager();
    if (security != null) {
      security.checkWrite("odps");
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return acceptsURL(url) ? new OdpsConnection(url.substring(URL_PREFIX.length()), info) : null;
  }

  // TODO: the check is very loose.
  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return (url != null) && url.startsWith(URL_PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException {

    if (info == null) {
      info = new Properties();
    }

    if (url != null && url.startsWith(URL_PREFIX)) {
      info.put(END_POINT_KEY, url.substring(URL_PREFIX.length()));
    }

    DriverPropertyInfo
        endPointProp =
        new DriverPropertyInfo(END_POINT_KEY, info.getProperty(END_POINT_KEY));
    endPointProp.required = false;
    endPointProp.description = "ODPS end point";

    // Fetch from info
    DriverPropertyInfo
        accessIdProp =
        new DriverPropertyInfo(ACCESS_ID_KEY, info.getProperty(ACCESS_ID_KEY, ""));
    accessIdProp.required = false;
    accessIdProp.description = "ODPS access id";

    DriverPropertyInfo
        accessKeyProp =
        new DriverPropertyInfo(ACCESS_KEY_KEY, info.getProperty(ACCESS_KEY_KEY, ""));
    accessKeyProp.required = false;
    accessKeyProp.description = "ODPS access key";

    DriverPropertyInfo
        projectNameProp =
        new DriverPropertyInfo(PROJECT_NAME_KEY, info.getProperty(PROJECT_NAME_KEY, ""));
    projectNameProp.required = false;
    projectNameProp.description = "ODPS project name";

    return new DriverPropertyInfo[] {endPointProp, accessIdProp, accessKeyProp, projectNameProp};
  }

  @Override
  public int getMajorVersion() {
    return 0;
  }

  @Override
  public int getMinorVersion() {
    return 1;
  }

  @Override
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }
}

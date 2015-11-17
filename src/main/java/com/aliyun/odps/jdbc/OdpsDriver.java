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

  public OdpsDriver() {
    SecurityManager security = System.getSecurityManager();
    if (security != null) {
      security.checkWrite("odps");
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    return acceptsURL(url) ? new OdpsConnection(url, info) : null;
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return ConnectionResource.acceptURL(url);
  }

  // each element is a DriverPropertyInfo object representing a connection URL attribute
  // that has not already been specified.
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException {

    ConnectionResource connRes = new ConnectionResource(url, info);

    DriverPropertyInfo
        accessIdProp =
        new DriverPropertyInfo(ConnectionResource.ACCESS_ID_PROP_KEY, connRes.getAccessId());
    accessIdProp.required = true;
    accessIdProp.description = "ODPS access id";

    DriverPropertyInfo
        accessKeyProp =
        new DriverPropertyInfo(ConnectionResource.ACCESS_KEY_PROP_KEY, connRes.getAccessKey());
    accessKeyProp.required = true;
    accessKeyProp.description = "ODPS access key";

    DriverPropertyInfo
        projectProp =
        new DriverPropertyInfo(ConnectionResource.PROJECT_PROP_KEY, connRes.getProject());
    projectProp.required = true;
    projectProp.description = "ODPS default project";

    DriverPropertyInfo
        charsetProp =
        new DriverPropertyInfo(ConnectionResource.CHARSET_PROP_KEY, connRes.getCharset());
    charsetProp.required = false;
    charsetProp.description = "character set for the string type";
    charsetProp.choices = new String[]{"UTF-8", "GBK"};

    DriverPropertyInfo
        logviewProp =
        new DriverPropertyInfo(ConnectionResource.LOGVIEW_HOST_PROP_KEY, connRes.getLogview());
    logviewProp.required = false;
    logviewProp.description = "logview host";

    return new DriverPropertyInfo[]{accessIdProp, accessKeyProp, projectProp, charsetProp};
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

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger("com.aliyun.odps.jdbc");
  }
}

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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ConnectionResource {

  private static final String CHARSET_DEFAULT_VALUE = "UTF-8";
  private static final String JDBC_ODPS_URL_PREFIX = "jdbc:odps:";

  /**
   * keys to retrieve properties from url.
   */
  private static final String ACCESS_ID_URL_KEY = "accessId";
  private static final String ACCESS_KEY_URL_KEY = "accessKey";
  private static final String PROJECT_URL_KEY = "defaultProject";
  private static final String CHARSET_URL_KEY = "charset";

  /**
   * Keys to retrieve properties from info.
   */
  public static final String ACCESS_ID_PROP_KEY = "access_id";
  public static final String ACCESS_KEY_PROP_KEY = "access_key";
  public static final String PROJECT_PROP_KEY = "project_name";
  public static final String CHARSET_PROP_KEY = "charset";
  public static final String LOGVIEW_HOST_PROP_KEY = "logview_host";

  // This is to support DriverManager.getConnection(url, user, password) API,
  // which put the 'user' and 'password' to the 'info'.
  // So the `access id` and `access_key` have aliases.
  private static final String ACCESS_ID_PROP_KEY_ALT = "user";
  private static final String ACCESS_KEY_PROP_KEY_ALT = "password";

  private String endpoint;
  private String accessId;
  private String accessKey;
  private String defaultProject;
  private String charset = CHARSET_DEFAULT_VALUE;
  private String logviewHost;

  public static boolean acceptURL(String url) {
    return (url != null) && url.startsWith(JDBC_ODPS_URL_PREFIX);
  }

  ConnectionResource(String url, Properties info) {

    // extract the params from the url
    Map<String, String> paramsInURL = new HashMap<String, String>();
    url = url.substring(JDBC_ODPS_URL_PREFIX.length());
    int atPos = url.indexOf("?");
    if (atPos == -1) {
      endpoint = url;
    } else {
      endpoint = url.substring(0, atPos);

      String query = url.substring(atPos + 1);
      String[] pairs = query.split("&");

      for (String pair : pairs) {
        String[] keyvalue = pair.split("=");
        paramsInURL.put(keyvalue[0], keyvalue[1]);
      }

      accessId = paramsInURL.get(ACCESS_ID_URL_KEY);
      accessKey = paramsInURL.get(ACCESS_KEY_URL_KEY);
      charset = paramsInURL.get(CHARSET_URL_KEY);
      defaultProject = paramsInURL.get(PROJECT_URL_KEY);
    }

    if (info == null) return;

    {
      String value = info.getProperty(ACCESS_ID_PROP_KEY);
      accessId = (value == null) ? accessId : value;

      String alt = info.getProperty(ACCESS_ID_PROP_KEY_ALT);
      accessId = (alt == null) ? accessId : alt;
    }

    {
      String value = info.getProperty(ACCESS_KEY_PROP_KEY);
      accessKey = (value == null) ? accessKey : value;

      String alt = info.getProperty(ACCESS_KEY_PROP_KEY_ALT);
      accessKey = (alt == null) ? accessKey : alt;
    }

    {
      String value = info.getProperty(PROJECT_PROP_KEY);
      defaultProject = (value == null) ? defaultProject : value;
    }

    {
      String value = info.getProperty(CHARSET_PROP_KEY);
      charset = (value == null) ? charset : value;
    }

    {
      // Logview host can only be get from props
      String value = info.getProperty(LOGVIEW_HOST_PROP_KEY);
      logviewHost = (value == null) ? logviewHost : value;
    }
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getDefaultProject() {
    return defaultProject;
  }

  public String getCharset() {
    return charset;
  }

  public String getAccessId() {
    return accessId;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getLogviewHost() {
    return logviewHost;
  }
}

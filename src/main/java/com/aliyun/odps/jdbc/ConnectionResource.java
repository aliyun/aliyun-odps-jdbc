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

  private static final String JDBC_ODPS_URL_PREFIX = "jdbc:odps:";
  private static final String CHARSET_DEFAULT_VALUE = "UTF-8";
  private static final String LIFECYCLE_DEFAULT_VALUE = "3";
  private static final String LOGLEVEL_DEFAULT_VALUE = "INFO";

  /**
   * keys to retrieve properties from url.
   */
  private static final String ACCESS_ID_URL_KEY = "accessId";
  private static final String ACCESS_KEY_URL_KEY = "accessKey";
  private static final String PROJECT_URL_KEY = "project";
  private static final String CHARSET_URL_KEY = "charset";
  private static final String LOGVIEW_URL_KEY = "logview";
  private static final String LIFECYCLE_URL_KEY = "lifecycle";
  private static final String LOGLEVEL_URL_KEY = "loglevel";

  /**
   * Keys to retrieve properties from info.
   *
   * public since they are accessed in getPropInfo()
   */
  public static final String ACCESS_ID_PROP_KEY = "access_id";
  public static final String ACCESS_KEY_PROP_KEY = "access_key";
  public static final String PROJECT_PROP_KEY = "project_name";
  public static final String CHARSET_PROP_KEY = "charset";
  public static final String LOGVIEW_HOST_PROP_KEY = "logview_host";
  public static final String LIFECYCLE_PROP_KEY = "lifecycle";
  public static final String LOGLEVEL_PROP_KEY = "log_level";

  // This is to support DriverManager.getConnection(url, user, password) API,
  // which put the 'user' and 'password' to the 'info'.
  // So the `access id` and `access_key` have aliases.
  private static final String ACCESS_ID_PROP_KEY_ALT = "user";
  private static final String ACCESS_KEY_PROP_KEY_ALT = "password";

  private String endpoint;
  private String accessId;
  private String accessKey;
  private String project;
  private String charset = CHARSET_DEFAULT_VALUE;
  private String logview;
  private String lifecycle = LIFECYCLE_DEFAULT_VALUE;
  private String logLevel = LOGLEVEL_DEFAULT_VALUE;

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

      if (paramsInURL.get(ACCESS_ID_URL_KEY) != null) {
        accessId = paramsInURL.get(ACCESS_ID_URL_KEY);
      }

      if (paramsInURL.get(ACCESS_KEY_URL_KEY) != null) {
        accessKey = paramsInURL.get(ACCESS_KEY_URL_KEY);
      }

      if (paramsInURL.get(CHARSET_URL_KEY) != null) {
        charset = paramsInURL.get(CHARSET_URL_KEY);
      }

      if (paramsInURL.get(PROJECT_URL_KEY) != null) {
        project = paramsInURL.get(PROJECT_URL_KEY);
      }

      if (paramsInURL.get(LOGVIEW_URL_KEY) != null) {
        logview = paramsInURL.get(LOGVIEW_URL_KEY);
      }

      if (paramsInURL.get(LIFECYCLE_URL_KEY) != null) {
        lifecycle = paramsInURL.get(LIFECYCLE_URL_KEY);
      }

      if (paramsInURL.get(LOGLEVEL_URL_KEY) != null) {
        logLevel = paramsInURL.get(LOGLEVEL_URL_KEY);
      }
    }

    if (info != null) {
      if (info.getProperty(ACCESS_ID_PROP_KEY) != null) {
        accessId = info.getProperty(ACCESS_ID_PROP_KEY);
      } else {
        if (info.getProperty(ACCESS_ID_PROP_KEY_ALT) != null) {
          accessId = info.getProperty(ACCESS_ID_PROP_KEY_ALT);
        }
      }

      if (info.getProperty(ACCESS_KEY_PROP_KEY) != null) {
        accessKey = info.getProperty(ACCESS_KEY_PROP_KEY);
      } else {
        if (info.getProperty(ACCESS_KEY_PROP_KEY_ALT) != null) {
          accessKey = info.getProperty(ACCESS_KEY_PROP_KEY_ALT);
        }
      }

      if (info.getProperty(PROJECT_PROP_KEY) != null) {
        project = info.getProperty(PROJECT_PROP_KEY);
      }

      if (info.getProperty(CHARSET_PROP_KEY) != null) {
        charset = info.getProperty(CHARSET_PROP_KEY);
      }

      if (info.getProperty(LOGVIEW_HOST_PROP_KEY) != null) {
        logview = info.getProperty(LOGVIEW_HOST_PROP_KEY);
      }

      if (info.getProperty(LIFECYCLE_PROP_KEY) != null) {
        lifecycle = info.getProperty(LIFECYCLE_PROP_KEY);
      }

      if (info.getProperty(LOGLEVEL_PROP_KEY) != null) {
        logLevel = info.getProperty(LOGLEVEL_PROP_KEY);
      }
    }
  }

  public String getEndpoint() {
    return endpoint;
  }

  public String getProject() {
    return project;
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

  public String getLogview() {
    return logview;
  }

  public String getLifecycle() { return lifecycle; }

  public String getLogLevel() { return logLevel; }
}

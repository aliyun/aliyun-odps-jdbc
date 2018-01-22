/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps.jdbc.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConnectionResource {

  private static final String JDBC_ODPS_URL_PREFIX = "jdbc:odps:";
  private static final String CHARSET_DEFAULT_VALUE = "UTF-8";
  private static final String LIFECYCLE_DEFAULT_VALUE = "3";

  /**
   * keys to retrieve properties from url.
   */
  private static final String ODPS_CONF_URL_KEY = "odps_config";
  private static final String ACCESS_ID_URL_KEY = "accessId";
  private static final String ACCESS_KEY_URL_KEY = "accessKey";
  private static final String PROJECT_URL_KEY = "project";
  private static final String CHARSET_URL_KEY = "charset";
  private static final String LOGVIEW_URL_KEY = "logview";
  private static final String LIFECYCLE_URL_KEY = "lifecycle";
  private static final String LOGLEVEL_URL_KEY = "loglevel";
  private static final String TUNNEL_ENDPOINT_URL_KEY = "tunnelEndpoint";
  private static final String LOGCONFFILE_URL_KEY = "logconffile";

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
  public static final String TUNNEL_ENDPOINT_PROP_KEY = "tunnel_endpoint";
  public static final String LOGCONFFILE_PROP_KEY = "log_conf_file";


  // This is to support DriverManager.getConnection(url, user, password) API,
  // which put the 'user' and 'password' to the 'info'.
  // So the `access_id` and `access_key` have aliases.
  private static final String ACCESS_ID_PROP_KEY_ALT = "user";
  private static final String ACCESS_KEY_PROP_KEY_ALT = "password";

  private String endpoint;
  private String accessId;
  private String accessKey;
  private String project;
  private String charset;
  private String logview;
  private String lifecycle;
  private String logLevel;
  private String tunnelEndpoint;
  private String logConfFile;

  public static boolean acceptURL(String url) {
    return (url != null) && url.startsWith(JDBC_ODPS_URL_PREFIX);
  }

  public ConnectionResource(String url, Properties info) {
    Map<String, String> paramsInURL = extractParamsFromUrl(url);
    init(info, paramsInURL);
  }

  @SuppressWarnings("rawtypes")
  void init(Properties info, Map<String, String> paramsInURL) {
    List<Map> maps = new ArrayList<Map>();
    if (paramsInURL.get(ODPS_CONF_URL_KEY) != null) {
      try {
        InputStream inputStream = new FileInputStream(paramsInURL.get(ODPS_CONF_URL_KEY));
        if (inputStream != null) {
          Properties props = new Properties();
          props.load(inputStream);
          maps.add(props);
        }
      } catch (IOException e) {
        throw new RuntimeException("Load odps conf failed:", e);
      }
    } else {
      maps.add(info);
      maps.add(paramsInURL);
    }

    accessId =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, ACCESS_ID_PROP_KEY_ALT,
            ACCESS_ID_PROP_KEY, ACCESS_ID_URL_KEY);
    accessKey =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, ACCESS_KEY_PROP_KEY_ALT,
            ACCESS_KEY_PROP_KEY, ACCESS_KEY_URL_KEY);
    
    if (accessKey != null) {
      try {
        accessKey = URLDecoder.decode(accessKey, CHARSET_DEFAULT_VALUE);
      } catch (UnsupportedEncodingException e) {
        accessKey = URLDecoder.decode(accessKey);
      }
    }
    
    charset =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, CHARSET_DEFAULT_VALUE, CHARSET_PROP_KEY,
            CHARSET_URL_KEY);
    project =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, PROJECT_PROP_KEY, PROJECT_URL_KEY);
    logview =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, LOGVIEW_HOST_PROP_KEY, LOGVIEW_URL_KEY);
    lifecycle =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, LIFECYCLE_DEFAULT_VALUE, LIFECYCLE_PROP_KEY,
            LIFECYCLE_URL_KEY);
    logLevel =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, LOGLEVEL_PROP_KEY, LOGLEVEL_URL_KEY);
    
    tunnelEndpoint =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, TUNNEL_ENDPOINT_PROP_KEY, TUNNEL_ENDPOINT_URL_KEY);
    
    logConfFile =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, LOGCONFFILE_PROP_KEY,
            LOGCONFFILE_URL_KEY);
  }

  @SuppressWarnings("deprecation")
  private Map<String, String> extractParamsFromUrl(String url) {
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
        int pos = pair.indexOf("=");
        if (pos > 0) {
          paramsInURL.put(URLDecoder.decode(pair.substring(0, pos)),
              URLDecoder.decode(pair.substring(pos + 1)));
        } else {
          paramsInURL.put(URLDecoder.decode(pair), null);
        }
      }
    }
    return paramsInURL;
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

  public String getLifecycle() {
    return lifecycle;
  }

  public String getLogLevel() {
    return logLevel;
  }
  
  public String getTunnelEndpoint() {
    return tunnelEndpoint;
  }

  public String getLogConfFile() {
    return logConfFile;
  }

  @SuppressWarnings("rawtypes")
  private static String tryGetFirstNonNullValueByAltMapAndAltKey(List<Map> maps,
      String defaultValue, String... altKeys) {
    String value = null;
    for (Map map : maps) {
      if (map != null) {
        for (String key : altKeys) {
          if ((value = (String) map.get(key)) != null) {
            return value;
          }
        }
      }
    }
    return defaultValue;
  }

}

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
import java.lang.reflect.Type;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.aliyun.odps.sqa.FallbackPolicy;
import com.aliyun.odps.utils.GsonObjectBuilder;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.reflect.TypeToken;

public class ConnectionResource {

  private static final String JDBC_ODPS_URL_PREFIX = "jdbc:odps:";
  private static final String CHARSET_DEFAULT_VALUE = "UTF-8";
  private static final String LIFECYCLE_DEFAULT_VALUE = "3";
  private static final String MAJOR_VERSION_DEFAULT_VALUE = "default";
  private static final String INTERACTIVE_SERVICE_NAME_DEFAULT_VALUE = "public.default";
  private static final String READ_TIMEOUT_DEFAULT_VALUE = "-1";
  private static final String CONNECT_TIMEOUT_DEFAULT_VALUE = "-1";

  /**
   * keys to retrieve properties from url.
   */
  private static final String ODPS_CONF_URL_KEY = "odps_config";
  private static final String ACCESS_ID_URL_KEY = "accessId";
  private static final String ACCESS_KEY_URL_KEY = "accessKey";
  private static final String PROJECT_URL_KEY = "project";
  private static final String EXECUTE_PROJECT_URL_KEY = "executeProject";
  private static final String CHARSET_URL_KEY = "charset";
  private static final String LOGVIEW_URL_KEY = "logview";
  private static final String TUNNEL_ENDPOINT_URL_KEY = "tunnelEndpoint";
  private static final String TUNNEL_RESULT_RETRY_TIME_URL_KEY = "tunnelRetryTime";
  private static final String LOG_CONF_FILE_URL_KEY = "logConfFile";
  private static final String INTERACTIVE_MODE_URL_KEY = "interactiveMode";
  private static final String SERVICE_NAME_URL_KEY = "interactiveServiceName";
  private static final String MAJOR_VERSION_URL_KEY = "majorVersion";
  private static final String ENABLE_ODPS_LOGGER_URL_KEY = "enableOdpsLogger";
  private static final String TABLE_LIST_URL_KEY = "tableList";
  private static final String FALLBACK_FOR_UNKNOWN_URL_KEY = "fallbackForUnknownError";
  private static final String FALLBACK_FOR_RESOURCE_URL_KEY = "fallbackForResourceNotEnough";
  private static final String FALLBACK_FOR_UPGRADING_URL_KEY = "fallbackForUpgrading";
  private static final String FALLBACK_FOR_TIMEOUT_URL_KEY = "fallbackForRunningTimeout";
  private static final String FALLBACK_FOR_UNSUPPORTED_URL_KEY = "fallbackForUnsupportedFeature";
  private static final String ALWAYS_FALLBACK_URL_KEY = "alwaysFallback";
  private static final String DISABLE_FALLBACK_URL_KEY = "disableFallback";
  private static final String FALLBACK_QUOTA_URL_KEY = "fallbackQuota";
  private static final String ATTACH_TIMEOUT_URL_KEY = "attachTimeout";
  private static final String AUTO_SELECT_LIMIT_URL_KEY = "autoSelectLimit";
  //Unit: result record row count, only applied in interactive mode
  private static final String INSTANCE_TUNNEL_MAX_RECORD_URL_KEY = "instanceTunnelMaxRecord";
  //Unit: Bytes, only applied in interactive mode
  private static final String INSTANCE_TUNNEL_MAX_SIZE_URL_KEY = "instanceTunnelMaxSize";
  private static final String STS_TOKEN_URL_KEY = "stsToken";
  private static final String DISABLE_CONN_SETTING_URL_KEY = "disableConnectionSetting";
  private static final String USE_PROJECT_TIME_ZONE_URL_KEY = "useProjectTimeZone";
  private static final String ENABLE_LIMIT_URL_KEY = "enableLimit";
  private static final String AUTO_LIMIT_FALLBACK_URL_KEY = "autoLimitFallback";
  private static final String SETTINGS_URL_KEY = "settings";
  private static final String ODPS_NAMESPACE_SCHEMA_URL_KEY = "odpsNamespaceSchema";
  private static final String SCHEMA_URL_KEY = "schema";
  private static final String READ_TIMEOUT_URL_KEY = "readTimeout";
  private static final String CONNECT_TIMEOUT_URL_KRY = "connectTimeout";
  private static final String ENABLE_COMMAND_API_URL_KEY = "enableCommandApi";
  private static final String USE_INSTANCE_TUNNEL_URL_KEY = "useInstanceTunnel";
  private static final String HTTPS_CHECK_URL_KEY = "httpsCheck";
  private static final String LOG_LEVEL_URL_KEY = "logLevel";
  private static final String TUNNEL_READ_TIMEOUT_URL_KEY = "tunnelReadTimeout";
  private static final String TUNNEL_CONNECT_TIMEOUT_URL_KRY = "tunnelConnectTimeout";
  private static final String TUNNEL_DOWNLOAD_USE_SINGLE_READER_URL_KEY = "tunnelDownloadUseSingleReader";
  private static final String RETRY_TIME_URL_KEY = "retryTime";
  private static final String SKIP_SQL_REWRITE_URL_KEY = "skipSqlRewrite";
  private static final String QUOTA_NAME_URL_KEY = "quotaName";
  private static final String SKIP_SQL_INJECT_CHECK_URL_KEY = "skipSqlInjectCheck";
  private static final String LOGVIEW_VERSION_URL_KEY = "logviewVersion";
  private static final String SKIP_CHECK_IF_SELECT = "skipCheckIfSelect";
  private static final String LONG_JOB_WARNING_THRESHOLD = "longJobWarningThreshold";

  /**
   * Keys to retrieve properties from info.
   * <p>
   * public since they are accessed in getPropInfo()
   */
  public static final String ACCESS_ID_PROP_KEY = "access_id";
  public static final String ACCESS_KEY_PROP_KEY = "access_key";
  public static final String PROJECT_PROP_KEY = "project_name";
  public static final String EXECUTE_PROJECT_PROP_KEY = "execute_project_name";
  public static final String CHARSET_PROP_KEY = "charset";
  public static final String LOGVIEW_HOST_PROP_KEY = "logview_host";
  public static final String TUNNEL_ENDPOINT_PROP_KEY = "tunnel_endpoint";
  public static final String TUNNEL_RESULT_RETRY_TIME_PROP_KEY = "tunnel_retry_time";
  public static final String LOG_CONF_FILE_PROP_KEY = "log_conf_file";
  public static final String INTERACTIVE_MODE_PROP_KEY = "interactive_mode";
  public static final String SERVICE_NAME_PROP_KEY = "interactive_service_name";
  public static final String MAJOR_VERSION_PROP_KEY = "major_version";
  public static final String ENABLE_ODPS_LOGGER_PROP_KEY = "enable_odps_logger";
  public static final String TABLE_LIST_PROP_KEY = "table_list";
  private static final String FALLBACK_FOR_UNKNOWN_PROP_KEY = "fallback_for_unknownerror";
  private static final String FALLBACK_FOR_RESOURCE_PROP_KEY = "fallback_for_resourcenotenough";
  private static final String FALLBACK_FOR_UPGRADING_PROP_KEY = "fallback_for_upgrading";
  private static final String FALLBACK_FOR_TIMEOUT_PROP_KEY = "fallback_for_runningtimeout";
  private static final String FALLBACK_FOR_UNSUPPORTED_PROP_KEY = "fallback_for_unsupportedfeature";
  private static final String ALWAYS_FALLBACK_PROP_KEY = "always_fallback";
  private static final String DISABLE_FALLBACK_PROP_KEY = "disable_fallback";
  private static final String FALLBACK_QUOTA_PROP_KEY = "fallback_quota";
  private static final String ATTACH_TIMEOUT_PROP_KEY = "attach_timeout";
  private static final String AUTO_SELECT_LIMIT_PROP_KEY = "auto_select_limit";
  //Unit: result record row count, only applied in interactive mode
  private static final String INSTANCE_TUNNEL_MAX_RECORD_PROP_KEY = "instance_tunnel_max_record";
  //Unit: Bytes, only applied in interactive mode
  private static final String INSTANCE_TUNNEL_MAX_SIZE_PROP_KEY = "instance_tunnel_max_size";
  private static final String STS_TOKEN_PROP_KEY = "sts_token";
  private static final String DISABLE_CONN_SETTING_PROP_KEY = "disable_connection_setting";
  private static final String USE_PROJECT_TIME_ZONE_PROP_KEY = "use_project_time_zone";
  private static final String ENABLE_LIMIT_PROP_KEY = "enable_limit";
  // only applied in non-interactive mode
  private static final String AUTO_FALLBACK_PROP_KEY = "auto_limit_fallback";
  private static final String SETTINGS_PROP_KEY = "settings";
  // This is to support DriverManager.getConnection(url, user, password) API,
  // which put the 'user' and 'password' to the 'info'.
  // So the `access_id` and `access_key` have aliases.
  private static final String ACCESS_ID_PROP_KEY_ALT = "user";
  private static final String ACCESS_KEY_PROP_KEY_ALT = "password";
  private static final String ODPS_NAMESPACE_SCHEMA_PROP_KEY = "odps_namespace_schema";
  private static final String SCHEMA_PROP_KEY = "schema";
  private static final String READ_TIMEOUT_PROP_KEY = "read_timeout";
  private static final String CONNECT_TIMEOUT_PROP_KEY = "connect_timeout";
  private static final String ENABLE_COMMAND_API_PROP_KEY = "enable_command_api";
  private static final String USE_INSTANCE_TUNNEL_PROP_KEY = "use_instance_tunnel";
  private static final String HTTPS_CHECK_PROP_KEY = "https_check";
  private static final String LOG_LEVEL_PROP_KEY = "log_level";
  private static final String TUNNEL_READ_TIMEOUT_PROP_KEY = "tunnel_read_timeout";
  private static final String TUNNEL_CONNECT_TIMEOUT_PROP_KEY = "tunnel_connect_timeout";
  private static final String TUNNEL_DOWNLOAD_USE_SINGLE_READER_PROP_KEY = "tunnel_download_use_single_reader";
  private static final String RETRY_TIME_PROP_KEY = "retry_time";
  private static final String SKIP_SQL_REWRITE_PROP_KEY = "skip_sql_rewrite";
  private static final String SKIP_SQL_INJECT_CHECK_PROP_KEY = "skip_sql_inject_check";
  private static final String QUOTA_NAME_PROP_KEY = "quota_name";
  private static final String VERBOSE_PROP_KEY = "verbose";
  private static final String LOGVIEW_VERSION_PROP_KEY = "logview_version";
  private static final String ASYNC_PROP_KEY = "async";

  private String endpoint;
  private String accessId;
  private String accessKey;
  private String project;
  private String executeProject;
  private String charset;
  private String logview;
  private String tunnelEndpoint;
  private String logConfFile;
  private boolean interactiveMode;
  private String interactiveServiceName;
  private String majorVersion;
  private String fallbackQuota;
  private boolean enableOdpsLogger = false;
  private Map<String, Map<String, List<String>>> tables = new HashMap<>();
  private FallbackPolicy fallbackPolicy = FallbackPolicy.alwaysFallbackPolicy();
  private Long autoSelectLimit;
  private Long countLimit;
  private Long sizeLimit;
  private Long attachTimeout;
  private int tunnelRetryTime;
  private String stsToken;
  private boolean disableConnSetting = false;
  private boolean useProjectTimeZone = false;
  private boolean enableLimit = false;
  private boolean autoLimitFallback = false;
  private boolean enableCommandApi = false;
  private boolean useInstanceTunnel = true;
  private boolean httpsCheck = false;
  private boolean skipSqlRewrite = false;
  private boolean skipSqlInjectCheck = false;
  private boolean verbose = false;
  private boolean async = false;
  private String quotaName;
  private int logviewVersion;

  public Boolean isOdpsNamespaceSchema() {
    return odpsNamespaceSchema;
  }

  public String getSchema() {
    return schema;
  }

  // null => use tenant flag
  // true/false => use session flag
  private Boolean odpsNamespaceSchema = null;
  private String schema;
  private Map<String, String> settings = new HashMap<>();
  private String readTimeout;
  private String connectTimeout;
  private String logLevel;
  private String tunnelReadTimeout;
  private String tunnelConnectTimeout;
  private boolean tunnelDownloadUseSingleReader = false;
  private int retryTime;
  private boolean skipCheckIfSelect;
  private long longJobWarningThreshold;

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

    accessId = tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, null, ACCESS_ID_PROP_KEY_ALT, ACCESS_ID_PROP_KEY, ACCESS_ID_URL_KEY);

    accessKey = tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, null, ACCESS_KEY_PROP_KEY_ALT, ACCESS_KEY_PROP_KEY, ACCESS_KEY_URL_KEY);
    if (accessKey != null) {
      try {
        accessKey = URLDecoder.decode(accessKey, CHARSET_DEFAULT_VALUE);
      } catch (UnsupportedEncodingException e) {
        accessKey = URLDecoder.decode(accessKey);
      }
    }

    useInstanceTunnel = Boolean.parseBoolean(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "true", USE_INSTANCE_TUNNEL_PROP_KEY,
            USE_INSTANCE_TUNNEL_URL_KEY));

    charset =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, CHARSET_DEFAULT_VALUE, CHARSET_PROP_KEY,
                                                 CHARSET_URL_KEY);
    project =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, PROJECT_PROP_KEY, PROJECT_URL_KEY);
    executeProject =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, EXECUTE_PROJECT_PROP_KEY,
                                                 EXECUTE_PROJECT_URL_KEY);
    logview =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, LOGVIEW_HOST_PROP_KEY,
                                                 LOGVIEW_URL_KEY);
    tunnelEndpoint =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, TUNNEL_ENDPOINT_PROP_KEY,
                                                 TUNNEL_ENDPOINT_URL_KEY);

    tunnelRetryTime = Integer.parseInt(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "6", TUNNEL_RESULT_RETRY_TIME_PROP_KEY,
                                                 TUNNEL_RESULT_RETRY_TIME_URL_KEY)
    );

    logConfFile =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, LOG_CONF_FILE_PROP_KEY,
                                                 LOG_CONF_FILE_URL_KEY);
    interactiveMode = Boolean.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", INTERACTIVE_MODE_PROP_KEY,
                                                 INTERACTIVE_MODE_URL_KEY));
    interactiveServiceName =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, INTERACTIVE_SERVICE_NAME_DEFAULT_VALUE,
                                                 SERVICE_NAME_PROP_KEY,
                                                 SERVICE_NAME_URL_KEY);
    majorVersion =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, MAJOR_VERSION_PROP_KEY,
                                                 MAJOR_VERSION_URL_KEY);
    enableOdpsLogger = Boolean.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", ENABLE_ODPS_LOGGER_PROP_KEY,
                                                 ENABLE_ODPS_LOGGER_URL_KEY)
    );

    fallbackPolicy.fallback4ResourceNotEnough(Boolean.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "true", FALLBACK_FOR_RESOURCE_PROP_KEY,
                                                 FALLBACK_FOR_RESOURCE_URL_KEY)
    ));
    fallbackPolicy.fallback4RunningTimeout(Boolean.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "true", FALLBACK_FOR_TIMEOUT_PROP_KEY,
                                                 FALLBACK_FOR_TIMEOUT_URL_KEY)
    ));
    fallbackPolicy.fallback4Upgrading(Boolean.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "true", FALLBACK_FOR_UPGRADING_PROP_KEY,
                                                 FALLBACK_FOR_UPGRADING_URL_KEY)
    ));
    fallbackPolicy.fallback4UnsupportedFeature(Boolean.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "true", FALLBACK_FOR_UNSUPPORTED_PROP_KEY,
                                                 FALLBACK_FOR_UNSUPPORTED_URL_KEY)
    ));
    fallbackPolicy.fallback4UnknownError(Boolean.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "true", FALLBACK_FOR_UNKNOWN_PROP_KEY,
                                                 FALLBACK_FOR_UNKNOWN_URL_KEY)
    ));

    boolean alwaysFallback = Boolean.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", ALWAYS_FALLBACK_PROP_KEY,
                                                 ALWAYS_FALLBACK_URL_KEY)
    );
    if (alwaysFallback) {
      fallbackPolicy = FallbackPolicy.alwaysFallbackPolicy();
    }

    boolean disableFallback = Boolean.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", DISABLE_FALLBACK_PROP_KEY,
                                                 DISABLE_FALLBACK_URL_KEY)
    );
    if (disableFallback) {
      fallbackPolicy = FallbackPolicy.nonFallbackPolicy();
    }

    fallbackQuota =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, FALLBACK_QUOTA_PROP_KEY, FALLBACK_QUOTA_URL_KEY);

    stsToken =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, STS_TOKEN_PROP_KEY, STS_TOKEN_URL_KEY);

    autoSelectLimit = Long.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "-1", AUTO_SELECT_LIMIT_PROP_KEY,
                                                 AUTO_SELECT_LIMIT_URL_KEY)
    );

    countLimit = Long.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "-1", INSTANCE_TUNNEL_MAX_RECORD_PROP_KEY,
                                                 INSTANCE_TUNNEL_MAX_RECORD_URL_KEY)
    );
    if (countLimit <= 0L) {
      countLimit = null;
    }

    sizeLimit = Long.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "-1", INSTANCE_TUNNEL_MAX_SIZE_PROP_KEY,
                                                 INSTANCE_TUNNEL_MAX_SIZE_URL_KEY)
    );
    if (sizeLimit <= 0L) {
      sizeLimit = null;
    }

    attachTimeout = Long.valueOf(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "-1", ATTACH_TIMEOUT_PROP_KEY,
                                                 ATTACH_TIMEOUT_URL_KEY)
    );
    if (attachTimeout <= 0L) {
      attachTimeout = null;
    }

    disableConnSetting = Boolean.parseBoolean(tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, "false", DISABLE_CONN_SETTING_PROP_KEY, DISABLE_CONN_SETTING_URL_KEY)
    );

    useProjectTimeZone = Boolean.parseBoolean(tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, "false", USE_PROJECT_TIME_ZONE_PROP_KEY, USE_PROJECT_TIME_ZONE_URL_KEY)
    );

    enableLimit = Boolean.parseBoolean(tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, "true", ENABLE_LIMIT_PROP_KEY, ENABLE_LIMIT_URL_KEY)
    );

    readTimeout =
            tryGetFirstNonNullValueByAltMapAndAltKey(maps, READ_TIMEOUT_DEFAULT_VALUE, READ_TIMEOUT_PROP_KEY, READ_TIMEOUT_URL_KEY);
    connectTimeout =
            tryGetFirstNonNullValueByAltMapAndAltKey(maps, CONNECT_TIMEOUT_DEFAULT_VALUE, CONNECT_TIMEOUT_PROP_KEY, CONNECT_TIMEOUT_URL_KRY);

    tunnelReadTimeout =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, READ_TIMEOUT_DEFAULT_VALUE, TUNNEL_READ_TIMEOUT_PROP_KEY, TUNNEL_READ_TIMEOUT_URL_KEY);
    tunnelConnectTimeout =
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, CONNECT_TIMEOUT_DEFAULT_VALUE, TUNNEL_CONNECT_TIMEOUT_PROP_KEY, TUNNEL_CONNECT_TIMEOUT_URL_KRY);
    tunnelDownloadUseSingleReader = Boolean.parseBoolean(tryGetFirstNonNullValueByAltMapAndAltKey(
            maps, "false", TUNNEL_DOWNLOAD_USE_SINGLE_READER_PROP_KEY, TUNNEL_DOWNLOAD_USE_SINGLE_READER_URL_KEY));


    // cancel enableLimit hint if autoSelectLimit turns on
    if (autoSelectLimit > 0) {
      enableLimit = false;
    }

    autoLimitFallback = Boolean.parseBoolean(tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, "false", AUTO_FALLBACK_PROP_KEY, AUTO_LIMIT_FALLBACK_URL_KEY));

    enableCommandApi =
        Boolean.parseBoolean(
            tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", ENABLE_COMMAND_API_PROP_KEY,
                                                     ENABLE_COMMAND_API_URL_KEY));

    httpsCheck = Boolean.parseBoolean(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", HTTPS_CHECK_PROP_KEY,
                                                 HTTPS_CHECK_URL_KEY));

    skipSqlRewrite = Boolean.parseBoolean(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", SKIP_SQL_REWRITE_PROP_KEY,
                                                 SKIP_SQL_REWRITE_URL_KEY));

    skipSqlInjectCheck = Boolean.parseBoolean(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", SKIP_SQL_INJECT_CHECK_PROP_KEY,
                                                 SKIP_SQL_INJECT_CHECK_URL_KEY));

    retryTime = Integer.parseInt(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "-1", RETRY_TIME_PROP_KEY, RETRY_TIME_URL_KEY)
    );

    logLevel = tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, LOG_LEVEL_PROP_KEY, LOG_LEVEL_URL_KEY);

    quotaName = tryGetFirstNonNullValueByAltMapAndAltKey(maps, null, QUOTA_NAME_PROP_KEY, QUOTA_NAME_URL_KEY);
    verbose =
        Boolean.parseBoolean(
            tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", VERBOSE_PROP_KEY,
                                                     VERBOSE_PROP_KEY));
    async =
        Boolean.parseBoolean(
            tryGetFirstNonNullValueByAltMapAndAltKey(maps, "async", ASYNC_PROP_KEY,
                                                     ASYNC_PROP_KEY));

    logviewVersion = Integer.parseInt(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "1", LOGVIEW_VERSION_PROP_KEY, LOGVIEW_VERSION_URL_KEY)
    );

    skipCheckIfSelect =
        Boolean.parseBoolean(
            tryGetFirstNonNullValueByAltMapAndAltKey(maps, "false", SKIP_CHECK_IF_SELECT,
                                                     SKIP_CHECK_IF_SELECT));

    longJobWarningThreshold = Long.parseLong(
        tryGetFirstNonNullValueByAltMapAndAltKey(maps, "-1", LONG_JOB_WARNING_THRESHOLD, LONG_JOB_WARNING_THRESHOLD)
    );

    // odpsNamespaceSchema in url or prop |  odps.namespace.schema in settings | odpsNamespaceSchema field
    // key not exists                     |      not set                       | null
    // true/false                         |      true/false                    | true/false
    String odpsNamespaceSchemaStr = tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, null, ODPS_NAMESPACE_SCHEMA_PROP_KEY, ODPS_NAMESPACE_SCHEMA_URL_KEY);

    checkValueIsValidBoolean(ODPS_NAMESPACE_SCHEMA_URL_KEY, odpsNamespaceSchemaStr);

    // not set => use tenant flag in sql
    // set true/false => use set value as session flag
    if (odpsNamespaceSchemaStr != null) {
      odpsNamespaceSchema = Boolean.parseBoolean(odpsNamespaceSchemaStr);
      settings.put("odps.namespace.schema", odpsNamespaceSchemaStr);
    }

    schema = tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, null, SCHEMA_PROP_KEY, SCHEMA_URL_KEY);

    if (schema != null) {
      if (odpsNamespaceSchema == null) {
        odpsNamespaceSchema = true;
        settings.put("odps.namespace.schema", "true");
      } else if (odpsNamespaceSchema == false) {
        throw new RuntimeException("ERROR: connection config invalid: schema can not be set when odpsNamespaceSchema=false");
      }

      settings.put("odps.default.schema", schema);
    }
    // The option 'tableList' accepts table names in pattern:
    //   <project name>.<table name>(,<project name>.<table name>)*
    //
    // This option is used to accelerate table loading. For a project contains thousands of tables,
    // BI software such as Tableau may load all the tables when the software starts and it could
    // take several minutes before the user could really start using it. To avoid this situation,
    // users could specify the table names they are going to use and the driver will only load
    // these tables.
    String tablesStr = tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, null, TABLE_LIST_PROP_KEY, TABLE_LIST_URL_KEY);
    String[] reusedParts = new String[3];
    if (!StringUtils.isNullOrEmpty(tablesStr)) {
      for (String tableStr : tablesStr.split(",")) {
        String[] parts = tableStr.split("\\.");
        if (parts.length == 2) {
          reusedParts[0] = parts[0];
          reusedParts[1] = null;
          reusedParts[2] = parts[1];
        } else if (parts.length == 3) {
          reusedParts = parts;
        } else {
          throw new IllegalArgumentException("Invalid table name: " + tableStr);
        }
        tables.computeIfAbsent(reusedParts[0], p -> new HashMap<>());
        tables.get(reusedParts[0]).computeIfAbsent(reusedParts[1], p -> new LinkedList<>());
        tables.get(reusedParts[0]).get(reusedParts[1]).add(reusedParts[2]);
      }
    }

    String globalSettingsInJson = tryGetFirstNonNullValueByAltMapAndAltKey(
        maps, null, SETTINGS_URL_KEY, SETTINGS_PROP_KEY);
    if (globalSettingsInJson != null) {
      Type type = new TypeToken<Map<String, String>>() {
      }.getType();
      settings.putAll(GsonObjectBuilder.get().fromJson(globalSettingsInJson, type));
    }
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

  public String getExecuteProject() {
    return executeProject;
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

  public int getLogviewVersion() {
    return logviewVersion;
  }

  public String getTunnelEndpoint() {
    return tunnelEndpoint;
  }

  public int getTunnelRetryTime() {
    return tunnelRetryTime;
  }

  public String getLogConfFile() {
    return logConfFile;
  }

  public String getInteractiveServiceName() {
    return interactiveServiceName;
  }

  public String getMajorVersion() {
    return majorVersion;
  }

  public String getFallbackQuota() {
    return fallbackQuota;
  }

  public boolean isEnableOdpsLogger() {
    return enableOdpsLogger;
  }

  public Long getAutoSelectLimit() {
    return autoSelectLimit;
  }

  public Long getCountLimit() {
    return countLimit;
  }

  public Long getSizeLimit() {
    return sizeLimit;
  }

  public Long getAttachTimeout() {
    return attachTimeout;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public boolean isAsync() {
    return async;
  }

  public boolean isSkipCheckIfSelect() {
    return skipCheckIfSelect;
  }

  public long getLongJobWarningThreshold() {
    return longJobWarningThreshold;
  }

  @SuppressWarnings("rawtypes")
  private static String tryGetFirstNonNullValueByAltMapAndAltKey(List<Map> maps,
                                                                 String defaultValue,
                                                                 String... altKeys) {
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

  private void checkValueIsValidBoolean(String key, String value) {
    if (null == value) {
      return;
    }
    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      return;
    }
    throw new IllegalArgumentException("key " + key + " value should be true/false. current value " + value + " is not valid");
  }

  public boolean isInteractiveMode() {
    return interactiveMode;
  }

  public Map<String, Map<String, List<String>>> getTables() {
    return tables;
  }

  public FallbackPolicy getFallbackPolicy() {
    return fallbackPolicy;
  }

  public String getStsToken() {
    return stsToken;
  }

  public boolean isDisableConnSetting() {
    return disableConnSetting;
  }

  public boolean isUseProjectTimeZone() {
    return useProjectTimeZone;
  }

  public boolean isEnableLimit() {
    return enableLimit;
  }

  public boolean isAutoLimitFallback() {
    return autoLimitFallback;
  }

  public Map<String, String> getSettings() {
    return settings;
  }

  public String getReadTimeout() {
    return readTimeout;
  }

  public String getConnectTimeout() {
    return connectTimeout;
  }

  public boolean isEnableCommandApi() {
    return enableCommandApi;
  }

  public boolean isUseInstanceTunnel() {
    return useInstanceTunnel;
  }

  public boolean isHttpsCheck() {
    return httpsCheck;
  }

  public boolean isSkipSqlRewrite() {
    return skipSqlRewrite;
  }

  public boolean isSkipSqlInjectCheck() {
    return skipSqlInjectCheck;
  }

  public String getLogLevel() {
    return logLevel;
  }

  public String getTunnelReadTimeout() {
    return tunnelReadTimeout;
  }

  public String getTunnelConnectTimeout() {
    return tunnelConnectTimeout;
  }

  public int getRetryTime() {
    return retryTime;
  }

  public boolean isTunnelDownloadUseSingleReader() {
    return tunnelDownloadUseSingleReader;
  }

  public String getQuotaName() {
    return quotaName;
  }
}
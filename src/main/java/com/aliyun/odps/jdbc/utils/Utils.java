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

package com.aliyun.odps.jdbc.utils;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Struct;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.jdbc.data.OdpsStruct;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.type.StructTypeInfo;
import com.aliyun.odps.utils.StringUtils;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Utils {

  public static final String JDBC_USER_AGENT = "odps.idata.useragent";
  public static final String JDBCKey = "driver.version";
  public static final String SDKKey = "sdk.version";
  public static String JDBCVersion = "JDBC-Version:" + retrieveVersion(JDBCKey);
  public static String SDKVersion = "SDK-Version:" + retrieveVersion(SDKKey);

  public static List<String> getSchemaList(Odps odps, String sql) throws SQLException {
    return Arrays.asList(getRawResult(odps, sql).split("\n"));
  }

  private static String getRawResult(Odps odps, String sql) throws SQLException {
    try {
      Instance i = SQLTask.run(odps, sql);
      i.waitForSuccess();
      return i.getTaskResults().get("AnonymousSQLTask");
    } catch (OdpsException e) {
      throw new SQLException(e.getMessage(), e);
    }
  }

  // see http://stackoverflow.com/questions/3697449/retrieve-version-from-maven-pom-xml-in-code
  public static String retrieveVersion(String key) {
    Properties prop = new Properties();
    try {
      prop.load(Utils.class.getResourceAsStream("/maxcompute-version.properties"));
      String value = prop.getProperty(key);
      return value != null ? value : "unknown";
    } catch (IOException e) {
      return "unknown";
    }
  }

  /** Match JDBC metadata patterns: percent matches any string, underscore one character.
   * Backslash escapes a wildcard (see DatabaseMetaData.getSearchStringEscape()).
   * Matching remains case insensitive for MaxCompute identifiers; all other
   * characters are literals, including regular expression metacharacters.
   */
  public static boolean matchPattern(String s, String pattern) {
    if (pattern == null) {
      return true;
    }
    if (s == null) {
      return false;
    }
    StringBuilder regex = new StringBuilder();
    boolean escaped = false;
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (escaped) {
        regex.append(java.util.regex.Pattern.quote(String.valueOf(c)));
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '%') {
        regex.append(".*");
      } else if (c == '_') {
        regex.append('.');
      } else {
        regex.append(java.util.regex.Pattern.quote(String.valueOf(c)));
      }
    }
    if (escaped) {
      regex.append(java.util.regex.Pattern.quote("\\"));
    }
    return java.util.regex.Pattern.compile(regex.toString(),
        java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.UNICODE_CASE
            | java.util.regex.Pattern.DOTALL).matcher(s).matches();
  }


  // return record count of sum(Outputs) in json summary
  // -1 if no Outputs
  public static int getSinkCountFromTaskSummary(String jsonSummary) {
    if (StringUtils.isNullOrEmpty(jsonSummary)) {
      return -1;
    }

    int ret = 0;
    try {
      JsonObject summary = new JsonParser().parse(jsonSummary).getAsJsonObject();
      JsonObject outputs = summary.getAsJsonObject("Outputs");
      if ("{}".equals(outputs.toString())) {
        return -1;
      }

      for (Map.Entry<String, JsonElement> entry : outputs.entrySet()) {
        ret += entry.getValue().getAsJsonArray().get(0).getAsInt();
      }
    } catch (Exception e) {
      // do nothing
      e.printStackTrace();
    }
    return ret;
  }

  public static String parseSetting(String sql, Properties properties) {
    if (StringUtils.isNullOrEmpty(sql)) {
      throw new IllegalArgumentException("Invalid query :" + sql);
    }

    //移除comments
    sql = removeComments(sql);
    if (!sql.trim().endsWith(";")) {
      sql += ";";
    }
    int index = 0;
    int end = 0;
    while ((end = sql.indexOf(';', index)) != -1) {
      String s = sql.substring(index, end);
      if (s.toUpperCase().matches("(?i)^(\\s*)(SET)(\\s+)(.*)=(.*);?(\\s*)$")) {
        // handle one setting
        int i = s.toLowerCase().indexOf("set");
        String pairString = s.substring(i + 3);
        String[] pair = pairString.split("=");
        properties.put(pair[0].trim(), pair[1].trim());
        index = end + 1;
      } else {
        // break if there is no settings before
        break;
      }
    }
    if (index >= sql.length()) {
      // only settings, no query behind
      return null;
    } else {
      // trim setting before query
      return sql.substring(index).trim();
    }
  }

  private static final Pattern INLINE_COMMENT_PATTERN = Pattern.compile("(?ms)--.*?$");
  private static final Pattern MULTILINE_COMMENT_PATTERN = Pattern.compile("(?ms)/\\*(?!\\+).*?\\*/");

  private static String removeComments(String sql) {
    sql = removePattern(sql, INLINE_COMMENT_PATTERN);
    sql = removePattern(sql, MULTILINE_COMMENT_PATTERN);
    return sql;
  }

  private static String removePattern(String sql, Pattern p) {

    Matcher matcher = p.matcher(sql);
    if (matcher.find()) {
      return matcher.replaceAll("");
    }

    return sql;
  }

  public static <T> T convertToSqlType(Object object, Class<T> type, TimeZone timeZone) {
    if (object == null) {
      return null;
    }
    if (type == String.class) {
      return (T) Objects.toString(object);
    }
    if (object instanceof ZonedDateTime) {
      ZonedDateTime zonedDateTime = (ZonedDateTime) object;
      ZonedDateTime utcZonedDateTime = zonedDateTime.withZoneSameInstant(timeZone.toZoneId());

      if (type == ZonedDateTime.class) {
        return (T) utcZonedDateTime;
      } else if (type == LocalDateTime.class) {
        return (T) utcZonedDateTime.toLocalDateTime();
      }
    } else if (object instanceof LocalDateTime) {
      LocalDateTime localDateTime = (LocalDateTime) object;

      if (type == ZonedDateTime.class) {
        return (T) localDateTime.atZone(ZoneId.of("UTC"));
      }
    } else if (object instanceof Instant) {
      Instant instant = (Instant) object;
      if (type == LocalDateTime.class) {
        return (T) LocalDateTime.ofInstant(instant, timeZone.toZoneId());
      } else if (type == ZonedDateTime.class) {
        return (T) LocalDateTime.ofInstant(instant, timeZone.toZoneId())
            .atZone(timeZone.toZoneId());
      }
    } else if (object instanceof SimpleStruct && type == Struct.class) {
      SimpleStruct simpleStruct = (SimpleStruct) object;
      Struct odpsStruct = new OdpsStruct(simpleStruct.getFieldValues().toArray(new Object[0]),
                                             (StructTypeInfo) simpleStruct.getTypeInfo());
      return (T) odpsStruct;
    }
    return (T) object;
  }
}

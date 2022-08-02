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

package com.aliyun.odps.jdbc.utils.transformer.to.jdbc;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Calendar.Builder;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;

import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.data.Struct;
import com.aliyun.odps.type.TypeInfo;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;


public class ToJdbcStringTransformer extends AbstractToJdbcDateTypeTransformer {
  private static final String ZEROS = "000000000";

  private static String formatTimestamp(java.sql.Timestamp value) {
    if (value.getNanos() == 0) {
      return DATETIME_FORMAT.get().format(value);
    } else {
      String nanosValueStr = Integer.toString(value.getNanos());
      nanosValueStr = ZEROS.substring(0, (9 - nanosValueStr.length())) + nanosValueStr;

      // Truncate trailing zeros
      char[] nanosChar = new char[nanosValueStr.length()];
      nanosValueStr.getChars(0, nanosValueStr.length(), nanosChar, 0);
      int truncIndex = 8;
      while (nanosChar[truncIndex] == '0') {
        truncIndex--;
      }
      nanosValueStr = new String(nanosChar, 0, truncIndex + 1);

      return
          String.format("%s.%s", DATETIME_FORMAT.get().format(value), nanosValueStr);
    }
  }

  @Override
  public Object transform(
      Object o,
      String charset,
      Calendar cal,
      TimeZone timeZone,
      TypeInfo odpsType) throws SQLException {
    if (o == null) {
      return null;
    }

    if (o instanceof byte[]) {
      return encodeBytes((byte[]) o, charset);
    }

    // The argument cal should always be ignored since MaxCompute stores timezone information.
    try {
      if (java.util.Date.class.isInstance(o)) {
        Builder calendarBuilder = new Calendar.Builder()
            .setCalendarType("iso8601")
            .setLenient(true);
        if (timeZone != null) {
          calendarBuilder.setTimeZone(timeZone);
        }
        Calendar calendar = calendarBuilder.build();

        if (java.sql.Timestamp.class.isInstance(o)) {
          // MaxCompute TIMESTAMP
          DATETIME_FORMAT.get().setCalendar(calendar);
          return formatTimestamp((java.sql.Timestamp) o);
        } else if (java.sql.Date.class.isInstance(o)) {
          // MaxCompute DATE
          DATE_FORMAT.get().setCalendar(calendar);
          return DATE_FORMAT.get().format(o);
        } else {
          // MaxCompute DATETIME
          DATETIME_FORMAT.get().setCalendar(calendar);
          return DATETIME_FORMAT.get().format(o);
        }
      } else if (o instanceof ZonedDateTime) {
        return ZONED_DATETIME_FORMAT.get().format(((ZonedDateTime) o).toInstant());
      } else {
        if (odpsType != null) {
          Builder calendarBuilder = new Calendar.Builder()
              .setCalendarType("iso8601")
              .setLenient(true);
          if (timeZone != null) {
            calendarBuilder.setTimeZone(timeZone);
          }
          Calendar calendar = calendarBuilder.build();
          DATETIME_FORMAT.get().setCalendar(calendar);

          switch (odpsType.getOdpsType()) {
            case ARRAY:
            case MAP: {
              return GSON_FORMAT.get().toJson(o);
            }
            case STRUCT: {
              return GSON_FORMAT.get().toJson(normalizeStruct(o));
            }
            default: {
              return o.toString();
            }
          }
        }

        return o.toString();
      }
    } finally {
      restoreToDefaultCalendar();
    }
  }

  @Override
  public Object transform(Object o, String charset, Calendar cal, TimeZone timeZone)
      throws SQLException {
    return transform(o, charset, cal, timeZone, null);
  }

  private static JsonElement normalizeStruct(Object object) {
    Map<String, Object> values = new LinkedHashMap<>();
    Struct struct = (Struct) object;
    for (int i = 0; i < struct.getFieldCount(); i++) {
      values.put(struct.getFieldName(i), struct.getFieldValue(i));
    }

    return new Gson().toJsonTree(values);
  }

  static ThreadLocal<Gson> GSON_FORMAT = ThreadLocal.withInitial(() -> {

    JsonSerializer<Date> dateTimeSerializer = (date, type, jsonSerializationContext) -> {
      if (date == null) {
        return null;
      }
      return new JsonPrimitive(DATETIME_FORMAT.get().format(date));
    };
    JsonSerializer<Timestamp> timestampSerializer = (timestamp, type, jsonSerializationContext) -> {
      if (timestamp == null) {
        return null;
      }
      return new JsonPrimitive(formatTimestamp(timestamp));
    };

    JsonSerializer<SimpleStruct> structSerializer = (struct, type, jsonSerializationContext) -> {
      if (struct == null) {
        return null;
      }
      return normalizeStruct(struct);
    };

    return new GsonBuilder()
        .registerTypeAdapter(Date.class, dateTimeSerializer)
        .registerTypeAdapter(Timestamp.class, timestampSerializer)
        .registerTypeAdapter(SimpleStruct.class, structSerializer)
        .serializeNulls()
        .create();
  });

}

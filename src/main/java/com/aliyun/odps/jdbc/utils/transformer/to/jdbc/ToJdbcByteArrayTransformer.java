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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Calendar.Builder;
import java.util.TimeZone;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.jdbc.utils.JdbcColumn;


public class ToJdbcByteArrayTransformer extends AbstractToJdbcDateTypeTransformer {

  @Override
  public Object transform(Object o, String charset, TimeZone timeZone) throws SQLException {
    if (o == null) {
      return null;
    }

    if (o instanceof byte[]) {
      return o;
    } else if (java.util.Date.class.isInstance(o)) {
      Calendar calendar = null;
      if (timeZone != null) {
        Builder calendarBuilder = new Calendar.Builder()
            .setCalendarType("iso8601")
            .setLenient(true);
        calendarBuilder.setTimeZone(timeZone);
        calendar = calendarBuilder.build();
      }

      try {
        if (java.sql.Timestamp.class.isInstance(o)) {
          // MaxCompute TIMESTAMP
          if (calendar != null) {
            TIMESTAMP_FORMAT.get().setCalendar(calendar);
          }
          return TIMESTAMP_FORMAT.get().format(o).getBytes();
        } else if (java.sql.Date.class.isInstance(o)) {
          // MaxCompute DATE
          if (calendar != null) {
            DATE_FORMAT.get().setCalendar(calendar);
          }
          return DATE_FORMAT.get().format(o).getBytes();
        } else {
          // MaxCompute DATETIME
          if (calendar != null) {
            DATETIME_FORMAT.get().setCalendar(calendar);
          }
          return DATETIME_FORMAT.get().format(o).getBytes();
        }
      } finally {
        restoreToDefaultCalendar();
      }
    } else if (o instanceof Binary) {
      return ((Binary) o).data();
    } else {
      return o.toString().getBytes();
    }
  }
}

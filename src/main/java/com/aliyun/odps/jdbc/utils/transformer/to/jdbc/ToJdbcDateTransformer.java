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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;

import com.aliyun.odps.jdbc.utils.TimeUtils;


public class ToJdbcDateTransformer extends AbstractToJdbcDateTypeTransformer {

  @Override
  public Object transform(
      Object o,
      String charset,
      Calendar cal,
      TimeZone timeZone) throws SQLException {

    if (o == null) {
      return null;
    }

    if (java.util.Date.class.isInstance(o)) {
      long time = ((java.util.Date) o).getTime();
      if (timeZone != null) {
        time += timeZone.getOffset(time);
      }
      return new java.sql.Date(time);
    } else if (o instanceof ZonedDateTime) {
      if (timeZone != null) {
        o = ((ZonedDateTime) o).withZoneSameInstant(timeZone.toZoneId());
      }
      return java.sql.Date.valueOf(((ZonedDateTime) o).toLocalDate());
    } else if (o instanceof Instant) {
      // 转换
      ZonedDateTime
          zonedDateTime =
          ZonedDateTime.ofInstant((Instant) o, timeZone == null ? ZONED_DATETIME_FORMAT.get().getZone() : timeZone.toZoneId());
      return java.sql.Date.valueOf(zonedDateTime.toLocalDate());
    } else if (o instanceof LocalDate) {
      return TimeUtils.getDate(java.sql.Date.valueOf((LocalDate) o), TimeZone.getDefault(),
                               timeZone);
    } else if (o instanceof byte[]) {
      try {
        SimpleDateFormat datetimeFormat = DATETIME_FORMAT.get();
        SimpleDateFormat dateFormat = DATE_FORMAT.get();
        if (cal != null) {
          datetimeFormat.setCalendar(cal);
          dateFormat.setCalendar(cal);
        }
        try {
          return new java.sql.Date(
              datetimeFormat.parse(encodeBytes((byte[]) o, charset)).getTime());
        } catch (ParseException ignored) {
        }
        try {
          return new java.sql.Date(dateFormat.parse(encodeBytes((byte[]) o, charset)).getTime());
        } catch (ParseException ignored) {
        }
        String errorMsg =
            getTransformationErrMsg(encodeBytes((byte[]) o, charset), java.sql.Date.class);
        throw new SQLException(errorMsg);
      } finally {
        restoreToDefaultCalendar();
      }
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), java.sql.Date.class);
      throw new SQLException(errorMsg);
    }
  }
}
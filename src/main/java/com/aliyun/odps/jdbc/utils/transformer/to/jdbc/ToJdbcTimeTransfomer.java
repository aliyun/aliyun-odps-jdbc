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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;

/**
 * Mapping of Java Types to ODPS Types for {@link java.sql.ResultSet#getTimestamp(int)} usage.
 * A transformer is applied to convert ODPS native types to match the Java byte requirement.
 * Following show which ODPS types can be converted.
 * Incompatible types or conversion errors will result in a SQLException being thrown.
 * <p>
 * ZonedDateTime (DATETIME), Instant (TIMESTAMP), LocalDateTime (TIMESTAMP_NTZ), byte[] (String), Binary (Binary)
 * <p>
 */
public class ToJdbcTimeTransfomer extends AbstractToJdbcDateTypeTransformer {

  @Override
  public Object transform(
      Object o,
      String charset,
      Calendar cal,
      TimeZone timeZone) throws SQLException {

    if (o == null) {
      return null;
    }
    try {
      if (o instanceof ZonedDateTime) {
        if (timeZone != null) {
          o = ((ZonedDateTime) o).withZoneSameInstant(timeZone.toZoneId());
        }
        return java.sql.Time.valueOf(((ZonedDateTime) o).toLocalTime());
      } else if (o instanceof Instant) {
        ZonedDateTime
            zonedDateTime =
            ZonedDateTime.ofInstant((Instant) o,
                                    timeZone == null ? ZoneId.systemDefault()
                                                     : timeZone.toZoneId());
        return java.sql.Time.valueOf(zonedDateTime.toLocalTime());
      } else if (o instanceof LocalDateTime) {
        return ((LocalDateTime) o).toLocalTime();
      } else if (o instanceof byte[]) {
        o = encodeBytes((byte[]) o, charset);
        try {
          SimpleDateFormat timeFormat = TIME_FORMAT.get();
          if (cal != null) {
            timeFormat.setCalendar(cal);
          }
          return new java.sql.Time(timeFormat.parse((String) o).getTime());
        } finally {
          restoreToDefaultCalendar();
        }
      } else {
        String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), java.sql.Timestamp.class);
        throw new SQLException(errorMsg);
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      String
          errorMsg =
          getTransformationErrMsg(Objects.toString(o), java.sql.Time.class, e.getMessage());
      throw new SQLException(errorMsg, e);
    }
  }
}

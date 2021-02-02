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
import java.util.Calendar;
import java.util.TimeZone;


public class ToJdbcTimeTransfomer extends AbstractToJdbcDateTypeTransformer {

  @Override
  public Object transform(
      Object o,
      String charset,
      Calendar cal,
      TimeZone projectTimeZone) throws SQLException {

    if (o == null) {
      return null;
    }

    if (java.util.Date.class.isInstance(o)) {
      long time = ((java.util.Date) o).getTime();
      if (projectTimeZone != null) {
        time += projectTimeZone.getOffset(time);
      }
      return new java.sql.Time(time);
    } else if (o instanceof byte[]) {
      try {
        SimpleDateFormat datetimeFormat = DATETIME_FORMAT.get();
        SimpleDateFormat timeFormat = TIME_FORMAT.get();
        if (cal != null) {
          datetimeFormat.setCalendar(cal);
          timeFormat.setCalendar(cal);
        }
        try {
          return new java.sql.Time(
              datetimeFormat.parse(encodeBytes((byte[]) o, charset)).getTime());
        } catch (ParseException ignored) {
        }
        try {
          return new java.sql.Time(timeFormat.parse(encodeBytes((byte[]) o, charset)).getTime());
        } catch (ParseException ignored) {
        }
        String errorMsg =
            getTransformationErrMsg(encodeBytes((byte[]) o, charset), java.sql.Time.class);
        throw new SQLException(errorMsg);
      } finally {
        restoreToDefaultCalendar();
      }
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), java.sql.Timestamp.class);
      throw new SQLException(errorMsg);
    }
  }
}

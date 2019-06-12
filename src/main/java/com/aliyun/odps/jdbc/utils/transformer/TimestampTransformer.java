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

package com.aliyun.odps.jdbc.utils.transformer;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.aliyun.odps.jdbc.utils.JdbcColumn;


public class TimestampTransformer extends AbstractDateTypeTransformer {

  @Override
  public Object transform(Object o, String charset, Calendar cal) throws SQLException {
    if (o == null) {
      return null;
    }

    if (java.util.Date.class.isInstance(o)) {
      if (o instanceof java.sql.Timestamp) {
        return o;
      } else {
        // The following conversion will lose nano values
        return new java.sql.Timestamp(((java.util.Date) o).getTime());
      }
    } else if (o instanceof byte[]) {
      // Acceptable pattern yyyy-MM-dd HH:mm:ss[.f...]
      SimpleDateFormat datetimeFormat = new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT);
      datetimeFormat.setTimeZone(getTimeZone(cal));
      try {
        // A timestamp string has two parts: datetime part and nano value part. We will firstly
        // process the datetime part and apply the timezone. The nano value part will be set to the
        // timestamp object later, since it has nothing to do with timezone.

        // Deal with the datetime part, apply the timezone
        String timestampStr = encodeBytes((byte[]) o, charset);
        int dotIndex = timestampStr.indexOf('.');
        Date date;
        if (dotIndex == -1) {
          date = datetimeFormat.parse(timestampStr);
        } else {
          date = datetimeFormat.parse(timestampStr.substring(0, dotIndex));
        }

        // Overwrite the datetime part
        Timestamp timestamp = java.sql.Timestamp.valueOf(timestampStr);
        int nanoValue = timestamp.getNanos();
        timestamp.setTime(date.getTime());
        timestamp.setNanos(nanoValue);

        return timestamp;
      } catch (IllegalArgumentException e) {
        String errorMsg = getTransformationErrMsg(o, java.sql.Timestamp.class);
        throw new SQLException(errorMsg);
      } catch (ParseException e) {
        String errorMsg = getTransformationErrMsg(o, java.sql.Timestamp.class);
        throw new SQLException(errorMsg);
      }
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), java.sql.Timestamp.class);
      throw new SQLException(errorMsg);
    }
  }
}

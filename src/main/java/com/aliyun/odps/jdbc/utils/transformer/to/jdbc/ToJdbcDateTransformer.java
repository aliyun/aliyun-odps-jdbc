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

import java.sql.Date;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.jdbc.utils.JdbcTimeUtil;
import com.aliyun.odps.jdbc.utils.RecordConverterCache;
import com.aliyun.odps.type.TypeInfoFactory;


/**
 * Mapping of Java Types to ODPS Types for {@link java.sql.ResultSet#getDate(int)} usage.
 * A transformer is applied to convert ODPS native types to match the Java byte requirement.
 * Following show which ODPS types can be converted.
 * Incompatible types or conversion errors will result in a SQLException being thrown.
 * <p>
 * LocalDate (DATE), ZonedDateTime (DATETIME), Instant (TIMESTAMP), LocalDateTime (TIMESTAMP_NTZ), byte[] (String), Binary (Binary)
 * <p>
 */
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
    TimeZone parserTimezone = timeZone;
    if (cal != null) {
      parserTimezone = cal.getTimeZone();
    }
    try {
      if (o instanceof byte[]) {
        String str = encodeBytes((byte[]) o, charset);
        // convert to local date
        o = RecordConverterCache.get(parserTimezone).parseObject(str, TypeInfoFactory.DATE);
      }
      if (o instanceof Binary) {
        String str = encodeBytes(((Binary) o).data(), charset);
        // convert to local date
        o = RecordConverterCache.get(parserTimezone).parseObject(str, TypeInfoFactory.DATE);
      }
      if (o instanceof LocalDate) {
        return JdbcTimeUtil.epochDayToJdbcDate(((LocalDate) o).toEpochDay(), timeZone);
      } else if (o instanceof ZonedDateTime || o instanceof Instant || o instanceof LocalDateTime) {
        return JdbcTimeUtil.toJdbcDate(JdbcTimeUtil.getEpochMillis(o), timeZone);
      } else {
        String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), Date.class);
        throw new SQLException(errorMsg);
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      String errorMsg = getTransformationErrMsg(Objects.toString(o), Date.class, e.getMessage());
      throw new SQLException(errorMsg, e);
    }
  }
}
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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.Binary;
import com.aliyun.odps.jdbc.utils.RecordConverterCache;
import com.aliyun.odps.jdbc.utils.TimeUtils;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.TypeInfoFactory;
import com.aliyun.odps.utils.OdpsCommonUtils;


/**
 * Mapping of Java Types to ODPS Types for {@link java.sql.ResultSet#getTimestamp(int)} usage.
 * A transformer is applied to convert ODPS native types to match the Java byte requirement.
 * Following show which ODPS types can be converted.
 * Incompatible types or conversion errors will result in a SQLException being thrown.
 * <p>
 * ZonedDateTime (DATETIME), Instant (TIMESTAMP), LocalDateTime (TIMESTAMP_NTZ), byte[] (String), Binary (Binary)
 * <p>
 */
public class ToJdbcTimestampTransformer extends AbstractToJdbcDateTypeTransformer {

  @Override
  public Object transform(
      Object o,
      String charset,
      Calendar cal,
      TimeZone timeZone) throws SQLException {
    return transform(o, charset, cal, timeZone, OdpsCommonUtils.indicateTypeFromClass(o));
  }

  @Override
  public Object transform(
      Object o,
      String charset,
      Calendar cal,
      TimeZone timeZone,
      TypeInfo typeInfo) throws SQLException {
    if (o == null) {
      return null;
    }
    // if typeInfo is null or not time type, use default (TIMESTAMP)
    if (typeInfo == null || (typeInfo.getOdpsType() != OdpsType.DATETIME
                             && typeInfo.getOdpsType() != OdpsType.TIMESTAMP
                             && typeInfo.getOdpsType() != OdpsType.TIMESTAMP_NTZ)) {
      typeInfo = TypeInfoFactory.TIMESTAMP;
    }
    try {
      if (o instanceof byte[]) {
        String str = encodeBytes((byte[]) o, charset);
        // convert to local date
        o = RecordConverterCache.get(timeZone).parseObject(str, typeInfo);
      }
      if (o instanceof Binary) {
        String str = encodeBytes(((Binary) o).data(), charset);
        // convert to local date
        o = RecordConverterCache.get(timeZone).parseObject(str, typeInfo);
      }

      if (o instanceof ZonedDateTime) {
        return TimeUtils.getTimestamp(((ZonedDateTime) o).toInstant(), timeZone);
      } else if (o instanceof Instant) {
        return TimeUtils.getTimestamp((Instant) o, timeZone);
      } else if (o instanceof LocalDateTime) {
        return TimeUtils.getTimestamp((LocalDateTime) o, timeZone);
      } else {
        String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), java.sql.Timestamp.class);
        throw new SQLException(errorMsg);
      }
    } catch (SQLException e) {
      throw e;
    } catch (Exception e) {
      String
          errorMsg =
          getTransformationErrMsg(Objects.toString(o), java.sql.Timestamp.class, e.getMessage());
      throw new SQLException(errorMsg, e);
    }
  }
}

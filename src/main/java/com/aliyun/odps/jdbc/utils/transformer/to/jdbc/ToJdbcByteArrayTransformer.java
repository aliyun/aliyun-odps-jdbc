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
import java.util.Calendar;
import java.util.Objects;
import java.util.TimeZone;

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.data.converter.OdpsRecordConverter;
import com.aliyun.odps.jdbc.utils.RecordConverterCache;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.utils.OdpsCommonUtils;


/**
 * Mapping of Java Types to ODPS Types for {@link java.sql.ResultSet#getBytes(int)}  usage.
 * A transformer is applied to convert ODPS native types to match the Java byte requirement.
 * All ODPS types can be converted.
 * use {@link OdpsRecordConverter} to convert object to string and use {@link RecordConverterCache} to cache the record converter.
 */
public class ToJdbcByteArrayTransformer extends AbstractToJdbcDateTypeTransformer {

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
      return o;
    }
    if (o instanceof Binary) {
      return ((Binary) o).data();
    }
    OdpsRecordConverter odpsRecordConverter = RecordConverterCache.get(timeZone);
    try {
      return odpsRecordConverter.formatObject(o, odpsType).getBytes(charset);
    } catch (Exception e) {
      String errorMsg = getTransformationErrMsg(Objects.toString(o), byte[].class, e.getMessage());
      throw new SQLException(errorMsg, e);
    }
  }

  @Override
  public Object transform(Object o, String charset, Calendar cal, TimeZone timeZone)
      throws SQLException {
    return transform(o, charset, cal, timeZone, OdpsCommonUtils.indicateTypeFromClass(o));
  }
}
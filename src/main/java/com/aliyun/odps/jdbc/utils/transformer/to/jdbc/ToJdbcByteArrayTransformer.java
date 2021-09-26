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

import com.aliyun.odps.data.Binary;
import com.aliyun.odps.jdbc.utils.JdbcColumn;


public class ToJdbcByteArrayTransformer extends AbstractToJdbcTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return null;
    }

    if (o instanceof byte[]) {
      return o;
    } else if (java.util.Date.class.isInstance(o)) {
      if (java.sql.Timestamp.class.isInstance(o)) {
        return o.toString().getBytes();
      }
      SimpleDateFormat dateFormat = new SimpleDateFormat(JdbcColumn.ODPS_DATETIME_FORMAT);
      return dateFormat.format(((java.util.Date) o)).getBytes();
    } else if (o instanceof Binary) {
      return ((Binary) o).data();
    } else {
      return o.toString().getBytes();
    }
  }
}
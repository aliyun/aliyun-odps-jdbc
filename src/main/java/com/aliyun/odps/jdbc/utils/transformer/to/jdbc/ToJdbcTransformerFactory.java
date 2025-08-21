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

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class ToJdbcTransformerFactory {

  private ToJdbcTransformerFactory() {
  }

  private static final ToJdbcByteTransformer BYTE_TRANSFORMER = new ToJdbcByteTransformer();
  private static final ToJdbcShortTransformer SHORT_TRANSFORMER = new ToJdbcShortTransformer();
  private static final ToJdbcIntTransformer INT_TRANSFORMER = new ToJdbcIntTransformer();
  private static final ToJdbcLongTransformer LONG_TRANSFORMER = new ToJdbcLongTransformer();
  private static final ToJdbcFloatTransformer FLOAT_TRANSFORMER = new ToJdbcFloatTransformer();
  private static final ToJdbcDoubleTransformer DOUBLE_TRANSFORMER = new ToJdbcDoubleTransformer();
  private static final ToJdbcBigDecimalTransformer
      BIG_DECIMAL_TRANSFORMER =
      new ToJdbcBigDecimalTransformer();
  private static final ToJdbcStringTransformer STRING_TRANSFORMER = new ToJdbcStringTransformer();
  private static final ToJdbcByteArrayTransformer
      BYTE_ARRAY_TRANSFORMER =
      new ToJdbcByteArrayTransformer();
  private static final ToJdbcDateTransformer DATE_TRANSFORMER = new ToJdbcDateTransformer();
  private static final ToJdbcTimeTransfomer TIME_TRANSFORMER = new ToJdbcTimeTransfomer();
  private static final ToJdbcTimestampTransformer
      TIMESTAMP_TRANSFORMER =
      new ToJdbcTimestampTransformer();
  private static final ToJdbcBooleanTransformer
      BOOLEAN_TRANSFORMER =
      new ToJdbcBooleanTransformer();
  private static final ToJdbcArrayTransformer
      ARRAY_TRANSFORMER =
      new ToJdbcArrayTransformer();

  private static final Map<Class, AbstractToJdbcTransformer> JDBC_CLASS_TO_TRANSFORMER =
      new HashMap<Class, AbstractToJdbcTransformer>();

  static {
    JDBC_CLASS_TO_TRANSFORMER.put(byte.class, BYTE_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(short.class, SHORT_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(int.class, INT_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(long.class, LONG_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(float.class, FLOAT_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(double.class, DOUBLE_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(BigDecimal.class, BIG_DECIMAL_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(String.class, STRING_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(byte[].class, BYTE_ARRAY_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(java.sql.Date.class, DATE_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(java.sql.Time.class, TIME_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(java.sql.Timestamp.class, TIMESTAMP_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(boolean.class, BOOLEAN_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(Array.class, ARRAY_TRANSFORMER);
  }

  public static AbstractToJdbcTransformer getTransformer(Class jdbcCls) throws SQLException {
    AbstractToJdbcTransformer transformer = JDBC_CLASS_TO_TRANSFORMER.get(jdbcCls);
    if (transformer == null) {
      throw new SQLException("Not supported JDBC class: " + jdbcCls.getName());
    }
    return transformer;
  }
}

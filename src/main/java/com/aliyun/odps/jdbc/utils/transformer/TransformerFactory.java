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

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


public class TransformerFactory {

  private TransformerFactory() {
  }

  private static final ByteTransformer BYTE_TRANSFORMER = new ByteTransformer();
  private static final ShortTransformer SHORT_TRANSFORMER = new ShortTransformer();
  private static final IntTransformer INT_TRANSFORMER = new IntTransformer();
  private static final LongTransformer LONG_TRANSFORMER = new LongTransformer();
  private static final FloatTransformer FLOAT_TRANSFORMER = new FloatTransformer();
  private static final DoubleTransformer DOUBLE_TRANSFORMER = new DoubleTransformer();
  private static final BigDecimalTransformer BIG_DECIMAL_TRANSFORMER = new BigDecimalTransformer();
  private static final StringTransformer STRING_TRANSFORMER = new StringTransformer();
  private static final ByteArrayTransformer BYTE_ARRAY_TRANSFORMER = new ByteArrayTransformer();
  private static final DateTransformer DATE_TRANSFORMER = new DateTransformer();
  private static final TimeTransfomer TIME_TRANSFOMER = new TimeTransfomer();
  private static final TimestampTransformer TIMESTAMP_TRANSFORMER = new TimestampTransformer();
  private static final BooleanTransformer BOOLEAN_TRANSFORMER = new BooleanTransformer();

  private static final Map<Class, AbstractTransformer> JDBC_CLASS_TO_TRANSFORMER =
      new HashMap<Class, AbstractTransformer>();

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
    JDBC_CLASS_TO_TRANSFORMER.put(java.sql.Time.class, TIME_TRANSFOMER);
    JDBC_CLASS_TO_TRANSFORMER.put(java.sql.Timestamp.class, TIMESTAMP_TRANSFORMER);
    JDBC_CLASS_TO_TRANSFORMER.put(boolean.class, BOOLEAN_TRANSFORMER);
  }

  public static AbstractTransformer getTransformer(Class jdbcCls) throws SQLException {
    AbstractTransformer transformer = JDBC_CLASS_TO_TRANSFORMER.get(jdbcCls);
    if (transformer == null) {
      throw new SQLException("Not supported JDBC class: " + jdbcCls.getName());
    }
    return transformer;
  }
}

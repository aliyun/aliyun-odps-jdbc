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

/**
 * Mapping of Java Types to ODPS Types for {@link java.sql.ResultSet#getBoolean(int)} usage.
 * A transformer is applied to convert ODPS native types to match the Java byte requirement.
 * Following table show which ODPS types can be converted.
 * Incompatible types or conversion errors will result in a SQLException being thrown.
 * <p>
 * | JAVA\ODPS  | TINYINT | SMALLINT | INT | BIGINT | FLOAT | DOUBLE | DECIMAL | CHAR | VARCHAR | STRING | DATE | DATETIME | TIMESTAMP | TIMESTAMP_NTZ | BOOLEAN | BINARY |
 * |:----------:|:-------:|:--------:|:---:|:------:|:-----:|:------:|:-------:|:----:|:-------:|:------:|:----:|:--------:|:---------:|:-------------:|:-------:|:------:|
 * |  boolean   |    Y    |    Y     |  Y  |   Y    |   Y   |   Y    |    Y    |      |         |   Y    |      |          |           |               |    Y    |        |
 * <p>
 * Note: The 'Y' marks indicate compatible types for the transformation.
 */
public class ToJdbcBooleanTransformer extends AbstractToJdbcTransformer {

  @Override
  public Object transform(Object o, String charset) throws SQLException {
    if (o == null) {
      return false;
    }

    if (o instanceof Boolean) {
      return o;
    } else if (o instanceof Number) {
      return ((Number) o).intValue() != 0;
    } else if (o instanceof byte[]) {
      return !"0".equals(encodeBytes((byte[]) o, charset));
    } else {
      String errorMsg = getInvalidTransformationErrorMsg(o.getClass(), boolean.class);
      throw new SQLException(errorMsg);
    }
  }
}

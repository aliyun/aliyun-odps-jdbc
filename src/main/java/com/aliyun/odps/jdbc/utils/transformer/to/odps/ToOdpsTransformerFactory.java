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

package com.aliyun.odps.jdbc.utils.transformer.to.odps;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.OdpsType;

public class ToOdpsTransformerFactory {

  private ToOdpsTransformerFactory() {
  }

  private static ToOdpsTinyintTransformer TINYINT_TRANSFORMER = new ToOdpsTinyintTransformer();
  private static ToOdpsSmallintTransformer SMALLINT_TRANSFORMER = new ToOdpsSmallintTransformer();
  private static ToOdpsIntTransformer INT_TRANSFORMER = new ToOdpsIntTransformer();
  private static ToOdpsBigIntTransformer BIGINT_TRANSFORMER = new ToOdpsBigIntTransformer();
  private static ToOdpsFloatTransformer FLOAT_TRANSFORMER = new ToOdpsFloatTransformer();
  private static ToOdpsDoubleTransformer DOUBLE_TRANSFORMER = new ToOdpsDoubleTransformer();
  private static ToOdpsDecimalTransformer DECIMAL_TRANSFORMER = new ToOdpsDecimalTransformer();
  private static ToOdpsVarcharTransformer VARCHAR_TRANSFORMER = new ToOdpsVarcharTransformer();
  private static ToOdpsStringTransformer STRING_TRANSFORMER = new ToOdpsStringTransformer();
  private static ToOdpsDatetimeTransformer DATETIME_TRANSFORMER = new ToOdpsDatetimeTransformer();
  private static ToOdpsTimeStampTransformer
      TIMESTAMP_TRANSFORMER =
      new ToOdpsTimeStampTransformer();
  private static ToOdpsDateTransformer DATE_TRANSFORMER = new ToOdpsDateTransformer();
  private static ToOdpsBooleanTransformer BOOLEAN_TRANSFORMER = new ToOdpsBooleanTransformer();
  private static ToOdpsBinaryTransformer BINARY_TRANSFORMER = new ToOdpsBinaryTransformer();
  private static ToOdpsCharTransformer CHAR_TRANSFORMER = new ToOdpsCharTransformer();
  private static ToOdpsJsonTransformer JSON_TRANSFORMER = new ToOdpsJsonTransformer();
  private static ToOdpsArrayTransformer ARRAY_TRANSFORMER = new ToOdpsArrayTransformer();
  private static ToOdpsStructTransformer STRUCT_TRANSFORMER = new ToOdpsStructTransformer();

  private static final Map<OdpsType, AbstractToOdpsTransformer> ODPS_TYPE_TO_TRANSFORMER =
      new HashMap<>();

  static {
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.TINYINT, TINYINT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.SMALLINT, SMALLINT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.INT, INT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.BIGINT, BIGINT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.FLOAT, FLOAT_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.DOUBLE, DOUBLE_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.DECIMAL, DECIMAL_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.VARCHAR, VARCHAR_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.STRING, STRING_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.DATETIME, DATETIME_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.TIMESTAMP, TIMESTAMP_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.DATE, DATE_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.BOOLEAN, BOOLEAN_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.BINARY, BINARY_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.CHAR, CHAR_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.JSON, JSON_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.ARRAY, ARRAY_TRANSFORMER);
    ODPS_TYPE_TO_TRANSFORMER.put(OdpsType.STRUCT, STRUCT_TRANSFORMER);
  }

  public static AbstractToOdpsTransformer getTransformer(OdpsType odpsType) throws SQLException {
    AbstractToOdpsTransformer transformer = ODPS_TYPE_TO_TRANSFORMER.get(odpsType);
    if (transformer == null) {
      throw new SQLException("Not supported ODPS type: " + odpsType);
    }
    return transformer;
  }
}

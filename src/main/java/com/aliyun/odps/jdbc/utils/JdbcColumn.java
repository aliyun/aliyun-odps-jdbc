/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.aliyun.odps.jdbc.utils;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.data.OdpsTypeTransformer;
import com.aliyun.odps.type.CharTypeInfo;
import com.aliyun.odps.type.DecimalTypeInfo;
import com.aliyun.odps.type.SimplePrimitiveTypeInfo;
import com.aliyun.odps.type.TypeInfo;
import com.aliyun.odps.type.VarcharTypeInfo;

/**
 * Wrap around column attributes in this class so that we can display getColumns() easily.
 */
public class JdbcColumn {
  public static final String ODPS_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
  public static final String ODPS_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final String ODPS_DATE_FORMAT = "yyyy-MM-dd";
  public static final String ODPS_TIME_FORMAT = "HH:mm:ss";
  public static final int ODPS_DECIMAL_PRECISON = 54;
  public static final int ODPS_DECIMAL_SCALE = 18;
  public static final int ODPS_STRING_CHARACTERS = 8 * 1024 * 1024 / 3;
  private static Map<OdpsType, Integer> ODPS_SQLTYPE_MAPPER = new HashMap<>();

  static {
    ODPS_SQLTYPE_MAPPER.put(OdpsType.VOID, java.sql.Types.NULL);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.BIGINT, java.sql.Types.BIGINT);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.STRING, java.sql.Types.VARCHAR);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.DATETIME, java.sql.Types.TIMESTAMP);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.DOUBLE, java.sql.Types.DOUBLE);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.BOOLEAN, java.sql.Types.BOOLEAN);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.DECIMAL, java.sql.Types.DECIMAL);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.ARRAY, java.sql.Types.ARRAY);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.MAP, java.sql.Types.JAVA_OBJECT);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.STRUCT, java.sql.Types.STRUCT);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.INT, java.sql.Types.INTEGER);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.TINYINT, java.sql.Types.TINYINT);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.SMALLINT, java.sql.Types.SMALLINT);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.DATE, java.sql.Types.DATE);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.TIMESTAMP, java.sql.Types.TIMESTAMP);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.FLOAT, java.sql.Types.FLOAT);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.CHAR, java.sql.Types.CHAR);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.BINARY, java.sql.Types.BINARY);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.VARCHAR, java.sql.Types.VARCHAR);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.INTERVAL_YEAR_MONTH, java.sql.Types.OTHER);
    ODPS_SQLTYPE_MAPPER.put(OdpsType.INTERVAL_DAY_TIME, java.sql.Types.OTHER);
  }

  private final String columnName;
  private final String tableName;
  private final String tableSchema;
  private final OdpsType type;
  private final String comment;
  private final int ordinalPos;
  private final TypeInfo typeInfo;

  public JdbcColumn(String columnName, String tableName, String tableSchema, OdpsType type,
      TypeInfo typeInfo, String comment, int ordinalPos) {
    this.columnName = columnName;
    this.tableName = tableName;
    this.tableSchema = tableSchema;
    this.type = type;
    this.typeInfo = typeInfo;
    this.comment = comment;
    this.ordinalPos = ordinalPos;
  }



  public static int odpsTypeToSqlType(OdpsType type) throws SQLException {
    if (ODPS_SQLTYPE_MAPPER.containsKey(type)) {
      return ODPS_SQLTYPE_MAPPER.get(type);
    }
    throw new SQLException("Invalid odps type: " + type);
  }

  public static String columnClassName(OdpsType type) throws SQLException {
    return OdpsTypeTransformer.odpsTypeToJavaType(type).getName();
  }

  public static int columnDisplaySize(TypeInfo typeInfo) throws SQLException {
    // according to odpsTypeToSqlType possible options are:
    int columnType = odpsTypeToSqlType(typeInfo.getOdpsType());
    switch (columnType) {
      case Types.NULL:
        return 4; // "NULL"
      case Types.BOOLEAN:
        return columnPrecision(typeInfo);
      case Types.CHAR:
      case Types.VARCHAR:
        return columnPrecision(typeInfo);
      case Types.BINARY:
        return Integer.MAX_VALUE; // hive has no max limit for binary
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
        return columnPrecision(typeInfo) + 1; // allow +/-
      case Types.DATE:
        return 10;
      case Types.TIMESTAMP:
        return columnPrecision(typeInfo);
        // see
        // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Float.MAX_EXPONENT
      case Types.FLOAT:
        return 24; // e.g. -(17#).e-###
        // see
        // http://download.oracle.com/javase/6/docs/api/constant-values.html#java.lang.Double.MAX_EXPONENT
      case Types.DOUBLE:
        return 25; // e.g. -(17#).e-####
      case Types.DECIMAL:
        return columnPrecision(typeInfo) + 2; // '-' sign and '.'
      case Types.OTHER:
      case Types.JAVA_OBJECT:
        return columnPrecision(typeInfo);
      case Types.ARRAY:
      case Types.STRUCT:
        return Integer.MAX_VALUE;
      default:
        throw new SQLException("Invalid odps type: " + columnType);
    }
  }

  public static int columnPrecision(TypeInfo typeInfo) throws SQLException {
    int columnType = odpsTypeToSqlType(typeInfo.getOdpsType());
    // according to odpsTypeToSqlType possible options are:
    switch (columnType) {
      case Types.NULL:
        return 0;
      case Types.BOOLEAN:
        return 1;
      case Types.CHAR:
      case Types.VARCHAR:
        if (typeInfo instanceof VarcharTypeInfo) {
          return ((VarcharTypeInfo) typeInfo).getLength();
        }
        if (typeInfo instanceof CharTypeInfo) {
          return ((CharTypeInfo) typeInfo).getLength();
        }
        return Integer.MAX_VALUE; // hive has no max limit for strings
      case Types.BINARY:
        return Integer.MAX_VALUE; // hive has no max limit for binary
      case Types.TINYINT:
        return 3;
      case Types.SMALLINT:
        return 5;
      case Types.INTEGER:
        return 10;
      case Types.BIGINT:
        return 19;
      case Types.FLOAT:
        return 7;
      case Types.DOUBLE:
        return 15;
      case Types.DATE:
        return 10;
      case Types.TIMESTAMP:
        return 29;
      case Types.DECIMAL:
        return ((DecimalTypeInfo) typeInfo).getPrecision();
      case Types.OTHER:
      case Types.JAVA_OBJECT: {
        if (typeInfo instanceof SimplePrimitiveTypeInfo) {
          SimplePrimitiveTypeInfo spti = (SimplePrimitiveTypeInfo) typeInfo;
          if (OdpsType.INTERVAL_YEAR_MONTH.equals(spti.getOdpsType())) {
            // -yyyyyyy-mm : should be more than enough
            return 11;
          } else if (OdpsType.INTERVAL_DAY_TIME.equals(spti.getOdpsType())) {
            // -ddddddddd hh:mm:ss.nnnnnnnnn
            return 29;
          }
        }
        return Integer.MAX_VALUE;
      }
      case Types.ARRAY:
      case Types.STRUCT:
        return Integer.MAX_VALUE;
      default:
        throw new SQLException("Invalid odps type: " + columnType);
    }
  }

  public static int columnScale(OdpsType type, TypeInfo typeInfo) throws SQLException {
    int columnType = odpsTypeToSqlType(type);
    // according to odpsTypeToSqlType possible options are:
    switch (columnType) {
      case Types.NULL:
      case Types.BOOLEAN:
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.DATE:
      case Types.BINARY:
        return 0;
      case Types.FLOAT:
        return 7;
      case Types.DOUBLE:
        return 15;
      case Types.TIMESTAMP:
        return 9;
      case Types.DECIMAL:
        return ((DecimalTypeInfo) typeInfo).getScale();
      case Types.OTHER:
      case Types.JAVA_OBJECT:
      case Types.ARRAY:
      case Types.STRUCT:
        return 0;
      default:
        throw new SQLException("Invalid odps type: " + columnType);
    }
  }

  public static boolean columnCaseSensitive(OdpsType type) throws SQLException {
    int columnType = odpsTypeToSqlType(type);
    // according to odpsTypeToSqlType possible options are:
    switch (columnType) {
      case Types.NULL:
      case Types.BOOLEAN:
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.DATE:
      case Types.BINARY:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.TIMESTAMP:
      case Types.DECIMAL:
      case Types.OTHER:
      case Types.JAVA_OBJECT:
      case Types.ARRAY:
      case Types.STRUCT:
        return false;
      case Types.CHAR:
      case Types.VARCHAR:
        return true;
      default:
        throw new SQLException("Invalid odps type: " + columnType);
    }
  }

  public static boolean columnSigned(OdpsType type) throws SQLException {
    int columnType = odpsTypeToSqlType(type);
    // according to odpsTypeToSqlType possible options are:
    switch (columnType) {
      case Types.NULL:
      case Types.BOOLEAN:
      case Types.DATE:
      case Types.BINARY:
      case Types.TIMESTAMP:
      case Types.OTHER:
      case Types.JAVA_OBJECT:
      case Types.ARRAY:
      case Types.STRUCT:
      case Types.CHAR:
      case Types.VARCHAR:
        return false;
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.DECIMAL:
        return true;
      default:
        throw new SQLException("unknown OdpsType");
    }
  }

  public String getColumnName() {
    return columnName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableSchema() {
    return tableSchema;
  }

  public int getType() throws SQLException {
    return odpsTypeToSqlType(type);
  }

  public String getComment() {
    return comment;
  }

  public String getTypeName() {
    return typeInfo.getTypeName();
  }

  public int getOrdinalPos() {
    return ordinalPos;
  }

  public int getNumPercRaidx() {
    return 10;
  }

  public int getIsNullable() {
    return DatabaseMetaData.columnNullable;
  }

  public String getIsNullableString() {
    switch (getIsNullable()) {
      case (DatabaseMetaData.columnNoNulls):
        return "NO";
      case (DatabaseMetaData.columnNullable):
        return "YES";
      case (DatabaseMetaData.columnNullableUnknown):
        return null;
      default:
        return null;
    }
  }

  public int getDecimalDigits() {
    return 0;
  }
}

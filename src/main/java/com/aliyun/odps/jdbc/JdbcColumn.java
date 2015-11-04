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

package com.aliyun.odps.jdbc;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;

import com.aliyun.odps.OdpsType;

/**
 * Wrap around column attributes in this class so that we can display getColumns()
 * easily.
 */
public class JdbcColumn {

  public static final String ODPS_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final int ODPS_DECIMAL_PRECISON = 54;
  public static final int ODPS_DECIMAL_SCALE = 18;
  public static final int ODPS_STRING_CHARACTERS = 8 * 1024 * 1024 / 3;

  private final String columnName;
  private final String tableName;
  private final String tableSchema;
  private final OdpsType type;
  private final String comment;
  private final int ordinalPos;

  JdbcColumn(String columnName, String tableName, String tableSchema
      , OdpsType type, String comment, int ordinalPos) {
    this.columnName = columnName;
    this.tableName = tableName;
    this.tableSchema = tableSchema;
    this.type = type;
    this.comment = comment;
    this.ordinalPos = ordinalPos;
  }

  public static int OdpsTypeToSqlType(OdpsType type) throws SQLException {
    switch (type) {
      case BIGINT:
        return Types.BIGINT;
      case BOOLEAN:
        return Types.BOOLEAN;
      case DOUBLE:
        return Types.DOUBLE;
      case STRING:
        return Types.VARCHAR;
      case DATETIME:
        return Types.TIMESTAMP;
      case DECIMAL:
        return Types.DECIMAL;
      default:
        throw new SQLException("unknown OdpsType");
    }
  }

  public static String columnClassName(OdpsType type) throws SQLException {
    switch (type) {
      case BIGINT:
        return Long.class.getName();
      case BOOLEAN:
        return Boolean.class.getName();
      case DOUBLE:
        return Double.class.getName();
      case STRING:
        return String.class.getName();
      case DATETIME:
        return java.sql.Timestamp.class.getName();
      case DECIMAL:
        return java.math.BigDecimal.class.getName();
      default:
        throw new SQLException("unknown OdpsType");
    }
  }

  public static int columnDisplaySize(OdpsType type) throws SQLException {
    switch (type) {
      case BIGINT:
        return columnPrecision(type) + 1; // '-' sign
      case DOUBLE:
        // IEEE 754: | 1-bit sign | 11-bit exponent | 52-bit precision |
        // exponent: 2^10 = 10^4, precision: 2^52 = 10^15
        // {-}{.}{precision}{E}{-}{exponent}
        return 2 + columnPrecision(type) + 2 + 4;
      case DECIMAL:
        return columnPrecision(type) + 2; // '-' sign and '.'
      case STRING:
      case DATETIME:
      case BOOLEAN:
        return columnPrecision(type);
      default:
        throw new SQLException("unknown OdpsType");
    }
  }

  public static int columnPrecision(OdpsType type) throws SQLException {
    switch (type) {
      case BIGINT:
        return 19;
      case BOOLEAN:
        return 1;
      case DOUBLE:
        // IEEE 754: | 1-bit sign | 10-bit exponent | 52-bit precision |
        // 2^52 = 10^15
        return 15;
      case STRING:
        return ODPS_STRING_CHARACTERS;
      case DATETIME:
        return ODPS_DATETIME_FORMAT.length();
      case DECIMAL:
        return ODPS_DECIMAL_PRECISON;
      default:
        throw new SQLException("unknown OdpsType");
    }
  }

  public static int columnScale(OdpsType type) throws SQLException {
    switch (type) {
      case DOUBLE:
        return columnPrecision(type);
      case DECIMAL:
        return ODPS_DECIMAL_SCALE;
      case STRING:
      case BOOLEAN:
      case BIGINT:
      case DATETIME:
        return 0;
      default:
        throw new SQLException("unknown OdpsType");
    }
  }

  public static boolean columnCaseSensitive(OdpsType type) throws SQLException {
    switch (type) {
      case BIGINT:
      case DECIMAL:
      case DOUBLE:
      case DATETIME:
      case BOOLEAN:
        return false;
      case STRING:
        return true;
      default:
        throw new SQLException("unknown OdpsType");
    }  }

  public static boolean columnSigned(OdpsType type) throws SQLException {
    switch (type) {
      case BIGINT:
      case DECIMAL:
      case DOUBLE:
        return true;
      case STRING:
      case DATETIME:
      case BOOLEAN:
        return false;
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
    return OdpsTypeToSqlType(type);
  }

  public String getComment() {
    return comment;
  }

  public String getTypeName() {
    return OdpsType.getFullTypeString(type, null);
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

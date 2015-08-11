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
  private final String columnName;
  private final String tableName;
  private final String tableCatalog;
  private final OdpsType type;
  private final String comment;
  private final int ordinalPos;

  JdbcColumn(String columnName, String tableName, String tableCatalog
      , OdpsType type, String comment, int ordinalPos) {
    this.columnName = columnName;
    this.tableName = tableName;
    this.tableCatalog = tableCatalog;
    this.type = type;
    this.comment = comment;
    this.ordinalPos = ordinalPos;
  }

  public static int OdpsTypeToSqlType(OdpsType type) throws SQLException {
    switch(type) {
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
        throw new SQLException("unknown OdpsType to sql type conversion");
    }
  }

  public static int columnDisplaySize(OdpsType type) throws SQLException {
    switch(type) {
      case BIGINT:
        return columnPrecision(type) + 1; // +/-
      case BOOLEAN:
        return columnPrecision(type);
      case DOUBLE:
        return 25;
      case STRING:
        return columnPrecision(type);
      case DATETIME:
        return columnPrecision(type);
      case DECIMAL:
        return columnPrecision(type) + 2;
      default:
        throw new SQLException("unknown OdpsType to sql type conversion");
    }
  }

  public static int columnPrecision(OdpsType type) throws SQLException {
    switch(type) {
      case BIGINT:
        return 19;
      case BOOLEAN:
        return 1;
      case DOUBLE:
        return 15;
      case STRING:
        return 10;   // TODO
      case DATETIME:
        return 19;   // YYYY-mm-dd HH:mm:ss
      case DECIMAL:
        return 18 + 36;
      default:
        throw new SQLException("unknown OdpsType to sql type conversion");
    }
  }

  public static int columnScale(OdpsType type) throws SQLException {
    switch(type) {
      case BIGINT:
        return 0;
      case BOOLEAN:
        return 0;
      case DOUBLE:
        return 15;
      case STRING:
        return 0;
      case DATETIME:
        return 0;
      case DECIMAL:
        return 18;
      default:
        throw new SQLException("unknown OdpsType to sql type conversion");
    }
  }

  public String getColumnName() {
    return columnName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableCatalog() {
    return tableCatalog;
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

  public int getColumnSize() {
    return 10;
  }

  public int getIsNullable() {
    return DatabaseMetaData.columnNullable;
  }

  public String getIsNullableString() {
    switch (getIsNullable()) {
      case(DatabaseMetaData.columnNoNulls):
        return "NO";
      case(DatabaseMetaData.columnNullable):
        return "YES";
      case(DatabaseMetaData.columnNullableUnknown):
        return null;
      default:
        return null;
    }
  }

  public int getDecimalDigits() {
    return 0;
  }
}

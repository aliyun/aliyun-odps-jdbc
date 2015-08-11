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

import com.aliyun.odps.OdpsType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OdpsResultSetMetaData extends WrapperAdapter implements ResultSetMetaData {

  private final List<String> columnNames;
  private final List<OdpsType> columnTypes;
  private Map<String, Integer> nameIndexMap;

  OdpsResultSetMetaData(List<String> columnNames, List<OdpsType> columnTypes) {
    this.columnNames = columnNames;
    this.columnTypes = columnTypes;
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columnNames.size();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Used by other classes for querying the index from column name
   *
   * @param name
   *     column name
   * @return column index
   */
  public int getColumnIndex(String name) {
    if (nameIndexMap == null) {
      nameIndexMap = new HashMap<String, Integer>();
      for (int i = 0; i < columnNames.size(); ++i) {
        nameIndexMap.put(columnNames.get(i), i + 1);
        nameIndexMap.put(columnNames.get(i).toLowerCase(), i + 1);
      }
    }

    Integer index = nameIndexMap.get(name);
    if (index == null) {
      String lowerName = name.toLowerCase();
      if (lowerName.equals(name)) {
        return -1;
      }

      index = nameIndexMap.get(name);
    }

    if (index == null) {
      return -1;
    }

    return index;
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return columnNames.get(toZeroIndex(column));
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return columnNames.get(toZeroIndex(column));
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    OdpsType type = columnTypes.get(toZeroIndex(column));
    return JdbcColumn.OdpsTypeToSqlType(type);
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    OdpsType type = columnTypes.get(toZeroIndex(column));
    return OdpsType.getFullTypeString(type, null);
  }

  /**
   * TODO: Remove the hard code.
   *
   * @param column
   *     the column index
   * @return
   * @throws SQLException
   */
  @Override
  public int getPrecision(int column) throws SQLException {
    OdpsType type = columnTypes.get(toZeroIndex(column));

    if (type == OdpsType.BIGINT) {
      return 19;
    } else if (type == OdpsType.BOOLEAN) {
      return 1;
    } else if (type == OdpsType.DOUBLE) {
      return 15; // http://stackoverflow.com/questions/9999221/double-precision-decimal-places
    } else if (type == OdpsType.STRING) {  // UTF-8
      return 2 * 1024 * 1024 / 3;
    } else if (type == OdpsType.DATETIME) { //"yyyy-MM-dd HH:mm:ss"
      return 19;
    } else if (type == OdpsType.DECIMAL) {
      return 18 + 36;
    } else {
      throw new SQLException("unknown OdpsType to sql type conversion");
    }
  }

  /**
   * TODO: Remove the hard code.
   *
   * @param column
   *     the column index
   * @return
   * @throws SQLException
   */
  @Override
  public int getScale(int column) throws SQLException {
    OdpsType type = columnTypes.get(toZeroIndex(column));

    if (type == OdpsType.BIGINT) {
      return 0;
    } else if (type == OdpsType.BOOLEAN) {
      return 0;
    } else if (type == OdpsType.DOUBLE) {
      return 15;
    } else if (type == OdpsType.STRING) {
      return 0;
    } else if (type == OdpsType.DATETIME) {
      return 0;
    } else if (type == OdpsType.DECIMAL) {
      return 18;
    } else {
      throw new SQLException("unknown OdpsType to sql type conversion");
    }
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public String getTableName(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  /**
   * Only string type is case sensitive
   *
   * @param column
   * @return
   * @throws SQLException
   */
  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    OdpsType type = columnTypes.get(toZeroIndex(column));
    return (type == OdpsType.STRING);
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int isNullable(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  protected int toZeroIndex(int column) {
    if (column <= 0 || column > columnNames.size()) {
      throw new IllegalArgumentException();
    }
    return column - 1;
  }
}

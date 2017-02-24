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

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.jdbc.utils.JdbcColumn;
import com.aliyun.odps.type.TypeInfo;

public class OdpsResultSetMetaData extends WrapperAdapter implements ResultSetMetaData {

  private final List<String> columnNames;
  private final List<OdpsType> columnTypes;
  private final List<? extends TypeInfo> typeInfos;
  private Map<String, Integer> nameIndexMap;

  private String catalogName = " ";
  private String schemeName = " ";
  private String tableName = " ";

  OdpsResultSetMetaData(List<String> columnNames, List<? extends TypeInfo> typeInfos) {
    this.columnNames = columnNames;
    this.typeInfos = typeInfos;
    this.columnTypes = new ArrayList<OdpsType>();
    if (columnNames != null) {
      for (int i = 0; i < columnNames.size(); i++) {
        columnTypes.add(typeInfos.get(i).getOdpsType());
      }
    }
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return catalogName;
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    OdpsType type = columnTypes.get(toZeroIndex(column));
    return JdbcColumn.columnClassName(type);
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columnNames.size();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    TypeInfo typeInfo = typeInfos.get(toZeroIndex(column));
    return JdbcColumn.columnDisplaySize(typeInfo);
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
    return JdbcColumn.odpsTypeToSqlType(type);
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    TypeInfo typeInfo = typeInfos.get(toZeroIndex(column));
    return typeInfo.getTypeName();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    TypeInfo typeInfo = typeInfos.get(toZeroIndex(column));
    return JdbcColumn.columnPrecision(typeInfo);
  }

  @Override
  public int getScale(int column) throws SQLException {
    OdpsType type = columnTypes.get(toZeroIndex(column));
    TypeInfo typeInfo = typeInfos.get(toZeroIndex(column));
    return JdbcColumn.columnScale(type, typeInfo);
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    return schemeName;
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return tableName;
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    OdpsType type = columnTypes.get(toZeroIndex(column));
    return JdbcColumn.columnCaseSensitive(type);
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return false;
  }

  // TODO: check
  @Override
  public int isNullable(int column) throws SQLException {
    return ResultSetMetaData.columnNullable;
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return true;
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    OdpsType type = columnTypes.get(toZeroIndex(column));
    return JdbcColumn.columnSigned(type);
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return false;
  }

  public void setTableName(String table) {
    tableName = table;
  }

  public void setSchemeName(String scheme) {
    schemeName = scheme;
  }

  public void setCatalogName(String catalog) {
    catalogName = catalog;
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

  protected int toZeroIndex(int column) {
    if (column <= 0 || column > columnNames.size()) {
      throw new IllegalArgumentException();
    }
    return column - 1;
  }
}

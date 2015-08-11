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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

import com.aliyun.odps.Table;

public class OdpsColumnsResultSet extends OdpsResultSet implements ResultSet {

  private Iterator<JdbcColumn> iterator;
  private JdbcColumn column = null;

  OdpsColumnsResultSet(Iterator<JdbcColumn> iterator, OdpsResultSetMetaData meta)
      throws SQLException {
    super(null, meta);
    this.iterator = iterator;
  }

  @Override
  public void close() throws SQLException {
    isClosed = true;
    iterator = null;
    column = null;
  }

  @Override
  public boolean next() throws SQLException {
    checkClosed();

    if (iterator.hasNext()) {
      column = iterator.next();
      return true;
    } else {
      column = null;
      return false;
    }
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    checkClosed();

    switch (columnIndex) {
      case 1: // TABLE_CAT
        return column.getTableCatalog();
      case 2: // TABLE_SCHEM
        return null;
      case 3: // TABLE_NAME
        return column.getTableName();
      case 4: // COLUMN_NAME
        return column.getColumnName();
      case 5: // DATA_TYPE int
        return column.getType();
      case 6: // TYPE_NAME
        return column.getTypeName();
      case 7: // COLUMN_SIZE
        return column.getColumnSize();
      case 8: // BUFFER_LENGTH
        return null;
      case 9: // DECIMAL_DIGITS
        return column.getDecimalDigits();
      case 10: // NUM_PERC_RADIX
        return column.getNumPercRaidx();
      case 11: // NULLABLE
        return column.getIsNullable();
      case 12: // REMARKS
        return column.getComment();
      case 13: // COLUMN_DEF
        return null;
      case 14: // SQL_DATA_TYPE
        return null;
      case 15: // SQL_DATETIME_SUB
        return null;
      case 16: // CHAR_OCTET_LENGTH
        return null;
      case 17: // ORDINAL_POSITION
        return column.getOrdinalPos();
      case 18: // IS_NULLABLE
        return column.getIsNullableString();
      case 19: // SCOPE_CATALOG
        return null;
      case 20: // SCOPE_SCHEMA
        return null;
      case 21: // SCOPE_TABLE
        return null;
      case 22: // SOURCE_DATA_TYPE
        return null;
      default:
        return null;
    }
  }

}

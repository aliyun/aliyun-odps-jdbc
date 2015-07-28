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

public class OdpsTablesResultSet extends OdpsResultSet implements ResultSet {

  private Iterator<Table> iterator;
  private Table table;

  OdpsTablesResultSet(Iterator<Table> iterator, OdpsResultSetMetaData meta)
      throws SQLException {
    super(null, meta);
    this.iterator = iterator;
  }

  @Override
  public boolean next() throws SQLException {
    if (iterator.hasNext()) {
      table = iterator.next();
      return true;
    } else {
      table = null;
      return false;
    }
  }

  @Override
  public void close() throws SQLException {
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    switch (columnIndex) {
      case 1:
        return table.getProject();
      case 2:
        return null;
      case 3:
        return table.getName();
      case 4:
        return table.isVirtualView() ? "VIEW" : "TABLE";
      case 5:
        return table.getComment();
      case 6: // TYPE_CAT
        return null;
      case 7: // TYPE_SCHEM
        return null;
      case 8: // TYPE_NAME
        return null;
      case 9: // SELF_REFERENCING_COL_NAME
        return null;
      case 10: // REF_GENERATION
        return "USER";
      default:
        return null;
    }
  }

}

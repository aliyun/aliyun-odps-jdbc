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
import java.util.Arrays;
import java.util.Iterator;

import com.aliyun.odps.Table;
import com.aliyun.odps.jdbc.utils.OdpsLogger;
import com.aliyun.odps.jdbc.utils.Utils;
import com.aliyun.odps.utils.StringUtils;

public class OdpsTableMetaResultSet extends OdpsResultSet implements ResultSet {

  private Iterator<Table> iterator = null;
  private Object[] row = null;
  private String tableNamePattern = null;
  private String[] types = null;
  private final OdpsLogger log;
  private long begin = 0;
  private long matched = 0;
  private long filtered = 0;

  OdpsTableMetaResultSet(OdpsConnection conn, OdpsResultSetMetaData meta, Iterator<Table> iterator,
                         String tableNamePattern, String[] types, long begin)
      throws SQLException {
    super(conn, null, meta);
    this.iterator = iterator;
    this.tableNamePattern = tableNamePattern;
    this.types = types;
    this.log = conn.log;
    this.begin = begin;
  }

  @Override
  Object[] rowAtCursor() throws SQLException {
    return row;
  }

  @Override
  public boolean next() throws SQLException {
    while (iterator != null && iterator.hasNext()) {
      Table currentTable = iterator.next();
      if (!StringUtils.isNullOrEmpty(tableNamePattern)) {
        if (!Utils.matchPattern(currentTable.getName(), tableNamePattern)) {
          filtered++;
          continue;
        }
        String tableType = currentTable.isVirtualView() ? "VIEW" : "TABLE";
        if (types != null && types.length != 0) {
          if (!Arrays.asList(types).contains(tableType)) {
            filtered++;
            continue;
          }
        }
        matched++;
        row = new Object[]
            { currentTable.getProject(), currentTable.getProject(), currentTable.getName(),
              tableType, currentTable.getComment(), null, null, null, null, "USER" };
        return true;
      }
    }
    long end = System.currentTimeMillis();
    log.debug("It took me " + (end - begin) + " ms to match " + matched + " Tables, and " + filtered + " filtered.");
    return false;
  }

  @Override
  public void close() throws SQLException {
    iterator = null;
    row = null;
  }
}

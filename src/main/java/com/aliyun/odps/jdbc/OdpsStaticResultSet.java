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


class OdpsStaticResultSet extends OdpsResultSet implements ResultSet {

  private Iterator<Object[]> iterator;
  private boolean isClosed = false;
  Object[] row;

  /**
   * For some meta query (like procedures) we need to return an empty query result.
   */
  private boolean isEmptyResultSet = false;

  OdpsStaticResultSet(OdpsConnection conn, OdpsResultSetMetaData meta) throws SQLException {
    super(conn, null, meta);
    // Construct an empty result set
    isEmptyResultSet = true;
  }

  /**
   * For non-empty result set, its data is passed via parameter
   */
  OdpsStaticResultSet(OdpsConnection conn, OdpsResultSetMetaData meta, Iterator<Object[]> iter)
      throws SQLException {
    super(conn, null, meta);
    iterator = iter;
    isEmptyResultSet = false;
  }

  @Override
  public void close() throws SQLException {
    iterator = null;
    isClosed = true;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  @Override
  public boolean next() throws SQLException {
    if (isEmptyResultSet) {
      return false;
    }
    if (iterator.hasNext()) {
      row = iterator.next();
      return true;
    } else {
      return false;
    }
  }

  @Override
  protected Object[] rowAtCursor() throws SQLException {
    return row;
  }
}



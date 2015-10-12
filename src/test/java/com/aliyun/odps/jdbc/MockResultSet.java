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

public class MockResultSet extends OdpsResultSet implements ResultSet {

  Object[][] rows;
  int currentRow = -1;

  MockResultSet(Object[][] rows, OdpsResultSetMetaData meta) throws SQLException {
    super(null, meta);
    this.rows = rows;
  }

  @Override
  public boolean next() throws SQLException {
    if (currentRow < rows.length) {
      currentRow++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void close() {
    rows = null;
  }

  protected Object[] rowAtCursor() throws SQLException {
    return rows[currentRow];
  }
}
